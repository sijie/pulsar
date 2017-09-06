/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.mledger.dlog;

import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.TooManyRequestsException;
import org.apache.bookkeeper.mledger.impl.*;
import org.apache.bookkeeper.mledger.util.Pair;
import org.apache.bookkeeper.mledger.util.RangeCache;
import org.apache.bookkeeper.mledger.util.RangeCache.Weighter;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecordWithDLSN;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.common.concurrent.FutureEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;

/**
 * Cache data payload for entries of all dlog log segments
 */
public class DlogBasedEntryCacheImpl implements DlogBasedEntryCache {

    private final DlogBasedEntryCacheManager manager;
    private final DlogBasedManagedLedger ml;
    private final RangeCache<DlogBasedPosition, DlogBasedEntry> entries;

    private static final double MB = 1024 * 1024;

    private static final Weighter<DlogBasedEntry> entryWeighter = new Weighter<DlogBasedEntry>() {
        @Override
        public long getSize(DlogBasedEntry entry) {
            return entry.getLength();
        }
    };

    public DlogBasedEntryCacheImpl(DlogBasedEntryCacheManager manager, DlogBasedManagedLedger ml) {
        this.manager = manager;
        this.ml = ml;
        this.entries = new RangeCache<DlogBasedPosition, DlogBasedEntry>(entryWeighter);

        if (log.isDebugEnabled()) {
            log.debug("[{}] Initialized managed-ledger entry cache", ml.getName());
        }
    }

    @Override
    public String getName() {
        return ml.getName();
    }

    public final static PooledByteBufAllocator allocator = new PooledByteBufAllocator( //
            true, // preferDirect
            0, // nHeapArenas,
            1, // nDirectArena
            8192, // pageSize
            11, // maxOrder
            64, // tinyCacheSize
            32, // smallCacheSize
            8, // normalCacheSize,
            true // Use cache for all threads
    );

    @Override
    public boolean insert(DlogBasedEntry entry) {
        if (!manager.hasSpaceInCache()) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Skipping cache while doing eviction: {} - size: {}", ml.getName(), entry.getPosition(),
                        entry.getLength());
            }
            return false;
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Adding entry to cache: {} - size: {}", ml.getName(), entry.getPosition(),
                    entry.getLength());
        }

        // Copy the entry into a buffer owned by the cache. The reason is that the incoming entry is retaining a buffer
        // from netty, usually allocated in 64Kb chunks. So if we just retain the entry without copying it, we might
        // retain actually the full 64Kb even for a small entry
        int size = entry.getLength();
        ByteBuf cachedData = null;
        try {
            cachedData = allocator.directBuffer(size, size);
        } catch (Throwable t) {
            log.warn("[{}] Failed to allocate buffer for entry cache: {}", ml.getName(), t.getMessage(), t);
            return false;
        }

        if (size > 0) {
            ByteBuf entryBuf = entry.getDataBuffer();
            int readerIdx = entryBuf.readerIndex();
            cachedData.writeBytes(entryBuf);
            entryBuf.readerIndex(readerIdx);
        }

        DlogBasedPosition position = (DlogBasedPosition)entry.getPosition();
        DlogBasedEntry cacheEntry = DlogBasedEntry.create(position, cachedData);
        cachedData.release();
        if (entries.put(position, cacheEntry)) {
            manager.entryAdded(entry.getLength());
            return true;
        } else {
            // entry was not inserted into cache, we need to discard it
            cacheEntry.release();
            return false;
        }
    }

    @Override
    public void invalidateEntries(final DlogBasedPosition lastPosition) {
        //todo reconstruct position's get func
        final DlogBasedPosition firstPosition = DlogBasedPosition.get(-1, 0, 0);

        Pair<Integer, Long> removed = entries.removeRange(firstPosition, lastPosition, true);
        int entriesRemoved = removed.first;
        long sizeRemoved = removed.second;
        if (log.isDebugEnabled()) {
            log.debug("[{}] Invalidated entries up to {} - Entries removed: {} - Size removed: {}", ml.getName(),
                    lastPosition, entriesRemoved, sizeRemoved);
        }

        manager.entriesRemoved(sizeRemoved);
    }

    @Override
    public void invalidateAllEntries(long ledgerId) {
        final DlogBasedPosition firstPosition = DlogBasedPosition.get(ledgerId, 0);
        final DlogBasedPosition lastPosition = DlogBasedPosition.get(ledgerId + 1, 0);

        Pair<Integer, Long> removed = entries.removeRange(firstPosition, lastPosition, false);
        int entriesRemoved = removed.first;
        long sizeRemoved = removed.second;
        if (log.isDebugEnabled()) {
            log.debug("[{}] Invalidated all entries on ledger {} - Entries removed: {} - Size removed: {}",
                    ml.getName(), ledgerId, entriesRemoved, sizeRemoved);
        }

        manager.entriesRemoved(sizeRemoved);
    }

    //todo we should create a logReader open from a specific location
    @Override
    public void asyncReadEntry(AsyncLogReader logReader, DlogBasedPosition position, final ReadEntryCallback callback,
                               final Object ctx) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Reading entry log stream {}: {}", ml.getName(), logReader.getStreamName(), position.getEntryId());
        }
        DlogBasedEntry entry = entries.get(position);
        if (entry != null) {
            DlogBasedEntry cachedEntry = DlogBasedEntry.create(entry);
            entry.release();
            manager.mlFactoryMBean.recordCacheHit(cachedEntry.getLength());
            callback.readEntryComplete(cachedEntry, ctx);
        } else {
            logReader.readNext().whenComplete(new FutureEventListener<LogRecordWithDLSN>() {
                @Override
                public void onSuccess(LogRecordWithDLSN logRecordWithDLSN) {
                    DlogBasedEntry returnEntry = DlogBasedEntry.create(logRecordWithDLSN);
                    //todo if LogRecordWithDLSN implemented by ByteBuf, should we realase it after use it? or Dlog deal auto?

                    manager.mlFactoryMBean.recordCacheMiss(1, returnEntry.getLength());
                    ml.mbean.addReadEntriesSample(1, returnEntry.getLength());

                    ml.getExecutor().submitOrdered(ml.getName(), safeRun(() -> {
                        callback.readEntryComplete(returnEntry, ctx);
                    }));
                }

                @Override
                public void onFailure(Throwable throwable) {

                    callback.readEntryFailed(new ManagedLedgerException(throwable), ctx);
                    return;
                }
            });

        }
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void asyncReadEntry(AsyncLogReader logReader, long logSegNo, long firstEntry, long lastEntry, boolean isSlowestReader,
                               final ReadEntriesCallback callback, Object ctx) {
        final int entriesToRead = (int) (lastEntry - firstEntry) + 1;
        final DlogBasedPosition firstPosition = DlogBasedPosition.get(logSegNo,firstEntry);
        final DlogBasedPosition lastPosition = DlogBasedPosition.get(logSegNo,lastEntry);

        if (log.isDebugEnabled()) {
            log.debug("[{}] Reading entries range log stream {}: {} to {}", ml.getName(), logReader.getStreamName(), firstEntry, lastEntry);
        }

        Collection<DlogBasedEntry> cachedEntries = entries.getRange(firstPosition, lastPosition);

        if (cachedEntries.size() == entriesToRead) {
            long totalCachedSize = 0;
            final List<DlogBasedEntry> entriesToReturn = Lists.newArrayListWithExpectedSize(entriesToRead);

            // All entries found in cache
            for (DlogBasedEntry entry : cachedEntries) {
                entriesToReturn.add(DlogBasedEntry.create(entry));
                totalCachedSize += entry.getLength();
                entry.release();
            }

            manager.mlFactoryMBean.recordCacheHits(entriesToReturn.size(), totalCachedSize);
            if (log.isDebugEnabled()) {
                log.debug("[{}] Log stream {} -- Found in cache entries: {}-{}", ml.getName(), logReader.getStreamName(), firstEntry,
                        lastEntry);
            }

            callback.readEntriesComplete((List) entriesToReturn, ctx);

        } else {
            if (!cachedEntries.isEmpty()) {
                cachedEntries.forEach(entry -> entry.release());
            }

            //todo do I use futureListener here ok?
            // Read all the entries from dlog
            logReader.readBulk(entriesToRead).whenComplete(new FutureEventListener<List<LogRecordWithDLSN>>() {
                @Override
                public void onSuccess(List<LogRecordWithDLSN> logRecordWithDLSNs) {

                    checkNotNull(ml.getName());
                    checkNotNull(ml.getExecutor());
                    ml.getExecutor().submitOrdered(ml.getName(), safeRun(() -> {
                        // We got the entries, we need to transform them to a List<> type
                        final List<DlogBasedEntry> entriesToReturn = Lists.newArrayListWithExpectedSize(entriesToRead);
                        long totalSize = 0;
                        Iterator iterator = logRecordWithDLSNs.iterator();
                        while (iterator.hasNext()){
                            DlogBasedEntry entry = DlogBasedEntry.create((LogRecordWithDLSN) iterator.next());
                            entriesToReturn.add(entry);
                            totalSize += entry.getLength();
                        }
//                        update totalSize failure in lambda
//                        logRecordWithDLSNs.forEach(logRecordWithDLSN -> {
//                            DlogBasedEntry entry = DlogBasedEntry.create(logRecordWithDLSN);
//
//                            entriesToReturn.add(entry);
//
//                            totalSize += entry.getLength();
//                        });

                        manager.mlFactoryMBean.recordCacheMiss(entriesToReturn.size(), totalSize);
                        ml.getMBean().addReadEntriesSample(entriesToReturn.size(), totalSize);

                        callback.readEntriesComplete((List) entriesToReturn, ctx);
                    }));
                }

                @Override
                public void onFailure(Throwable throwable) {
                    callback.readEntriesFailed(new ManagedLedgerException(throwable), ctx);
                }
            });
        }
    }

    @Override
    public void clear() {
        long removedSize = entries.clear();
        manager.entriesRemoved(removedSize);
    }

    @Override
    public long getSize() {
        return entries.getSize();
    }

    @Override
    public int compareTo(DlogBasedEntryCache other) {
        return Longs.compare(getSize(), other.getSize());
    }

    @Override
    public Pair<Integer, Long> evictEntries(long sizeToFree) {
        checkArgument(sizeToFree > 0);
        Pair<Integer, Long> evicted = entries.evictLeastAccessedEntries(sizeToFree);
        int evictedEntries = evicted.first;
        long evictedSize = evicted.second;
        if (log.isDebugEnabled()) {
            log.debug(
                    "[{}] Doing cache eviction of at least {} Mb -- Deleted {} entries - Total size deleted: {} Mb "
                            + " -- Current Size: {} Mb",
                    ml.getName(), sizeToFree / MB, evictedEntries, evictedSize / MB, entries.getSize() / MB);
        }
        manager.entriesRemoved(evictedSize);
        return evicted;
    }

    private static final Logger log = LoggerFactory.getLogger(DlogBasedEntryCacheImpl.class);
}
