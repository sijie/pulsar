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

import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.util.Pair;
import org.apache.distributedlog.api.AsyncLogReader;

/**
 * Cache of entries used by a single ManagedLedger. An EntryCache is compared to other EntryCache instances using their
 * size (the memory that is occupied by each of them).
 */
public interface DlogBasedEntryCache extends Comparable<DlogBasedEntryCache> {

    /**
     * @return the name of the cache
     */
    String getName();

    /**
     * Insert an entry in the cache.
     * <p>
     * If the overall limit have been reached, this will triggered the eviction of other entries, possibly from other
     * EntryCache instances
     *
     * @param entry
     *            the entry to be cached
     * @return whether the entry was inserted in cache
     */
    boolean insert(DlogBasedEntry entry);

    /**
     * Remove from cache all the entries related to a ledger up to lastPosition included.
     *
     * @param lastPosition
     *            the position of the last entry to be invalidated (inclusive)
     */
    void invalidateEntries(DlogBasedPosition lastPosition);

    /**
     * Remove from the cache all the entries belonging to a specific log segment
     *
     * @param ledgerId
     *            the log segment id
     */
    void invalidateAllEntries(long ledgerId);

    /**
     * Remove all the entries from the cache
     */
    void clear();

    /**
     * Force the cache to drop entries to free space.
     *
     * @param sizeToFree
     *            the total memory size to free
     * @return a pair containing the number of entries evicted and their total size
     */
    Pair<Integer, Long> evictEntries(long sizeToFree);

    //todo refactor the interface to keep consistent with Pulsar Entrycache
    /**
     * Read entries from the cache or from dlog.
     *
     * Get the entry data either from cache or dlog and mixes up the results in a single list.
     *
     * @param logReader
     *            the logReader
     * @param logSegNo
     *            the log segment #
     * @param firstEntry
     *            the first entry to read (inclusive)
     * @param lastEntry
     *            the last entry to read (inclusive)
     * @param isSlowestReader
     *            whether the reader cursor is the most far behind in the stream
     * @param callback
     *            the callback object that will be notified when read is done
     * @param ctx
     *            the context object
     */
    void asyncReadEntry(AsyncLogReader logReader,long logSegNo, long firstEntry, long lastEntry, boolean isSlowestReader,
                        ReadEntriesCallback callback, Object ctx);

    /**
     * Read entry at given position from the cache or from dlog.
     *
     * Get the entry data either from cache or dlog and mixes up the results in a single list.
     *
     * @param logReader
     *            the logReader
     * @param position
     *            position to read the entry from
     * @param callback
     *            the callback object that will be notified when read is done
     * @param ctx
     *            the context object
     */
    void asyncReadEntry(AsyncLogReader logReader, DlogBasedPosition position, ReadEntryCallback callback, Object ctx);

    /**
     * Get the total size in bytes of all the entries stored in this cache
     *
     * @return the size of the entry cache
     */
    long getSize();
}
