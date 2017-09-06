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
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.TooManyRequestsException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;

public class DlogBasedOpReadEntry implements ReadEntriesCallback {

    DlogBasedManagedCursor cursor;
    DlogBasedPosition readPosition;
    private int count;
    private ReadEntriesCallback callback;
    Object ctx;

    // Results
    private List<Entry> entries;
    private DlogBasedPosition nextReadPosition;

    public static DlogBasedOpReadEntry create(DlogBasedManagedCursor cursor, DlogBasedPosition readPositionRef, int count,
                                              ReadEntriesCallback callback, Object ctx) {
        DlogBasedOpReadEntry op = RECYCLER.get();
        op.readPosition = cursor.ledger.startReadOperationOnLedger(readPositionRef);
        op.cursor = cursor;
        op.count = count;
        op.callback = callback;
        op.entries = Lists.newArrayList();
        op.ctx = ctx;
        op.nextReadPosition = DlogBasedPosition.get(op.readPosition);
        return op;
    }

    @Override
    public void readEntriesComplete(List<Entry> returnedEntries, Object ctx) {
        // Filter the returned entries for individual deleted messages
        int entriesSize = returnedEntries.size();
        final DlogBasedPosition lastPosition = (DlogBasedPosition) returnedEntries.get(entriesSize - 1).getPosition();
        if (log.isDebugEnabled()) {
            log.debug("[{}][{}] Read entries succeeded batch_size={} cumulative_size={} requested_count={}",
                    cursor.ledger.getName(), cursor.getName(), returnedEntries.size(), entries.size(), count);
        }
        List<Entry> filteredEntries = cursor.filterReadEntries(returnedEntries);
        entries.addAll(filteredEntries);

        // if entries have been filtered out then try to skip reading of already deletedMessages in that range
        final Position nexReadPosition = entriesSize != filteredEntries.size()
                ? cursor.getNextAvailablePosition(lastPosition) : lastPosition.getNext();
        updateReadPosition(nexReadPosition);
        checkReadCompletion();
    }

    @Override
    public void readEntriesFailed(ManagedLedgerException status, Object ctx) {
        cursor.readOperationCompleted();

        if (!entries.isEmpty()) {
            // There were already some entries that were read before, we can return them
            cursor.ledger.getExecutor().submit(safeRun(() -> {
                callback.readEntriesComplete(entries, ctx);
                recycle();
            }));
        } else {
            if (!(status instanceof TooManyRequestsException)) {
                log.warn("[{}][{}] read failed from ledger at position:{} : {}", cursor.ledger.getName(), cursor.getName(),
                        readPosition, status.getMessage());
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("[{}][{}] read throttled failed from ledger at position:{}", cursor.ledger.getName(),
                            cursor.getName(), readPosition);
                }
            }

            callback.readEntriesFailed(status, ctx);
            cursor.ledger.mbean.recordReadEntriesError();
            recycle();
        }
    }

    void updateReadPosition(Position newReadPosition) {
        nextReadPosition = (DlogBasedPosition) newReadPosition;
        cursor.setReadPosition(nextReadPosition);
    }

    void checkReadCompletion() {
        if (entries.size() < count && cursor.hasMoreEntries()) {
            // We still have more entries to read from the next ledger, schedule a new async operation
            if (nextReadPosition.getLedgerId() != readPosition.getLedgerId()) {
                cursor.ledger.startReadOperationOnLedger(nextReadPosition);
            }

            // Schedule next read in a different thread
            cursor.ledger.getExecutor().submit(safeRun(() -> {
                readPosition = cursor.ledger.startReadOperationOnLedger(nextReadPosition);
                cursor.ledger.asyncReadEntries(DlogBasedOpReadEntry.this);
            }));
        } else {
            // The reading was already completed, release resources and trigger callback
            cursor.readOperationCompleted();

            cursor.ledger.getExecutor().submit(safeRun(() -> {
                callback.readEntriesComplete(entries, ctx);
                recycle();
            }));
        }
    }

    public int getNumberOfEntriesToRead() {
        return count - entries.size();
    }

    public boolean isSlowestReader() {
        return cursor.ledger.getSlowestConsumer() == cursor;
    }

    private final Handle recyclerHandle;

    private DlogBasedOpReadEntry(Handle recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<DlogBasedOpReadEntry> RECYCLER = new Recycler<DlogBasedOpReadEntry>() {
        protected DlogBasedOpReadEntry newObject(Handle recyclerHandle) {
            return new DlogBasedOpReadEntry(recyclerHandle);
        }
    };

    public void recycle() {
        cursor = null;
        readPosition = null;
        callback = null;
        ctx = null;
        entries = null;
        nextReadPosition = null;
        RECYCLER.recycle(this, recyclerHandle);
    }

    private static final Logger log = LoggerFactory.getLogger(DlogBasedOpReadEntry.class);
}
