package org.apache.bookkeeper.mledger.dlog;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.RecyclableDuplicateByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.common.concurrent.FutureEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Handles the life-cycle of an addEntry() operation
 *
 */
class DlogBasedOpAddEntry extends SafeRunnable implements FutureEventListener<DLSN> {
    private DlogBasedManagedLedger ml;
    private AsyncLogWriter asyncLogWriter;
    private DLSN dlsn;

    @SuppressWarnings("unused")
    private volatile AddEntryCallback callback;
    private Object ctx;
    private long startTime;
    ByteBuf data;
    private int dataLength;

    private static final AtomicReferenceFieldUpdater<DlogBasedOpAddEntry, AddEntryCallback> callbackUpdater = AtomicReferenceFieldUpdater
            .newUpdater(DlogBasedOpAddEntry.class, AddEntryCallback.class, "callback");

    public static DlogBasedOpAddEntry create(DlogBasedManagedLedger ml, ByteBuf data, AsyncLogWriter asyncLogWriter, AddEntryCallback callback, Object ctx) {
        DlogBasedOpAddEntry op = RECYCLER.get();
        op.ml = ml;
        op.asyncLogWriter = asyncLogWriter;
        op.data = data.retain();
        op.dataLength = data.readableBytes();
        op.callback = callback;
        op.ctx = ctx;
        op.dlsn = null;
        op.startTime = System.nanoTime();
        ml.mbean.addAddEntrySample(op.dataLength);
        if (log.isDebugEnabled()) {
            log.debug("Created new OpAddEntry {}", op);
        }
        return op;
    }



    public void initiate() {
        ByteBuf duplicateBuffer = RecyclableDuplicateByteBuf.create(data);
        // duplicatedBuffer has refCnt=1 at this point
//        asyncLogWriter.write(new LogRecord(System.currentTimeMillis(),duplicateBuffer.array())).whenComplete(this);
        asyncLogWriter.write(new LogRecord(System.currentTimeMillis(),duplicateBuffer)).whenComplete(this);

        // Internally, asyncAddEntry() is refCnt neutral to respect to the passed buffer and it will keep a ref on it
        // until is done using it. We need to release this buffer here to balance the 1 refCnt added at the creation
        // time.
        duplicateBuffer.release();
    }

    public void failed(ManagedLedgerException e) {
        AddEntryCallback cb = callbackUpdater.getAndSet(this, null);
        if (cb != null) {
            cb.addFailed(e, ctx);
            ml.mbean.recordAddEntryError();
        }
    }



    // Called in exector hashed on managed ledger name, once the add operation is complete
    @Override
    public void safeRun() {
        // Remove this entry from the head of the pending queue
        DlogBasedOpAddEntry firstInQueue = ml.pendingAddEntries.poll();
        checkArgument(this == firstInQueue);

        DlogBasedManagedLedger.NUMBER_OF_ENTRIES_UPDATER.incrementAndGet(ml);
        DlogBasedManagedLedger.TOTAL_SIZE_UPDATER.addAndGet(ml, dataLength);
        if (ml.hasActiveCursors()) {
            // Avoid caching entries if no cursor has been created
            DlogBasedEntry entry = DlogBasedEntry.create(dlsn, data);
            // EntryCache.insert: duplicates entry by allocating new entry and data. so, recycle entry after calling
            // insert
            ml.entryCache.insert(entry);
            entry.release();
        }

        // We are done using the byte buffer
        data.release();
        PositionImpl lastEntry = PositionImpl.get(dlsn);
        DlogBasedManagedLedger.ENTRIES_ADDED_COUNTER_UPDATER.incrementAndGet(ml);
        ml.lastConfirmedEntry = lastEntry;

        updateLatency();
        AddEntryCallback cb = callbackUpdater.getAndSet(this, null);
        if (cb != null) {
            cb.addComplete(lastEntry, ctx);
            ml.notifyCursors();
            this.recycle();
        }
    }

    private void updateLatency() {
        ml.mbean.addAddEntryLatencySample(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
    }

    private final Handle recyclerHandle;

    private DlogBasedOpAddEntry(Handle recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<DlogBasedOpAddEntry> RECYCLER = new Recycler<DlogBasedOpAddEntry>() {
        protected DlogBasedOpAddEntry newObject(Handle recyclerHandle) {
            return new DlogBasedOpAddEntry(recyclerHandle);
        }
    };

    public void recycle() {
        ml = null;
        dlsn = null;
        asyncLogWriter = null;
        data = null;
        dataLength = -1;
        callback = null;
        ctx = null;
        startTime = -1;
        RECYCLER.recycle(this, recyclerHandle);
    }

    private static final Logger log = LoggerFactory.getLogger(DlogBasedOpAddEntry.class);

    @Override
    public void onSuccess(DLSN dlsn) {
        this.dlsn = dlsn;
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] write-complete: dlsn={} size={}", this, ml.getName(),
                    dlsn, dataLength);
        }
        // Trigger addComplete callback in a thread hashed on the managed ledger name
        ml.getExecutor().submitOrdered(ml.getName(), this);
    }

    @Override
    public void onFailure(Throwable throwable) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] [{}] write fail: dlsn={} size={}", this, ml.getName(),
                    dlsn, dataLength);
        }
        AddEntryCallback cb = callbackUpdater.getAndSet(this, null);
        if (cb != null) {
            cb.addFailed(new ManagedLedgerException(throwable), ctx);
            ml.mbean.recordAddEntryError();
        }
        // todo when to deal failure(start new log writer)
        // according to the type of throwable, do different handle
//        ml.dealAddFailure();
    }
}
