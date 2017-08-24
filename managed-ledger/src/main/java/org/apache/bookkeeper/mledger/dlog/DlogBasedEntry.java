package org.apache.bookkeeper.mledger.dlog;

import io.netty.buffer.ByteBuf;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import io.netty.util.ReferenceCounted;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.distributedlog.DLSN;
import org.apache.distributedlog.LogRecord;
import org.apache.distributedlog.LogRecordWithDLSN;

/**
 * Created by yaoguangzhong on 2017/8/17.
 */

final class DlogBasedEntry extends AbstractReferenceCounted implements Entry, Comparable<org.apache.bookkeeper.mledger.dlog.DlogBasedEntry>
{
    private static final Recycler<DlogBasedEntry> RECYCLER = new Recycler<DlogBasedEntry>() {
        @Override
        protected DlogBasedEntry newObject(Handle handle) {
            return new DlogBasedEntry(handle);
        }
    };

    private final Recycler.Handle recyclerHandle;
    private DLSN dlsn;

    ByteBuf data;

    private DlogBasedEntry(Recycler.Handle recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    public static DlogBasedEntry create(LogRecordWithDLSN logRecord) {
        DlogBasedEntry entry = RECYCLER.get();
        entry.dlsn = logRecord.getDlsn();
        entry.data = logRecord.getPayload();
        entry.data.retain();
        entry.setRefCnt(1);
        return entry;
    }

    // Used just for tests
    public static DlogBasedEntry create(long ledgerId, long entryId, byte[] data) {
        EntryImpl entry = RECYCLER.get();
        entry.ledgerId = ledgerId;
        entry.entryId = entryId;
        entry.data = Unpooled.wrappedBuffer(data);
        entry.setRefCnt(1);
        return entry;
    }

    public static DlogBasedEntry create(long ledgerId, long entryId, ByteBuf data) {
        EntryImpl entry = RECYCLER.get();
        entry.ledgerId = ledgerId;
        entry.entryId = entryId;
        entry.data = data;
        entry.data.retain();
        entry.setRefCnt(1);
        return entry;
    }

    public static DlogBasedEntry create(PositionImpl position, ByteBuf data) {
        EntryImpl entry = RECYCLER.get();
        entry.ledgerId = position.getLedgerId();
        entry.entryId = position.getEntryId();
        entry.data = data;
        entry.data.retain();
        entry.setRefCnt(1);
        return entry;
    }

    public static DlogBasedEntry create(EntryImpl other) {
        EntryImpl entry = RECYCLER.get();
        entry.ledgerId = other.ledgerId;
        entry.entryId = other.entryId;
        entry.data = RecyclableDuplicateByteBuf.create(other.data);
        entry.setRefCnt(1);
        return entry;
    }


    @Override
    protected void deallocate() {
        data.release();
        data = null;
        dlsn = null;
        RECYCLER.recycle(this, recyclerHandle);
    }

    @Override
    public int compareTo(DlogBasedEntry o) {
        return dlsn.compareTo(o.dlsn);
    }

    @Override
    public byte[] getData() {

        byte[] array = new byte[(int) data.readableBytes()];
        data.getBytes(data.readerIndex(), array);
        return array;
    }

    @Override
    public byte[] getDataAndRelease() {

        byte[] array = getData();
        release();
        return array;
    }

    @Override
    public int getLength() {
        return data.readableBytes();
    }

    @Override
    public ByteBuf getDataBuffer() {
        return data;
    }

    @Override
    public Position getPosition() {
        return new DlogBasedPosition(dlsn);
    }

    //todo remove getLedgerId and getEntryId in Entry
    @Override
    public long getLedgerId() {
        return 0;
    }

    @Override
    public long getEntryId() {
        return 0;
    }
}
