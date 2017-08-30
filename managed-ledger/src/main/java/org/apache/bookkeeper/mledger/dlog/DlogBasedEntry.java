package org.apache.bookkeeper.mledger.dlog;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.RecyclableDuplicateByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.Recycler;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.distributedlog.DLSN;
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

    //todo is it ok to use unpool?
    public static DlogBasedEntry create(LogRecordWithDLSN logRecord) {
        DlogBasedEntry entry = RECYCLER.get();
        entry.dlsn = logRecord.getDlsn();
        entry.data = Unpooled.wrappedBuffer(logRecord.getPayload());
        entry.data.retain();
        entry.setRefCnt(1);
        return entry;
    }

    // Used just for tests, todo why not call entry.data.retain()?  Unpool related?
    public static DlogBasedEntry create(DLSN dlsn, byte[] data) {
        DlogBasedEntry entry = RECYCLER.get();
        entry.dlsn = dlsn;
        entry.data = Unpooled.wrappedBuffer(data);
        entry.setRefCnt(1);
        return entry;
    }

    public static DlogBasedEntry create(DLSN dlsn, ByteBuf data) {
        DlogBasedEntry entry = RECYCLER.get();
        entry.dlsn = dlsn;
        entry.data = data;
        entry.data.retain();
        entry.setRefCnt(1);
        return entry;
    }

    public static DlogBasedEntry create(DlogBasedPosition position, ByteBuf data) {
        DlogBasedEntry entry = RECYCLER.get();
        entry.dlsn = position.getDlsn();
        entry.data = data;
        entry.data.retain();
        entry.setRefCnt(1);
        return entry;
    }

    public static DlogBasedEntry create(DlogBasedEntry other) {
        DlogBasedEntry entry = RECYCLER.get();
        entry.dlsn = other.dlsn;
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
