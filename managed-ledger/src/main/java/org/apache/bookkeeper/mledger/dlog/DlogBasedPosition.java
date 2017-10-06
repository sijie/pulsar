package org.apache.bookkeeper.mledger.dlog;

import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.distributedlog.DLSN;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * manage dlog DLSN, when entry aggregate buffer=1,
 * LogSegmentSequenceNo-> ledgerId(bk), EntryId -> EntryId(pulsar)
 *
 */
public class DlogBasedPosition implements Position, Comparable<DlogBasedPosition>{
    private DLSN dlsn;

    public DlogBasedPosition(long logSegmentSequenceNo, long entryId, long slotId){
        dlsn = new DLSN(logSegmentSequenceNo, entryId, slotId);
    }
    public DlogBasedPosition(long logSegmentSequenceNo, long entryId){
        dlsn = new DLSN(logSegmentSequenceNo, entryId, 0);
    }
    public static Position earliest = new DlogBasedPosition(-1, -1, -1);
    public static Position latest = new DlogBasedPosition(Long.MAX_VALUE, Long.MAX_VALUE, 0);

    // construct from metadata in zk
    public DlogBasedPosition(MLDataFormats.NestedPositionInfo npi) {
        this.dlsn = new DLSN(npi.getLedgerId(),npi.getEntryId(),0);
    }
    // construct from metadata in zk
    public DlogBasedPosition(MLDataFormats.PositionInfo pi) {
        this.dlsn = new DLSN(pi.getLedgerId(),pi.getEntryId(),0);
    }
    public DlogBasedPosition(DLSN dlsn){
        checkNotNull(dlsn);
        this.dlsn = dlsn;
    }
    public DlogBasedPosition(DlogBasedPosition dlogBasedPosition){
        this.dlsn = dlogBasedPosition.dlsn;
    }
    public static DlogBasedPosition get(long logSegmentSequenceNo, long entryId, long slotId) {
        return new DlogBasedPosition(logSegmentSequenceNo, entryId, slotId);
    }
    public static DlogBasedPosition get(long logSegmentSequenceNo, long entryId) {
        return new DlogBasedPosition(logSegmentSequenceNo, entryId, 0);
    }
    public static DlogBasedPosition get(DLSN dlsn) {
        return new DlogBasedPosition(dlsn);
    }

    public static DlogBasedPosition get(DlogBasedPosition other) {
        return new DlogBasedPosition(other);
    }

    public DLSN getDlsn(){return dlsn;}
    @Override
    public DlogBasedPosition getNext() {
        return new DlogBasedPosition(new DLSN(this.dlsn.getLogSegmentSequenceNo(), this.dlsn.getEntryId() + 1,0));
    }

    public long getLedgerId(){
        return dlsn.getLogSegmentSequenceNo();
    }
    public long getEntryId(){
        return dlsn.getEntryId();
    }

    @Override
    public int compareTo(DlogBasedPosition o) {

        return dlsn.compareTo(o.dlsn);
    }
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DlogBasedPosition) {
            return dlsn.equals(((DlogBasedPosition) obj).dlsn);
        }

        return false;
    }
    @Override
    public int hashCode() {
        //todo is this ok?
        return dlsn.hashCode();
    }
    @Override
    public String toString(){
        return dlsn.toString();
    }
}
