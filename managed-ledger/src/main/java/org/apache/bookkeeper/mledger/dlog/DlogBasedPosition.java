package org.apache.bookkeeper.mledger.dlog;

import org.apache.bookkeeper.mledger.Position;
import org.apache.distributedlog.DLSN;

/**
 * manage dlog DLSN
 *
 */
public class DlogBasedPosition implements Position, Comparable<DlogBasedPosition>{
    private DLSN dlsn;

    public DlogBasedPosition(long logSegmentSequenceNo, long entryId, long slotId){
        dlsn = new DLSN(logSegmentSequenceNo, entryId, slotId);
    }
    public DlogBasedPosition(DLSN dlsn){
        this.dlsn = dlsn;
    }
    public DlogBasedPosition(DlogBasedPosition dlogBasedPosition){
        this.dlsn = dlogBasedPosition.dlsn;
    }
    public static DlogBasedPosition get(long logSegmentSequenceNo, long entryId, long slotId) {
        return new DlogBasedPosition(logSegmentSequenceNo, entryId, slotId);
    }

    public static DlogBasedPosition get(DlogBasedPosition other) {
        return new DlogBasedPosition(other);
    }

    public DLSN getDlsn(){return dlsn;}
    @Override
    public Position getNext() {

        return new DlogBasedPosition(dlsn.getNextDLSN());
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
}
