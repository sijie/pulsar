package org.apache.bookkeeper.mledger.dlog;

import org.apache.distributedlog.DLSN;

import java.util.List;
import java.util.Map;

/**
 * Created by yaoguangzhong on 2017/8/22.
 */
public class DlogBasedManagedLedgerInfo {
    /** Z-Node version */
    public int version;
    public String creationDate;
    public String modificationDate;


    public Map<String, DlogBasedCursorInfo> cursors;



    public static class DlogBasedCursorInfo {
        /** Z-Node version */
        public int version;
        public String creationDate;
        public String modificationDate;

        // If the ledger id is -1, then the mark-delete position is
        // the one from the (ledgerId, entryId) snapshot below
        public long cursorsLedgerId;
        // todo deal deletion semantics carefully
        // Last snapshot of the mark-delete position
        public DLSN markDelete;
        public List<MessageRangeInfo> individualDeletedMessages;
    }



    //todo range info currently use wrong initialization
    public static class MessageRangeInfo {
        // Starting of the range (not included)
        public DLSN from = new DLSN(-1,-1,-1);

        // End of the range (included)
        public DLSN to = new DLSN(-1,-1,-1);

        @Override
        public String toString() {
            return String.format("(%s, %s]", from, to);
        }
    }
}

