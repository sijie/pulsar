package org.apache.bookkeeper.mledger.dlog;

import org.apache.bookkeeper.mledger.*;

/**
 * Created by yaoguangzhong on 2017/8/17.
 */
public class DlogBasedManagedLedgerFactory implements ManagedLedgerFactory {
    @Override
    public ManagedLedger open(String name) throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public ManagedLedger open(String name, ManagedLedgerConfig config) throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public void asyncOpen(String name, AsyncCallbacks.OpenLedgerCallback callback, Object ctx) {

    }

    @Override
    public void asyncOpen(String name, ManagedLedgerConfig config, AsyncCallbacks.OpenLedgerCallback callback, Object ctx) {

    }

    @Override
    public ManagedLedgerInfo getManagedLedgerInfo(String name) throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public void asyncGetManagedLedgerInfo(String name, AsyncCallbacks.ManagedLedgerInfoCallback callback, Object ctx) {

    }

    @Override
    public void shutdown() throws InterruptedException, ManagedLedgerException {

    }
}
