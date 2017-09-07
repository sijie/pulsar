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

import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo;

import java.util.List;

/**
 * Interface that describes the operations that the ManagedLedger need to do on the metadata store.
 */
public interface DlogBasedMetaStore {

    public static interface Stat {
        int getVersion();
        long getCreationTimestamp();
        long getModificationTimestamp();
    }

    public static interface UpdateLedgersIdsCallback {
        void updateLedgersIdsComplete(MetaStoreException status, Stat stat);
    }

    public static interface MetaStoreCallback<T> {
        void operationComplete(T result, Stat stat);

        void operationFailed(MetaStoreException e);
    }

    /**
     * Get the metadata used by the ManagedLedger
     *
     * @param ledgerName
     *            the name of the ManagedLedger
     * @return a version object and a list of LedgerStats
     * @throws MetaStoreException
     */
    void getManagedLedgerInfo(String ledgerName, MetaStoreCallback<ManagedLedgerInfo> callback);

    /**
     *
     * @param ledgerName
     *            the name of the ManagedLedger
     *
     * @param mlInfo
     *            the metadata object to be persisted
     * @param version
     *            version object associated with current state
     * @param callback
     *            callback object
     */
    void asyncUpdateLedgerIds(String ledgerName, ManagedLedgerInfo mlInfo, Stat version,
                              MetaStoreCallback<Void> callback);

    /**
     * Get the list of cursors registered on a ManagedLedger.
     *
     * @param ledgerName
     *            the name of the ManagedLedger
     * @return a list of the consumer Ids
     * @throws MetaStoreException
     */
    void getCursors(String ledgerName, MetaStoreCallback<List<String>> callback);

    /**
     * Get the ledger id associated with a cursor.
     *
     * This ledger id will contains the mark-deleted position for the cursor.
     *
     * @param ledgerName
     * @param cursorName
     * @param callback
     */
    void asyncGetCursorInfo(String ledgerName, String cursorName, MetaStoreCallback<ManagedCursorInfo> callback);

    /**
     * Update the persisted position of a cursor
     *
     * @param ledgerName
     *            the name of the ManagedLedger
     * @param cursorName
     * @param info
     * @param callback
     *            the callback
     * @throws MetaStoreException
     */
    void asyncUpdateCursorInfo(String ledgerName, String cursorName, ManagedCursorInfo info, Stat version,
                               MetaStoreCallback<Void> callback);

    /**
     * Drop the persistent state of a consumer from the metadata store
     *
     * @param ledgerName
     *            the name of the ManagedLedger
     * @param cursorName
     *            the cursor name
     * @param callback
     *            callback object
     */
    void asyncRemoveCursor(String ledgerName, String cursorName, MetaStoreCallback<Void> callback);

    /**
     * Drop the persistent state for the ManagedLedger and all its associated consumers.
     *
     * @param ledgerName
     *            the name of the ManagedLedger
     * @param callback
     *            callback object
     */
    void removeManagedLedger(String ledgerName, MetaStoreCallback<Void> callback);

    /**
     * Get a list of all the managed ledgers in the system
     *
     * @return an Iterable of the names of the managed ledgers
     * @throws MetaStoreException
     */
    Iterable<String> getManagedLedgers() throws MetaStoreException;
}
