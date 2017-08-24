package org.apache.bookkeeper.mledger.dlog;

import io.netty.buffer.ByteBuf;
import org.apache.bookkeeper.mledger.*;

/**
 * Created by yaoguangzhong on 2017/8/17.
 * distributed log based managedLedger
 * one managedLedger relate to one dlog stream
 */
//todo use which dlog conf and core api
public class DlogBasedManagedLedger implements ManagedLedger{



    @Override
    public String getName() {
        return null;
    }

    @Override
    public Position addEntry(byte[] data) throws InterruptedException, ManagedLedgerException {
        return null;

    }

    @Override
    public void asyncAddEntry(byte[] data, AsyncCallbacks.AddEntryCallback callback, Object ctx) {

    }

    @Override
    public Position addEntry(byte[] data, int offset, int length) throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public void asyncAddEntry(byte[] data, int offset, int length, AsyncCallbacks.AddEntryCallback callback, Object ctx) {

    }

    @Override
    public void asyncAddEntry(ByteBuf buffer, AsyncCallbacks.AddEntryCallback callback, Object ctx) {

    }

    @Override
    public ManagedCursor openCursor(String name) throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position startCursorPosition) throws ManagedLedgerException {
        return null;
    }

    @Override
    public void asyncDeleteCursor(String name, AsyncCallbacks.DeleteCursorCallback callback, Object ctx) {

    }

    @Override
    public void deleteCursor(String name) throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncOpenCursor(String name, AsyncCallbacks.OpenCursorCallback callback, Object ctx) {

    }

    @Override
    public Iterable<ManagedCursor> getCursors() {
        return null;
    }

    @Override
    public Iterable<ManagedCursor> getActiveCursors() {
        return null;
    }

    @Override
    public long getNumberOfEntries() {
        return 0;
    }

    @Override
    public long getNumberOfActiveEntries() {
        return 0;
    }

    @Override
    public long getTotalSize() {
        return 0;
    }

    @Override
    public long getEstimatedBacklogSize() {
        return 0;
    }

    @Override
    public void checkBackloggedCursors() {

    }

    @Override
    public void close() throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx) {

    }

    @Override
    public ManagedLedgerMXBean getStats() {
        return null;
    }

    @Override
    public void delete() throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncDelete(AsyncCallbacks.DeleteLedgerCallback callback, Object ctx) {

    }

    @Override
    public ManagedCursor getSlowestConsumer() {
        return null;
    }
}
