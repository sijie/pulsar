package org.apache.bookkeeper.mledger.dlog;

import com.google.common.base.Predicate;
import org.apache.bookkeeper.mledger.*;

import java.util.List;
import java.util.Set;

/**
 * Created by yaoguangzhong on 2017/8/17.
 */
public class DlogBasedManagedCursor implements ManagedCursor {
    @Override
    public String getName() {
        return null;
    }

    @Override
    public List<Entry> readEntries(int numberOfEntriesToRead) throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public void asyncReadEntries(int numberOfEntriesToRead, AsyncCallbacks.ReadEntriesCallback callback, Object ctx) {

    }

    @Override
    public Entry getNthEntry(int N, IndividualDeletedEntries deletedEntries) throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public void asyncGetNthEntry(int N, IndividualDeletedEntries deletedEntries, AsyncCallbacks.ReadEntryCallback callback, Object ctx) {

    }

    @Override
    public List<Entry> readEntriesOrWait(int numberOfEntriesToRead) throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public void asyncReadEntriesOrWait(int numberOfEntriesToRead, AsyncCallbacks.ReadEntriesCallback callback, Object ctx) {

    }

    @Override
    public boolean cancelPendingReadRequest() {
        return false;
    }

    @Override
    public boolean hasMoreEntries() {
        return false;
    }

    @Override
    public long getNumberOfEntries() {
        return 0;
    }

    @Override
    public long getNumberOfEntriesInBacklog() {
        return 0;
    }

    @Override
    public void markDelete(Position position) throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncMarkDelete(Position position, AsyncCallbacks.MarkDeleteCallback callback, Object ctx) {

    }

    @Override
    public void delete(Position position) throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncDelete(Position position, AsyncCallbacks.DeleteCallback callback, Object ctx) {

    }

    @Override
    public Position getReadPosition() {
        return null;
    }

    @Override
    public Position getMarkDeletedPosition() {
        return null;
    }

    @Override
    public void rewind() {

    }

    @Override
    public void seek(Position newReadPosition) {

    }

    @Override
    public void clearBacklog() throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncClearBacklog(AsyncCallbacks.ClearBacklogCallback callback, Object ctx) {

    }

    @Override
    public void skipEntries(int numEntriesToSkip, IndividualDeletedEntries deletedEntries) throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncSkipEntries(int numEntriesToSkip, IndividualDeletedEntries deletedEntries, AsyncCallbacks.SkipEntriesCallback callback, Object ctx) {

    }

    @Override
    public Position findNewestMatching(Predicate<Entry> condition) throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public void asyncFindNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition, AsyncCallbacks.FindEntryCallback callback, Object ctx) {

    }

    @Override
    public void resetCursor(Position position) throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncResetCursor(Position position, AsyncCallbacks.ResetCursorCallback callback) {

    }

    @Override
    public List<Entry> replayEntries(Set<? extends Position> positions) throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public Set<? extends Position> asyncReplayEntries(Set<? extends Position> positions, AsyncCallbacks.ReadEntriesCallback callback, Object ctx) {
        return null;
    }

    @Override
    public void close() throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncClose(AsyncCallbacks.CloseCallback callback, Object ctx) {

    }

    @Override
    public Position getFirstPosition() {
        return null;
    }

    @Override
    public void setActive() {

    }

    @Override
    public void setInactive() {

    }

    @Override
    public boolean isActive() {
        return false;
    }

    @Override
    public boolean isDurable() {
        return false;
    }
}
