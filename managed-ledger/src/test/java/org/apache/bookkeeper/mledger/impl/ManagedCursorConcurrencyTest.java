/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.mledger.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursor.IndividualDeletedEntries;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

public class ManagedCursorConcurrencyTest extends MockedBookKeeperTestCase {

    Executor executor = Executors.newCachedThreadPool();
    private static final Logger log = LoggerFactory.getLogger(ManagedCursorConcurrencyTest.class);

    private final AsyncCallbacks.DeleteCallback deleteCallback = new AsyncCallbacks.DeleteCallback() {
        @Override
        public void deleteComplete(Object ctx) {
            log.info("Deleted message at {}", ctx);
        }

        @Override
        public void deleteFailed(ManagedLedgerException exception, Object ctx) {
            log.error("Failed to delete message at {}", ctx, exception);
        }
    };

    @Test
    public void testMarkDeleteAndRead() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(2));

        final ManagedCursor cursor = ledger.openCursor("c1");

        final List<Position> addedEntries = Lists.newArrayList();

        for (int i = 0; i < 1000; i++) {
            Position pos = ledger.addEntry("entry".getBytes());
            addedEntries.add(pos);
        }

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch counter = new CountDownLatch(2);
        final AtomicBoolean gotException = new AtomicBoolean(false);

        Thread deleter = new Thread() {
            public void run() {
                try {
                    barrier.await();

                    for (Position position : addedEntries) {
                        cursor.markDelete(position);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        Thread reader = new Thread() {
            public void run() {
                try {
                    barrier.await();

                    for (int i = 0; i < 1000; i++) {
                        cursor.readEntries(1).forEach(e -> e.release());
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        deleter.start();
        reader.start();

        counter.await();

        assertEquals(gotException.get(), false);
    }

    @Test
    public void testCloseAndRead() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger_test_close_and_read",
                new ManagedLedgerConfig().setMaxEntriesPerLedger(2));

        final ManagedCursor cursor = ledger.openCursor("c1");
        final CompletableFuture<String> closeFuture = new CompletableFuture<>();
        final String CLOSED = "closed";

        final List<Position> addedEntries = Lists.newArrayList();

        for (int i = 0; i < 1000; i++) {
            Position pos = ledger.addEntry("entry".getBytes());
            addedEntries.add(pos);
        }

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch counter = new CountDownLatch(2);
        final AtomicBoolean gotException = new AtomicBoolean(false);

        Thread deleter = new Thread() {
            public void run() {
                try {
                    barrier.await();

                    for (Position position : addedEntries) {
                        cursor.markDelete(position);
                        Thread.sleep(1);
                    }
                } catch (ManagedLedgerException e) {
                    if (!e.getMessage().equals("Cursor was already closed")) {
                        gotException.set(true);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        Thread reader = new Thread() {
            public void run() {
                try {
                    barrier.await();

                    for (int i = 0; i < 1000; i++) {
                        cursor.readEntries(1).forEach(e -> e.release());
                        // Thread.sleep(2,200);
                        Thread.sleep(2, 195);
                    }
                    cursor.asyncClose(new AsyncCallbacks.CloseCallback() {
                        @Override
                        public void closeComplete(Object ctx) {
                            log.info("Successfully closed cursor ledger");
                            closeFuture.complete(CLOSED);
                        }

                        @Override
                        public void closeFailed(ManagedLedgerException exception, Object ctx) {
                            log.error("Error closing cursor: ", exception);
                            closeFuture.completeExceptionally(new Exception(exception));
                        }
                    }, null);

                } catch (Exception e) {
                    e.printStackTrace();
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        deleter.start();
        reader.start();

        counter.await();

        assertEquals(gotException.get(), false);
        assertEquals(closeFuture.get(), CLOSED);
    }

    @Test
    public void testAckAndClose() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger_test_ack_and_close",
                new ManagedLedgerConfig().setMaxEntriesPerLedger(2));

        final ManagedCursor cursor = ledger.openCursor("c1");

        final List<Position> addedEntries = Lists.newArrayList();

        for (int i = 0; i < 1000; i++) {
            Position pos = ledger.addEntry("entry".getBytes());
            addedEntries.add(pos);
        }

        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch counter = new CountDownLatch(2);
        final AtomicBoolean gotException = new AtomicBoolean(false);

        Thread deleter = new Thread() {
            public void run() {
                try {
                    barrier.await();

                    for (Position position : addedEntries) {
                        cursor.asyncDelete(position, deleteCallback, position);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };

        Thread reader = new Thread() {
            public void run() {
                try {
                    barrier.await();

                    for (int i = 0; i < 1000; i++) {
                        cursor.readEntries(1).forEach(e -> e.release());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }
            }
        };
        System.out.println("starting deleter and reader.." + System.currentTimeMillis());
        deleter.start();
        reader.start();

        counter.await();

        assertEquals(gotException.get(), false);
        System.out.println("Finished.." + System.currentTimeMillis());

    }

    @Test
    public void testConcurrentIndividualDeletes() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger", new ManagedLedgerConfig().setMaxEntriesPerLedger(100));

        final ManagedCursor cursor = ledger.openCursor("c1");

        final int N = 1000;
        final List<Position> addedEntries = Lists.newArrayListWithExpectedSize(N);

        for (int i = 0; i < N; i++) {
            Position pos = ledger.addEntry("entry".getBytes());
            addedEntries.add(pos);
        }

        final int Threads = 10;
        final CyclicBarrier barrier = new CyclicBarrier(Threads);
        final CountDownLatch counter = new CountDownLatch(Threads);
        final AtomicBoolean gotException = new AtomicBoolean(false);

        for (int thread = 0; thread < Threads; thread++) {
            final int myThread = thread;
            executor.execute(new Runnable() {
                public void run() {
                    try {
                        barrier.await();

                        for (int i = 0; i < N; i++) {
                            int threadId = i % Threads;
                            if (threadId == myThread) {
                                cursor.delete(addedEntries.get(i));
                            }
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                        gotException.set(true);
                    } finally {
                        counter.countDown();
                    }
                }
            });
        }

        counter.await();

        assertEquals(gotException.get(), false);
        assertEquals(cursor.getMarkDeletedPosition(), addedEntries.get(addedEntries.size() - 1));
    }

    @Test
    public void testConcurrentReadOfSameEntry() throws Exception {
        ManagedLedger ledger = factory.open("testConcurrentReadOfSameEntry", new ManagedLedgerConfig());
        final int numCursors = 5;
        final List<ManagedCursor> cursors = Lists.newArrayList();
        for (int i = 0; i < numCursors; i++) {
            final ManagedCursor cursor = ledger.openCursor("c" + i);
            cursors.add(cursor);
        }

        final int N = 100;
        for (int i = 0; i < N; i++) {
            ledger.addEntry(("entry" + i).getBytes());
        }
        long currentLedger = ((PositionImpl) cursors.get(0).getMarkDeletedPosition()).getLedgerId();

        // empty the cache
        ((ManagedLedgerImpl) ledger).entryCache.invalidateAllEntries(currentLedger);

        final CyclicBarrier barrier = new CyclicBarrier(numCursors);
        final CountDownLatch counter = new CountDownLatch(numCursors);

        AtomicReference<String> result = new AtomicReference<String>();

        for (int i = 0; i < numCursors; i++) {
            final int cursorIndex = i;
            final ManagedCursor cursor = cursors.get(cursorIndex);
            executor.execute(() -> {
                try {
                    barrier.await();
                    for (int j = 0; j < N; j++) {
                        String expected = "entry" + j;
                        String data = new String(cursor.readEntries(1).get(0).getDataAndRelease());
                        if ((!expected.equals(data)) && result.get() == null) {
                            result.set("Mismatched entry in cursor " + (cursorIndex + 1) + " at position " + (j + 1)
                                    + "--- Expected: " + expected + ", Actual: " + data);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    counter.countDown();
                }
            });
        }

        counter.await();
        assertNull(result.get());
    }

    @Test
    public void testConcurrentIndividualDeletesWithGetNthEntry() throws Exception {
        ManagedLedger ledger = factory.open("my_test_ledger",
                new ManagedLedgerConfig().setMaxEntriesPerLedger(100).setThrottleMarkDelete(0.5));

        final ManagedCursor cursor = ledger.openCursor("c1");

        final int N = 1000;
        final List<Position> addedEntries = Lists.newArrayListWithExpectedSize(N);

        for (int i = 0; i < N; i++) {
            Position pos = ledger.addEntry("entry".getBytes());
            addedEntries.add(pos);
        }

        final int deleteEntries = 100;
        final CountDownLatch counter = new CountDownLatch(deleteEntries);
        final AtomicBoolean gotException = new AtomicBoolean(false);
        final AtomicInteger iteration = new AtomicInteger(0);

        for (int i = 0; i < deleteEntries; i++) {
            executor.execute(() -> {
                try {
                    cursor.asyncDelete(addedEntries.get(iteration.getAndIncrement()), new DeleteCallback() {
                        @Override
                        public void deleteComplete(Object ctx) {
                            log.info("Successfully deleted cursor");
                        }

                        @Override
                        public void deleteFailed(ManagedLedgerException exception, Object ctx) {
                            exception.printStackTrace();
                            gotException.set(true);
                        }
                    }, null);
                } catch (Exception e) {
                    e.printStackTrace();
                    gotException.set(true);
                } finally {
                    counter.countDown();
                }

            });
        }

        counter.await();

        final int readEntries = N - deleteEntries;
        final CountDownLatch readCounter = new CountDownLatch(readEntries);
        final AtomicInteger successReadEntries = new AtomicInteger(0);
        for (int i = 1; i <= readEntries; i++) {
            try {
                cursor.asyncGetNthEntry(i, IndividualDeletedEntries.Exclude, new ReadEntryCallback() {
                    @Override
                    public void readEntryComplete(Entry entry, Object ctx) {
                        successReadEntries.getAndIncrement();
                        entry.release();
                    }

                    @Override
                    public void readEntryFailed(ManagedLedgerException exception, Object ctx) {
                        exception.printStackTrace();
                        gotException.set(true);
                    }
                }, null);
            } catch (Exception e) {
                e.printStackTrace();
                gotException.set(true);
            } finally {
                readCounter.countDown();
            }
        }

        readCounter.await();

        assertEquals(gotException.get(), false);
        assertEquals(readEntries, successReadEntries.get());
    }
}
