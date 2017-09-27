package org.apache.bookkeeper.mledger.dlog;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.RateLimiter;
import dlshade.org.apache.bookkeeper.client.BookKeeper;
import dlshade.org.apache.bookkeeper.client.LedgerHandle;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.TerminateCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerFencedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerTerminatedException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.ManagedLedgerMXBean;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.dlog.DlogBasedManagedCursor.VoidCallback;
import org.apache.bookkeeper.mledger.impl.MetaStore;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.impl.MetaStore.Stat;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.NestedPositionInfo;
import org.apache.bookkeeper.mledger.util.CallbackMutex;
import org.apache.bookkeeper.mledger.util.Futures;
import org.apache.bookkeeper.mledger.util.Pair;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.UnboundArrayBlockingQueue;
import org.apache.distributedlog.BookKeeperClient;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.LogSegmentMetadata;
import org.apache.distributedlog.api.AsyncLogReader;
import org.apache.distributedlog.api.AsyncLogWriter;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.LogReader;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.callback.LogSegmentListener;
import org.apache.distributedlog.common.concurrent.FutureEventListener;
import org.apache.distributedlog.exceptions.LogEmptyException;
import org.apache.distributedlog.impl.BKNamespaceDriver;
import org.apache.distributedlog.namespace.NamespaceDriver;
import org.apache.pulsar.common.api.Commands;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;

public class DlogBasedManagedLedger implements ManagedLedger,FutureEventListener<AsyncLogWriter>,LogSegmentListener {

    protected final static int AsyncOperationTimeoutSeconds = 30;
    private final static long maxActiveCursorBacklogEntries = 100;
    private static long maxMessageCacheRetentionTimeMillis = 10 * 1000;

    private final String name;
    private final BookKeeper bookKeeper;

    private final DlogBasedManagedLedgerConfig config;
    private final MetaStore store;

    // ledger here is dlog log segment,
    // so the key is log segment sequence number, which is not equal to bk ledgerId
    private final NavigableMap<Long, LedgerInfo> ledgers = new ConcurrentSkipListMap<>();
    private volatile Stat ledgersStat;


    private final DlogBasedManagedCursorContainer cursors = new DlogBasedManagedCursorContainer();
    private final DlogBasedManagedCursorContainer activeCursors = new DlogBasedManagedCursorContainer();

    // Ever increasing counter of entries added
    static final AtomicLongFieldUpdater<DlogBasedManagedLedger> ENTRIES_ADDED_COUNTER_UPDATER =
            AtomicLongFieldUpdater.newUpdater(DlogBasedManagedLedger.class, "entriesAddedCounter");
    @SuppressWarnings("unused")
    private volatile long entriesAddedCounter = 0;

    static final AtomicLongFieldUpdater<DlogBasedManagedLedger> NUMBER_OF_ENTRIES_UPDATER =
            AtomicLongFieldUpdater.newUpdater(DlogBasedManagedLedger.class, "numberOfEntries");
    @SuppressWarnings("unused")
    private volatile long numberOfEntries = 0;
    static final AtomicLongFieldUpdater<DlogBasedManagedLedger> TOTAL_SIZE_UPDATER =
            AtomicLongFieldUpdater.newUpdater(DlogBasedManagedLedger.class, "totalSize");
    @SuppressWarnings("unused")
    private volatile long totalSize = 0;

    private RateLimiter updateCursorRateLimit;

    // Cursors that are waiting to be notified when new entries are persisted
    final ConcurrentLinkedQueue<DlogBasedManagedCursor> waitingCursors;

    // This map is used for concurrent open cursor requests, where the 2nd request will attach a listener to the
    // uninitialized cursor future from the 1st request
    final Map<String, CompletableFuture<ManagedCursor>> uninitializedCursors;

    final DlogBasedEntryCache entryCache;


    private final CallbackMutex trimmerMutex = new CallbackMutex();

    // the ledger here corresponding to the log segment in dlog
    private volatile long currentLedger;
    private long currentLedgerEntries = 0;
    private long currentLedgerSize = 0;
    private long lastLedgerCreatedTimestamp = 0;
    private long lastLedgerCreationFailureTimestamp = -1;

    // Time period in which new write requests will not be accepted, after we fail in creating a new ledger.
    // todo use it when queue write op
    final static long WaitTimeAfterLedgerCreationFailureMs = 10000;

    volatile DlogBasedPosition lastConfirmedEntry;
    // update slowest consuming position
    private DlogBasedPosition slowestPosition = null;


    enum State {
        None, // Uninitialized
        WriterOpened, // A log stream is ready to write into
        Closed, // ManagedLedger has been closed
        Fenced, // A managed ledger is fenced when there is some concurrent
        // access from a different session/machine. In this state the
        // managed ledger will throw exception for all operations, since
        // the new instance will take over, the fencing mechanism is like bk.
        Terminated, // Managed ledger was terminated and no more entries
        // are allowed to be added. Reads are allowed
    }

    // define boundaries for position based seeks and searches
    enum PositionBound {
        startIncluded, startExcluded
    }

    private static final AtomicReferenceFieldUpdater<DlogBasedManagedLedger, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(DlogBasedManagedLedger.class, State.class, "state");
    private volatile State state = null;

    private final ScheduledExecutorService scheduledExecutor;
    private final OrderedSafeExecutor executor;
    private final DlogBasedManagedLedgerFactory factory;
    protected final DlogBasedManagedLedgerMBean mbean;

    /**
     * Queue of pending entries to be added to the managed ledger. Typically entries are queued when a new ledger is
     * created asynchronously and hence there is no ready ledger to write into.
     */
    final Queue<DlogBasedOpAddEntry> pendingAddEntries = new UnboundArrayBlockingQueue<>();

    // managing dlog log stream
    private AsyncLogWriter asyncLogWriter;
    private DistributedLogManager dlm;
    private final Namespace dlNamespace;
    private final DistributedLogConfiguration dlConfig;

    public DlogBasedManagedLedger(DlogBasedManagedLedgerFactory factory, BookKeeper bookKeeper, Namespace namespace, DistributedLogConfiguration dlConfig,
                                  DlogBasedManagedLedgerConfig config, MetaStore store, ScheduledExecutorService scheduledExecutor, OrderedSafeExecutor orderedExecutor,
                                  final String name) {
        this.factory = factory;
        this.config = config;
        this.bookKeeper = bookKeeper;
        this.store = store;
        this.name = name;
        this.scheduledExecutor = scheduledExecutor;
        this.executor = orderedExecutor;
        this.dlNamespace = namespace;
        this.dlConfig = dlConfig;
        this.ledgersStat = null;

        TOTAL_SIZE_UPDATER.set(this, 0);
        NUMBER_OF_ENTRIES_UPDATER.set(this, 0);
        ENTRIES_ADDED_COUNTER_UPDATER.set(this, 0);
        STATE_UPDATER.set(this, State.None);
        this.mbean = new DlogBasedManagedLedgerMBean(this);
        this.entryCache = factory.getEntryCacheManager().getEntryCache(this);
        this.waitingCursors = Queues.newConcurrentLinkedQueue();
        this.uninitializedCursors = Maps.newHashMap();
        this.updateCursorRateLimit = RateLimiter.create(1);

    }


    synchronized void initialize(final ManagedLedgerInitializeLedgerCallback callback, final Object ctx)  throws IOException{
        log.info("Opening managed ledger {}", name);

        //todo is this check necessary, statsLogger now is empty
        if(dlNamespace.logExists(name))
        {
            dlm = dlNamespace.openLog(name,Optional.of(dlConfig),Optional.empty(),Optional.empty());
        }
        else {
            dlNamespace.createLog(name);
            dlm = dlNamespace.openLog(name,Optional.of(dlConfig),Optional.empty(),Optional.empty());
        }
        dlm.registerListener(this);

        store.getManagedLedgerInfo(name, new MetaStoreCallback<ManagedLedgerInfo>() {
            @Override
            public void operationComplete(ManagedLedgerInfo mlInfo, Stat stat) {
                ledgersStat = stat;
                if (mlInfo.hasTerminatedPosition()) {
                    state = State.Terminated;
                    lastConfirmedEntry = new DlogBasedPosition(mlInfo.getTerminatedPosition());
                    log.info("[{}] Recovering managed ledger terminated at {}", name, lastConfirmedEntry);
                }

                initializeLogWriter(callback);
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                callback.initializeFailed(new ManagedLedgerException(e));
            }
        });

    }

    /**
     * update local Ledgers from dlog, we should do this action when initialize ml and dlog logsegment medata change.
     * local ledgers is used to calculate stats
     *
     */
    private synchronized void updateLedgers(){
        int originalSize = ledgers.size();
        // Fetch the list of existing ledgers in the managed ledger
        List<LogSegmentMetadata> logSegmentMetadatas = null;
        try{
            logSegmentMetadatas =  dlm.getLogSegments();

        }catch (IOException e){
            log.error("[{}] getLogSegments failed in updateLedgers", name, e);
        }

        // first get bk client from dlog, because dlog not provide log segment size info
        NamespaceDriver driver = dlNamespace.getNamespaceDriver();
        assert(driver instanceof BKNamespaceDriver);
        BookKeeperClient bkc = ((BKNamespaceDriver) driver).getReaderBKC();
        long max = 0L;

        if(logSegmentMetadatas != null){
            LedgerHandle lh = null;
            if (log.isDebugEnabled()) {
                log.debug("[{}] logSegmentMetadatas's size is {} ", name, logSegmentMetadatas.size());
            }
            for(LogSegmentMetadata logSegment: logSegmentMetadatas){

                long logSegmentSequenceNumber = logSegment.getLogSegmentSequenceNumber();
                if(logSegmentSequenceNumber > max)
                    max = logSegmentSequenceNumber;

                LedgerInfo info = null;

                try{
                    lh = bkc.get().openLedgerNoRecovery(logSegment.getLogSegmentId(),
                            dlshade.org.apache.bookkeeper.client.BookKeeper.DigestType.CRC32, dlConfig.getBKDigestPW().getBytes(UTF_8));
                    info = LedgerInfo.newBuilder().setLedgerId(logSegment.getLogSegmentId()).setSize(lh.getLength())
                            .setEntries(logSegment.getRecordCount())
                            .setTimestamp(logSegment.getCompletionTime()).build();

                    lh.close();

                }catch (Exception e){
                    log.error("[{}] get bk client failed in updateLedgers", name, e);
                }

                ledgers.put(logSegment.getLogSegmentSequenceNumber(), info);
            }

            //update currentLedgerId
            currentLedger = max;

            // Calculate total entries and size
            NUMBER_OF_ENTRIES_UPDATER.set(DlogBasedManagedLedger.this,0);
            TOTAL_SIZE_UPDATER.set(DlogBasedManagedLedger.this,0);
            Iterator<LedgerInfo> iterator = ledgers.values().iterator();
            while (iterator.hasNext()) {
                LedgerInfo li = iterator.next();
                if (li.getEntries() > 0) {
                    NUMBER_OF_ENTRIES_UPDATER.addAndGet(this, li.getEntries());
                    TOTAL_SIZE_UPDATER.addAndGet(this, li.getSize());
                }
            }
            if (log.isDebugEnabled()) {
                log.debug("[{}] befor updateLedgers, ledger size is {}, after it's {} ", name, originalSize,ledgers.size());
            }
        }

    }
    /**
     * create dlog log writer to enable ml's writing ability
     *
     */
    private synchronized void initializeLogWriter(final ManagedLedgerInitializeLedgerCallback callback) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] initializing log writer.", name);
        }

        if (state == State.Terminated) {
            // When recovering a terminated managed ledger, we don't need to create
            // a new ledger for writing, since no more writes are allowed.
            // We just move on to the next stage
            initializeCursors(callback);
            return;
        }

        // Open a new log writer to response writing
        mbean.startDataLedgerCreateOp();
        dlm.openAsyncLogWriter().whenComplete(new FutureEventListener<AsyncLogWriter>() {
            @Override
            public void onSuccess(AsyncLogWriter asyncLogWriter) {
                DlogBasedManagedLedger.this.asyncLogWriter = asyncLogWriter;
                mbean.endDataLedgerCreateOp();
                log.info("[{}] Created log writer {}", name, asyncLogWriter.toString());
                lastLedgerCreatedTimestamp = System.currentTimeMillis();
                updateLedgers();
                try{
                    lastConfirmedEntry = new DlogBasedPosition(dlm.getLastDLSN());
                }catch (LogEmptyException lee){

                    // the stream has no entry, reset the lastConfirmedEntry
                    // todo is the first ledgerId always 0, updateLedgers set it to 0 in default
                    lastConfirmedEntry = new DlogBasedPosition(currentLedger,-1,0);
                    log.info("the log stream is empty {}, current lce is {}",lee.toString(),lastConfirmedEntry);
                }
                catch (IOException e){
                    log.error("Failed getLastDLSN in initializing log stream",e);
                }
                STATE_UPDATER.set(DlogBasedManagedLedger.this, State.WriterOpened);
                initializeCursors(callback);
            }

            @Override
            public void onFailure(Throwable throwable) {
                log.error("Failed open AsyncLogWriter for {}",name,throwable);
                callback.initializeFailed(new ManagedLedgerException(throwable));

            }
        });

    }

    private void initializeCursors(final ManagedLedgerInitializeLedgerCallback callback) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] initializing cursors", name);
        }
        store.getCursors(name, new MetaStoreCallback<List<String>>() {
            @Override
            public void operationComplete(List<String> consumers, Stat s) {
                // Load existing cursors
                final AtomicInteger cursorCount = new AtomicInteger(consumers.size());
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Found {} cursors", name, consumers.size());
                }

                if (consumers.isEmpty()) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}]  cursor is empty", name);
                    }
                    callback.initializeComplete();
                    return;
                }

                for (final String cursorName : consumers) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Loading cursor {}", name, cursorName);
                    }
                    final DlogBasedManagedCursor cursor;
                    cursor = new DlogBasedManagedCursor(bookKeeper, config, DlogBasedManagedLedger.this, cursorName);

                    cursor.recover(new VoidCallback() {
                        @Override
                        public void operationComplete() {
                            log.info("[{}] Recovery for cursor {} completed. pos={} -- todo={}", name, cursorName,
                                    cursor.getMarkDeletedPosition(), cursorCount.get() - 1);
                            cursor.setActive();
                            cursors.add(cursor);

                            if (cursorCount.decrementAndGet() == 0) {
                                // The initialization is now completed, register the jmx mbean
                                callback.initializeComplete();
                            }
                        }

                        @Override
                        public void operationFailed(ManagedLedgerException exception) {
                            log.warn("[{}] Recovery for cursor {} failed", name, cursorName, exception);
                            cursorCount.set(-1);
                            callback.initializeFailed(exception);
                        }
                    });
                }
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                log.warn("[{}] Failed to get the cursors list", name, e);
                callback.initializeFailed(new ManagedLedgerException(e));
            }
        });
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Position addEntry(byte[] data) throws InterruptedException, ManagedLedgerException {
        if (log.isDebugEnabled()) {
            log.debug("[{}] addEntry ", name);
        }
        return addEntry(data, 0, data.length);
    }

    @Override
    public Position addEntry(byte[] data, int offset, int length) throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        // Result list will contain the status exception and the resulting
        // position
        class Result {
            ManagedLedgerException status = null;
            Position position = null;
        }
        final Result result = new Result();

        asyncAddEntry(data, offset, length, new AddEntryCallback() {
            @Override
            public void addComplete(Position position, Object ctx) {
                result.position = position;
                counter.countDown();
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                result.status = exception;
                counter.countDown();
            }
        }, null);

        counter.await();

        if (result.status != null) {
            log.error("[{}] Error adding entry", name, result.status);
            throw result.status;
        }

        return result.position;
    }

    @Override
    public void asyncAddEntry(final byte[] data, final AddEntryCallback callback, final Object ctx) {
        asyncAddEntry(data, 0, data.length, callback, ctx);
    }

    @Override
    public void asyncAddEntry(final byte[] data, int offset, int length, final AddEntryCallback callback,
                              final Object ctx) {
        ByteBuf buffer = Unpooled.wrappedBuffer(data, offset, length);
        asyncAddEntry(buffer, callback, ctx);
    }

    @Override
    public synchronized void asyncAddEntry(ByteBuf buffer, AddEntryCallback callback, Object ctx) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] asyncAddEntry size={} state={}", name, buffer.readableBytes(), state);
        }
        final State state = STATE_UPDATER.get(this);
        if (state == State.Fenced) {
            callback.addFailed(new ManagedLedgerFencedException(), ctx);
            return;
        } else if (state == State.Terminated) {
            callback.addFailed(new ManagedLedgerTerminatedException("Managed ledger was already terminated"), ctx);
            return;
        } else if (state == State.Closed) {
            callback.addFailed(new ManagedLedgerException("Managed ledger was already closed"), ctx);
            return;
        }

        DlogBasedOpAddEntry addOperation = DlogBasedOpAddEntry.create(this, buffer, asyncLogWriter,callback, ctx);
        pendingAddEntries.add(addOperation);

        checkArgument(state == State.WriterOpened);

        ++currentLedgerEntries;
        currentLedgerSize += buffer.readableBytes();

        if (log.isDebugEnabled()) {
            log.debug("[{}] Write into current stream={} entries={}", name, asyncLogWriter.getStreamName(),
                    currentLedgerEntries);
        }
        addOperation.initiate();
    }

    @Override
    public ManagedCursor openCursor(String cursorName) throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedCursor cursor = null;
            ManagedLedgerException exception = null;
        }
        final Result result = new Result();

        asyncOpenCursor(cursorName, new OpenCursorCallback() {
            @Override
            public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                result.cursor = cursor;
                counter.countDown();
            }

            @Override
            public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }

        }, null);

        if (!counter.await(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            throw new ManagedLedgerException("Timeout during open-cursor operation");
        }

        if (result.exception != null) {
            log.error("Error adding entry", result.exception);
            throw result.exception;
        }

        return result.cursor;
    }

    @Override
    public synchronized void asyncOpenCursor(final String cursorName, final OpenCursorCallback callback,
                                             final Object ctx) {

        try {
            checkManagedLedgerIsOpen();
            checkFenced();
        } catch (ManagedLedgerException e) {
            callback.openCursorFailed(e, ctx);
            return;
        }

        if (uninitializedCursors.containsKey(cursorName)) {
            uninitializedCursors.get(cursorName).thenAccept(cursor -> {
                callback.openCursorComplete(cursor, ctx);
            }).exceptionally(ex -> {
                callback.openCursorFailed((ManagedLedgerException) ex, ctx);
                return null;
            });
            return;
        }
        ManagedCursor cachedCursor = cursors.get(cursorName);
        if (cachedCursor != null) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Cursor was already created {}", name, cachedCursor);
            }
            callback.openCursorComplete(cachedCursor, ctx);
            return;
        }

        // Create a new one and persist it
        if (log.isDebugEnabled()) {
            log.debug("[{}] Creating new cursor: {}", name, cursorName);
        }
        final DlogBasedManagedCursor cursor = new DlogBasedManagedCursor(bookKeeper, config, this, cursorName);
        CompletableFuture<ManagedCursor> cursorFuture = new CompletableFuture<>();
        uninitializedCursors.put(cursorName, cursorFuture);
        // Create a new one and persist it

        log.info("[{}] Before initialize the cursor, lastPosition is {}", name, getLastPosition());

        cursor.initialize(getLastPosition(), new VoidCallback() {
            @Override
            public void operationComplete() {
                log.info("[{}] Opened new cursor: {}", name, cursor);
                cursor.setActive();
                // Update the ack position (ignoring entries that were written while the cursor was being created)
                cursor.initializeCursorPosition(getLastPositionAndCounter());

                synchronized (this) {
                    cursors.add(cursor);
                    uninitializedCursors.remove(cursorName).complete(cursor);
                }
                callback.openCursorComplete(cursor, ctx);
            }

            @Override
            public void operationFailed(ManagedLedgerException exception) {
                log.warn("[{}] Failed to open cursor: {}", name, cursor);

                synchronized (this) {
                    uninitializedCursors.remove(cursorName).completeExceptionally(exception);
                }
                callback.openCursorFailed(exception, ctx);
            }
        });
    }

    @Override
    public synchronized void asyncDeleteCursor(final String consumerName, final DeleteCursorCallback callback,
                                               final Object ctx) {
        final DlogBasedManagedCursor cursor = (DlogBasedManagedCursor) cursors.get(consumerName);
        if (cursor == null) {
            callback.deleteCursorFailed(new ManagedLedgerException("ManagedCursor not found: " + consumerName), ctx);
            return;
        }

        // First remove the consumer form the MetaStore. If this operation succeeds and the next one (removing the
        // ledger from BK) don't, we end up having a loose ledger leaked but the state will be consistent.
        store.asyncRemoveCursor(DlogBasedManagedLedger.this.name, consumerName, new MetaStoreCallback<Void>() {
            @Override
            public void operationComplete(Void result, Stat stat) {
                cursor.asyncDeleteCursorLedger();
                cursors.removeCursor(consumerName);

                // Redo invalidation of entries in cache
                DlogBasedPosition slowestConsumerPosition = cursors.getSlowestReaderPosition();
                if (slowestConsumerPosition != null) {
                    if (log.isDebugEnabled()) {
                        log.debug("Doing cache invalidation up to {}", slowestConsumerPosition);
                    }
                    entryCache.invalidateEntries(slowestConsumerPosition);
                } else {
                    //why clear cache? 9-4
                    entryCache.clear();
                }

                trimConsumedLedgersInBackground();

                log.info("[{}] [{}] Deleted cursor", name, consumerName);
                callback.deleteCursorComplete(ctx);
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                callback.deleteCursorFailed(e, ctx);
            }

        });
    }

    @Override
    public void deleteCursor(String name) throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedLedgerException exception = null;
        }
        final Result result = new Result();

        asyncDeleteCursor(name, new DeleteCursorCallback() {
            @Override
            public void deleteCursorComplete(Object ctx) {
                counter.countDown();
            }

            @Override
            public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }

        }, null);

        if (!counter.await(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            throw new ManagedLedgerException("Timeout during delete-cursors operation");
        }

        if (result.exception != null) {
            log.error("Deleting cursor", result.exception);
            throw result.exception;
        }
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position startCursorPosition) throws ManagedLedgerException {
        checkManagedLedgerIsOpen();
        checkFenced();

        return new DlogBasedNonDurableCursor(bookKeeper, config, this, null, (DlogBasedPosition) startCursorPosition);
    }

    @Override
    public Iterable<ManagedCursor> getCursors() {
        return cursors;
    }

    @Override
    public Iterable<ManagedCursor> getActiveCursors() {
        return activeCursors;
    }

    /**
     * Tells whether the managed ledger has any active-cursor registered.
     *
     * @return true if at least a cursor exists
     */
    public boolean hasActiveCursors() {
        return !activeCursors.isEmpty();
    }

    @Override
    public long getNumberOfEntries() {
        return NUMBER_OF_ENTRIES_UPDATER.get(this);
    }

    @Override
    public long getNumberOfActiveEntries() {
        long totalEntries = getNumberOfEntries();
        DlogBasedPosition pos = cursors.getSlowestReaderPosition();
        if (pos == null) {
            // If there are no consumers, there are no active entries
            return 0;
        } else {
            // The slowest consumer will be in the first ledger in the list. We need to subtract the entries it has
            // already consumed in order to get the active entries count.
            return totalEntries - (pos.getEntryId() + 1);
        }
    }

    @Override
    public long getTotalSize() {
        return TOTAL_SIZE_UPDATER.get(this);
    }

    @Override
    public void checkBackloggedCursors() {

        // activate caught up cursors
        cursors.forEach(cursor -> {
            if (cursor.getNumberOfEntries() < maxActiveCursorBacklogEntries) {
                cursor.setActive();
            }
        });

        // deactivate backlog cursors
        Iterator<ManagedCursor> cursors = activeCursors.iterator();
        while (cursors.hasNext()) {
            ManagedCursor cursor = cursors.next();
            long backlogEntries = cursor.getNumberOfEntries();
            if (backlogEntries > maxActiveCursorBacklogEntries) {
                DlogBasedPosition readPosition = (DlogBasedPosition) cursor.getReadPosition();
                readPosition = isValidPosition(readPosition) ? readPosition : getNextValidPosition(readPosition);
                if (readPosition == null) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Couldn't find valid read position [{}] {}", name, cursor.getName(),
                                cursor.getReadPosition());
                    }
                    continue;
                }
                try {
                    asyncReadEntry(readPosition, new ReadEntryCallback() {

                        @Override
                        public void readEntryFailed(ManagedLedgerException e, Object ctx) {
                            log.warn("[{}] Failed while reading entries on [{}] {}", name, cursor.getName(),
                                    e.getMessage(), e);

                        }

                        @Override
                        public void readEntryComplete(Entry entry, Object ctx) {
                            MessageMetadata msgMetadata = null;
                            try {
                                msgMetadata = Commands.parseMessageMetadata(entry.getDataBuffer());
                                long msgTimeSincePublish = (System.currentTimeMillis() - msgMetadata.getPublishTime());
                                if (msgTimeSincePublish > maxMessageCacheRetentionTimeMillis) {
                                    cursor.setInactive();
                                }
                            } finally {
                                if (msgMetadata != null) {
                                    msgMetadata.recycle();
                                }
                                entry.release();
                            }

                        }
                    }, null);
                } catch (Exception e) {
                    log.warn("[{}] Failed while reading entries from cache on [{}] {}", name, cursor.getName(),
                            e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public long getEstimatedBacklogSize() {

        DlogBasedPosition pos = getMarkDeletePositionOfSlowestConsumer();

        while (true) {
            if (pos == null) {
                return 0;
            }
            long size = 0;
            final long slowestConsumerLedgerId = pos.getLedgerId();

            // Subtract size of ledgers that were already fully consumed but not trimmed yet
            synchronized (this) {
                size = getTotalSize();
                size -= ledgers.values().stream().filter(li -> li.getLedgerId() < slowestConsumerLedgerId)
                        .mapToLong(li -> li.getSize()).sum();
            }

            LedgerInfo ledgerInfo = null;
            synchronized (this) {
                ledgerInfo = ledgers.get(pos.getLedgerId());
            }
            if (ledgerInfo == null) {
                // ledger was removed
                if (pos.compareTo(getMarkDeletePositionOfSlowestConsumer()) == 0) {
                    // position still has not moved
                    return size;
                }
                // retry with new slowest consumer
                pos = getMarkDeletePositionOfSlowestConsumer();
                continue;
            }

            long numEntries = pos.getEntryId();
            if (ledgerInfo.getEntries() == 0) {
                size -= consumedLedgerSize(currentLedgerSize, currentLedgerEntries, numEntries);
                return size;
            } else {
                size -= consumedLedgerSize(ledgerInfo.getSize(), ledgerInfo.getEntries(), numEntries);
                return size;
            }
        }
    }

    private long consumedLedgerSize(long ledgerSize, long ledgerEntries, long consumedEntries) {
        if (ledgerEntries <= 0) {
            return 0;
        }
        long averageSize = ledgerSize / ledgerEntries;
        return consumedEntries >= 0 ? (consumedEntries + 1) * averageSize : 0;
    }

    @Override
    public synchronized void asyncTerminate(TerminateCallback callback, Object ctx) {
        if (state == State.Fenced) {
            callback.terminateFailed(new ManagedLedgerFencedException(), ctx);
            return;
        } else if (state == State.Terminated) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Ignoring request to terminate an already terminated managed ledger", name);
            }
            callback.terminateComplete(lastConfirmedEntry, ctx);
            return;
        }

        log.info("[{}] Terminating managed ledger", name);
        state = State.Terminated;

        if (log.isDebugEnabled()) {
            log.debug("[{}] Closing current stream={}", name, asyncLogWriter.getStreamName());
        }

        //todo change this stats
        mbean.startDataLedgerCloseOp();
        asyncLogWriter.asyncClose().whenComplete(new FutureEventListener<Void>() {
            @Override
            public void onSuccess(Void aVoid) {
                try{
                    lastConfirmedEntry = new DlogBasedPosition(dlm.getLastDLSN());
                }catch (IOException e){
                    log.info("[{}] Failed getLastDLSN when terminate the  managed ledger", name);

                }
                // Store the new state in metadata
                store.asyncUpdateLedgerIds(name, getManagedLedgerInfo(), ledgersStat, new MetaStoreCallback<Void>() {
                    @Override
                    public void operationComplete(Void result, Stat stat) {
                        ledgersStat = stat;
                        log.info("[{}] Terminated managed ledger at {}", name, lastConfirmedEntry);
                        callback.terminateComplete(lastConfirmedEntry, ctx);
                    }

                    @Override
                    public void operationFailed(MetaStoreException e) {
                        log.error("[{}] Failed to terminate managed ledger: {}", name, e.getMessage());
                        callback.terminateFailed(new ManagedLedgerException(e), ctx);
                    }
                });
            }

            @Override
            public void onFailure(Throwable throwable) {
                callback.terminateFailed(new ManagedLedgerException(throwable), ctx);

            }
        });

    }

    @Override
    public Position terminate() throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            Position lastPosition = null;
            ManagedLedgerException exception = null;
        }
        final Result result = new Result();

        asyncTerminate(new TerminateCallback() {
            @Override
            public void terminateComplete(Position lastPosition, Object ctx) {
                result.lastPosition = lastPosition;
                counter.countDown();
            }

            @Override
            public void terminateFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }

        }, null);

        if (!counter.await(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            throw new ManagedLedgerException("Timeout during managed ledger terminate");
        }

        if (result.exception != null) {
            log.error("[{}] Error terminating managed ledger", name, result.exception);
            throw result.exception;
        }

        return result.lastPosition;
    }

    @Override
    public boolean isTerminated() {
        return state == State.Terminated;
    }

    @Override
    public void close() throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        class Result {
            ManagedLedgerException exception = null;
        }
        final Result result = new Result();

        asyncClose(new CloseCallback() {
            @Override
            public void closeComplete(Object ctx) {
                counter.countDown();
            }

            @Override
            public void closeFailed(ManagedLedgerException exception, Object ctx) {
                result.exception = exception;
                counter.countDown();
            }

        }, null);

        if (!counter.await(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            throw new ManagedLedgerException("Timeout during managed ledger close");
        }

        if (result.exception != null) {
            log.error("[{}] Error closing managed ledger", name, result.exception);
            throw result.exception;
        }
    }

    @Override
    public synchronized void asyncClose(final CloseCallback callback, final Object ctx) {
        State state = STATE_UPDATER.get(this);
        if (state == State.Fenced) {
            factory.close(this);
            callback.closeFailed(new ManagedLedgerFencedException(), ctx);
            return;
        } else if (state == State.Closed) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Ignoring request to close a closed managed ledger", name);
            }
            callback.closeComplete(ctx);
            return;
        }

        log.info("[{}] Closing managed ledger", name);

        factory.close(this);
        STATE_UPDATER.set(this, State.Closed);


        if (asyncLogWriter == null) {
            // No writer to close, proceed with next step
            closeAllCursors(callback, ctx);
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("[{}] Closing log writer {}", name, asyncLogWriter.toString());
        }

        mbean.startDataLedgerCloseOp();
        asyncLogWriter.asyncClose().whenComplete(new FutureEventListener<Void>() {
            @Override
            public void onSuccess(Void aVoid) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Close complete for log writer {}", name, asyncLogWriter.toString());
                }
                mbean.endDataLedgerCloseOp();

                closeAllCursors(callback, ctx);
            }

            @Override
            public void onFailure(Throwable throwable) {
                callback.closeFailed(new ManagedLedgerException(throwable), ctx);

            }
        });
    }

    private void closeAllCursors(CloseCallback callback, final Object ctx) {
        // Close all cursors in parallel
        List<CompletableFuture<Void>> futures = Lists.newArrayList();
        for (ManagedCursor cursor : cursors) {
            Futures.CloseFuture closeFuture = new Futures.CloseFuture();
            cursor.asyncClose(closeFuture, null);
            futures.add(closeFuture);
        }

        Futures.waitForAll(futures).thenRun(() -> {
            callback.closeComplete(ctx);
        }).exceptionally(exception -> {
            callback.closeFailed(new ManagedLedgerException(exception), ctx);
            return null;
        });
    }

    // //////////////////////////////////////////////////////////////////////
    // Callbacks
    // open log writer callback
    @Override
    public void onFailure(Throwable throwable){
        log.error("[{}] Error creating writer {}", name, throwable);
        ManagedLedgerException status = new ManagedLedgerException(throwable);

        // Empty the list of pending requests and make all of them fail
        clearPendingAddEntries(status);
        lastLedgerCreationFailureTimestamp = System.currentTimeMillis();
        //let ml be fenced state, not service for writing anymore
        STATE_UPDATER.set(this, State.Fenced);
    }

    @Override
    public synchronized void onSuccess(AsyncLogWriter asyncLogWriter) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] createComplete writer={}", name, asyncLogWriter != null ? asyncLogWriter.toString() : -1);
        }
        mbean.endDataLedgerCreateOp();

        log.info("[{}] Created new writer {}", name, asyncLogWriter.toString());
        this.asyncLogWriter = asyncLogWriter;

        if(STATE_UPDATER.get(this) != State.WriterOpened)
            STATE_UPDATER.set(this, State.WriterOpened);

        // when open a new write, dlog will create a new ledger.
        lastLedgerCreatedTimestamp = System.currentTimeMillis();
        currentLedgerEntries = 0;
        currentLedgerSize = 0;

        if (log.isDebugEnabled()) {
            log.debug("[{}] Resending {} pending messages", name, pendingAddEntries.size());
        }

        // Process all the pending addEntry requests
        for (DlogBasedOpAddEntry op : pendingAddEntries) {
            ++currentLedgerEntries;
            currentLedgerSize += op.data.readableBytes();

            if (log.isDebugEnabled()) {
                log.debug("[{}] Sending {}", name, op);
            }

            op.initiate();

        }


    }

    //dlm metadata change callback, updateLedgers when metadata change.
    @Override
    public void onSegmentsUpdated(List<LogSegmentMetadata> segments) {

        lastLedgerCreatedTimestamp = System.currentTimeMillis();
        updateLedgers();
        if (log.isDebugEnabled()) {
            log.debug("[{}] dlog segment metadata update ", name);
        }
    }

    @Override
    public void onLogStreamDeleted() {

    }





    // //////////////////////////////////////////////////////////////////////
    // Private helpers

    //deal write fail event: close log writer, and creat a new one
    synchronized void dealAddFailure() {
        final State state = STATE_UPDATER.get(this);

        //no need to create a new one
        if (state != State.WriterOpened) {
            return;
        }


        trimConsumedLedgersInBackground();

        // Need to create a new writer to write pending entries
        if (log.isDebugEnabled()) {
            log.debug("[{}] Creating a new writer in dealAddFailure", name);
        }
        if(asyncLogWriter != null)
            asyncLogWriter.asyncClose();
        dlm.openAsyncLogWriter().whenComplete(this);


    }

    private void clearPendingAddEntries(ManagedLedgerException e) {
        while (!pendingAddEntries.isEmpty()) {
            DlogBasedOpAddEntry op = pendingAddEntries.poll();
            op.data.release();
            op.failed(e);
        }
    }


    void asyncReadEntries(DlogBasedOpReadEntry dlogBasedOpReadEntry) {
        final State state = STATE_UPDATER.get(this);
        if (state == State.Fenced || state == State.Closed) {
            dlogBasedOpReadEntry.readEntriesFailed(new ManagedLedgerFencedException(), dlogBasedOpReadEntry.ctx);
            return;
        }

        internalRead(dlogBasedOpReadEntry);
    }

    void asyncReadEntry(DlogBasedPosition position, ReadEntryCallback callback, Object ctx) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Reading entry ledger {}: {}", name, position.getLedgerId(), position.getEntryId());
        }
        AsyncLogReader logReader = null;
        try{

          logReader = dlm.getAsyncLogReader(position.getDlsn());

        }catch (IOException e){
            log.error("[{}] Opening log reader in asyncReadEntry fail {}", name,e);

        }
        if(logReader != null){
            entryCache.asyncReadEntry(logReader, position, callback, ctx);
        }


    }

    private void internalRead(DlogBasedOpReadEntry dlogBasedOpReadEntry) {

        // Perform the read
        long firstEntry = dlogBasedOpReadEntry.readPosition.getEntryId();
        long lastEntryInLedger = lastConfirmedEntry.getEntryId();
        final DlogBasedManagedCursor cursor = dlogBasedOpReadEntry.cursor;


        if (firstEntry > lastEntryInLedger) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] No more messages to read from lastEntry={} readEntry={}", name,
                        lastEntryInLedger, firstEntry);
            }
            return;
        }

        long lastEntry = min(firstEntry + dlogBasedOpReadEntry.getNumberOfEntriesToRead() - 1, lastEntryInLedger);

        AsyncLogReader logReader = null;
        try{

            logReader = dlm.getAsyncLogReader(dlogBasedOpReadEntry.readPosition.getDlsn());

        }catch (IOException e){
            log.error("[{}] Opening log reader in asyncReadEntry fail {}", name,e);

        }
        if(logReader != null){
            if (log.isDebugEnabled()) {
                log.debug("[{}] Reading entries from - first={} last={}", name, firstEntry,
                        lastEntry);
            }

            entryCache.asyncReadEntry(logReader,dlogBasedOpReadEntry.readPosition.getLedgerId(), firstEntry, lastEntry, false, dlogBasedOpReadEntry, dlogBasedOpReadEntry.ctx);

            if (updateCursorRateLimit.tryAcquire()) {
                if (isCursorActive(cursor)) {
                    final DlogBasedPosition lastReadPosition = DlogBasedPosition.get(dlogBasedOpReadEntry.readPosition.getLedgerId(), lastEntry);
                    discardEntriesFromCache(cursor, lastReadPosition);
                }
            }
        }

    }

    @Override
    public ManagedLedgerMXBean getStats() {
        return mbean;
    }


    void discardEntriesFromCache(DlogBasedManagedCursor cursor, DlogBasedPosition newPosition) {
        Pair<DlogBasedPosition, DlogBasedPosition> pair = activeCursors.cursorUpdated(cursor, newPosition);
        if (pair != null) {
            entryCache.invalidateEntries(pair.second);
        }
    }

    void updateCursor(DlogBasedManagedCursor cursor, DlogBasedPosition newPosition) {
        Pair<DlogBasedPosition, DlogBasedPosition> pair = cursors.cursorUpdated(cursor, newPosition);
        if (pair == null) {
            // Cursor has been removed in the meantime
            trimConsumedLedgersInBackground();
            return;
        }

        DlogBasedPosition previousSlowestReader = pair.first;
        DlogBasedPosition currentSlowestReader = pair.second;

        if (previousSlowestReader.compareTo(currentSlowestReader) == 0) {
            // The slowest consumer has not changed position. Nothing to do right now
            return;
        }

        // Only trigger a trimming when switching to the next ledger
        if (previousSlowestReader.getLedgerId() != newPosition.getLedgerId()) {
            trimConsumedLedgersInBackground();
        }
    }

    DlogBasedPosition startReadOperationOnLedger(DlogBasedPosition position) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] original position is {} and ledgers is {} in startReadOperationOnLedger", name, position, ledgers);
        }
        long ledgerId = ledgers.ceilingKey(position.getLedgerId());
        if (ledgerId != position.getLedgerId()) {
            // The ledger pointed by this position does not exist anymore. It was deleted because it was empty. We need
            // to skip on the next available ledger
            position = new DlogBasedPosition(ledgerId, 0, 0);
        }

        return position;
    }

    void notifyCursors() {
        while (true) {
            final DlogBasedManagedCursor waitingCursor = waitingCursors.poll();
            if (waitingCursor == null) {
                break;
            }

            executor.submit(safeRun(() -> waitingCursor.notifyEntriesAvailable()));
        }
    }

    private void trimConsumedLedgersInBackground() {
        executor.submitOrdered(name, safeRun(() -> {
            internalTrimConsumedLedgers();
        }));
    }

    private void scheduleDeferredTrimming() {
        scheduledExecutor.schedule(safeRun(() -> trimConsumedLedgersInBackground()), 100, TimeUnit.MILLISECONDS);
    }

    /**
     * Get the txId for a specific Position, used when trim.
     * return -1 when fail
     */
    private long getTxId(DlogBasedPosition position){
        LogReader logReader = null;
        try{
            logReader = dlm.openLogReader(position.getDlsn());
            if(logReader != null)
                return logReader.readNext(false).getTransactionId();
            return -1;
        }catch (IOException ioe){
            log.error("[{}]  fail in getTxId ", name);
        }finally {
            logReader.asyncClose();
        }
       return -1;
    }
    /**
     * Checks whether should truncate the log to slowestPosition and truncate.
     *
     * @throws Exception
     */
    private void internalTrimConsumedLedgers() {
        // Ensure only one trimming operation is active
        if (!trimmerMutex.tryLock()) {
            scheduleDeferredTrimming();
            return;
        }

        synchronized (this) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Start truncate log stream. slowest={} totalSize={}", name, slowestPosition,
                        TOTAL_SIZE_UPDATER.get(this));
            }
            if (STATE_UPDATER.get(this) == State.Closed) {
                log.debug("[{}] Ignoring trimming request since the managed ledger was already closed", name);
                trimmerMutex.unlock();
                return;
            }
            if (!cursors.isEmpty()) {
                DlogBasedPosition slowestReaderPosition = cursors.getSlowestReaderPosition();
                if (slowestReaderPosition != null) {
                    slowestPosition = slowestReaderPosition;
                } else {
                    trimmerMutex.unlock();
                    return;
                }
            }
            if(slowestPosition.getLedgerId() == currentLedger){
                trimmerMutex.unlock();
                return;
            }

            try{
                dlm.purgeLogsOlderThan(getTxId(slowestPosition));
            }catch (IOException ioe){
                log.error("[{}] dlm purge log error", name);
            }

            // Update metadata
            updateLedgers();
            entryCache.invalidateAllEntries(slowestPosition.getLedgerId());

            if (log.isDebugEnabled()) {
                log.debug("[{}] Updating of ledgers list after trimming", name);
            }
            trimmerMutex.unlock();

        }
    }

    /**
     * Delete this ManagedLedger completely from the system.
     *
     * @throws Exception
     */
    @Override
    public void delete() throws InterruptedException, ManagedLedgerException {
        final CountDownLatch counter = new CountDownLatch(1);
        final AtomicReference<ManagedLedgerException> exception = new AtomicReference<>();

        asyncDelete(new DeleteLedgerCallback() {
            @Override
            public void deleteLedgerComplete(Object ctx) {
                counter.countDown();
            }

            @Override
            public void deleteLedgerFailed(ManagedLedgerException e, Object ctx) {
                exception.set(e);
                counter.countDown();
            }

        }, null);

        if (!counter.await(AsyncOperationTimeoutSeconds, TimeUnit.SECONDS)) {
            throw new ManagedLedgerException("Timeout during managed ledger delete operation");
        }

        if (exception.get() != null) {
            log.error("[{}] Error deleting managed ledger", name, exception.get());
            throw exception.get();
        }
    }

    @Override
    public void asyncDelete(final DeleteLedgerCallback callback, final Object ctx) {
        // Delete the managed ledger without closing, since we are not interested in gracefully closing cursors and
        // ledgers
        STATE_UPDATER.set(this, State.Fenced);

        List<ManagedCursor> cursors = Lists.newArrayList(this.cursors);
        if (cursors.isEmpty()) {
            // No cursors to delete, proceed with next step
            deleteAllLedgers(callback, ctx);
            return;
        }

        AtomicReference<ManagedLedgerException> cursorDeleteException = new AtomicReference<>();
        AtomicInteger cursorsToDelete = new AtomicInteger(cursors.size());
        for (ManagedCursor cursor : cursors) {
            asyncDeleteCursor(cursor.getName(), new DeleteCursorCallback() {
                @Override
                public void deleteCursorComplete(Object ctx) {
                    if (cursorsToDelete.decrementAndGet() == 0) {
                        if (cursorDeleteException.get() != null) {
                            // Some cursor failed to delete
                            callback.deleteLedgerFailed(cursorDeleteException.get(), ctx);
                            return;
                        }

                        // All cursors deleted, continue with deleting all ledgers
                        deleteAllLedgers(callback, ctx);
                    }
                }

                @Override
                public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                    log.warn("[{}] Failed to delete cursor {}", name, cursor, exception);
                    cursorDeleteException.compareAndSet(null, exception);
                    if (cursorsToDelete.decrementAndGet() == 0) {
                        // Trigger callback only once
                        callback.deleteLedgerFailed(exception, ctx);
                    }
                }
            }, null);
        }
    }

    private void deleteAllLedgers(DeleteLedgerCallback callback, Object ctx) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Deleting dlog stream {}", name, dlm.getStreamName());
        }
        try{
            dlm.delete();

        }catch (IOException e){
            callback.deleteLedgerFailed(new ManagedLedgerException(e), ctx);
            log.error("[{}] Deleting dlog stream :{} fail", name, dlm.getStreamName(), e);
        }
        deleteMetadata(callback, ctx);

    }

    private void deleteMetadata(DeleteLedgerCallback callback, Object ctx) {
        store.removeManagedLedger(name, new MetaStoreCallback<Void>() {
            @Override
            public void operationComplete(Void result, Stat stat) {
                log.info("[{}] Successfully deleted managed ledger", name);
                factory.close(DlogBasedManagedLedger.this);
                callback.deleteLedgerComplete(ctx);
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                log.warn("[{}] Failed to delete managed ledger", name, e);
                factory.close(DlogBasedManagedLedger.this);
                callback.deleteLedgerFailed(e, ctx);
            }
        });
    }

    /**
     * Get the number of entries between a contiguous range of two positions
     *
     * @param range
     *            the position range
     * @return the count of entries
     */
    long getNumberOfEntries(Range<DlogBasedPosition> range) {
        DlogBasedPosition fromPosition = range.lowerEndpoint();
        boolean fromIncluded = range.lowerBoundType() == BoundType.CLOSED;
        DlogBasedPosition toPosition = range.upperEndpoint();
        boolean toIncluded = range.upperBoundType() == BoundType.CLOSED;

        if (fromPosition.getLedgerId() == toPosition.getLedgerId()) {
            // If the 2 positions are in the same ledger
            long count = toPosition.getEntryId() - fromPosition.getEntryId() - 1;
            count += fromIncluded ? 1 : 0;
            count += toIncluded ? 1 : 0;
            return count;
        } else {
            long count = 0;
            // If the from & to are pointing to different ledgers, then we need to :
            // 1. Add the entries in the ledger pointed by toPosition
            count += toPosition.getEntryId();
            count += toIncluded ? 1 : 0;

            // 2. Add the entries in the ledger pointed by fromPosition
            LedgerInfo li = ledgers.get(fromPosition.getLedgerId());
            if (li != null) {
                count += li.getEntries() - (fromPosition.getEntryId() + 1);
                count += fromIncluded ? 1 : 0;
            }

            // 3. Add the whole ledgers entries in between
            for (LedgerInfo ls : ledgers.subMap(fromPosition.getLedgerId(), false, toPosition.getLedgerId(), false)
                    .values()) {
                count += ls.getEntries();
            }


            return count;
        }
    }

    /**
     * Get the entry position at a given distance from a given position
     *
     * @param startPosition
     *            starting position
     * @param n
     *            number of entries to skip ahead
     * @param startRange
     *            specifies whether or not to include the start position in calculating the distance
     * @return the new position that is n entries ahead
     */
    DlogBasedPosition getPositionAfterN(final DlogBasedPosition startPosition, long n, PositionBound startRange) {
        long entriesToSkip = n;
        long currentLedgerId;
        long currentEntryId;

        if (startRange == PositionBound.startIncluded) {
            currentLedgerId = startPosition.getLedgerId();
            currentEntryId = startPosition.getEntryId();
        } else {
            // e.g. a mark-delete position
            DlogBasedPosition nextValidPosition = getNextValidPosition(startPosition);
            currentLedgerId = nextValidPosition.getLedgerId();
            currentEntryId = nextValidPosition.getEntryId();
        }

        boolean lastLedger = false;
        long totalEntriesInCurrentLedger;

        while (entriesToSkip >= 0) {
            // for the current ledger, the number of entries written is deduced from the lastConfirmedEntry
            // for previous ledgers, LedgerInfo in ZK has the number of entries
            if (currentLedgerId == currentLedger) {
                lastLedger = true;
                totalEntriesInCurrentLedger = lastConfirmedEntry.getEntryId() + 1;
            } else {
                totalEntriesInCurrentLedger = ledgers.get(currentLedgerId).getEntries();
            }

            long unreadEntriesInCurrentLedger = totalEntriesInCurrentLedger - currentEntryId;

            if (unreadEntriesInCurrentLedger >= entriesToSkip) {
                // if the current ledger has more entries than what we need to skip
                // then the return position is in the same ledger
                currentEntryId += entriesToSkip;
                break;
            } else {
                // skip remaining entry from the next ledger
                entriesToSkip -= unreadEntriesInCurrentLedger;
                if (lastLedger) {
                    // there are no more ledgers, return the last position
                    currentEntryId = totalEntriesInCurrentLedger;
                    break;
                } else {
                    currentLedgerId = ledgers.ceilingKey(currentLedgerId + 1);
                    currentEntryId = 0;
                }
            }
        }

        DlogBasedPosition positionToReturn = getPreviousPosition(DlogBasedPosition.get(currentLedgerId, currentEntryId));
        if (log.isDebugEnabled()) {
            log.debug("getPositionAfterN: Start position {}:{}, startIncluded: {}, Return position {}:{}",
                    startPosition.getLedgerId(), startPosition.getEntryId(), startRange, positionToReturn.getLedgerId(),
                    positionToReturn.getEntryId());
        }

        return positionToReturn;
    }

    /**
     * Get the entry position that come before the specified position in the message stream, using information from the
     * ledger list and each ledger entries count.
     *
     * @param position
     *            the current position
     * @return the previous position
     */
    DlogBasedPosition getPreviousPosition(DlogBasedPosition position) {
        if (position.getEntryId() > 0) {
            return DlogBasedPosition.get(position.getLedgerId(), position.getEntryId() - 1);
        }

        // The previous position will be the last position of an earlier ledgers
        NavigableMap<Long, LedgerInfo> headMap = ledgers.headMap(position.getLedgerId(), false);

        if (headMap.isEmpty()) {
            // There is no previous ledger, return an invalid position in the current ledger
            return DlogBasedPosition.get(position.getLedgerId(), -1);
        }

        // We need to find the most recent non-empty ledger
        for (long ledgerId : headMap.descendingKeySet()) {
            LedgerInfo li = headMap.get(ledgerId);
            if (li.getEntries() > 0) {
                return DlogBasedPosition.get(li.getLedgerId(), li.getEntries() - 1);
            }
        }

        // in case there are only empty ledgers, we return a position in the first one
        return DlogBasedPosition.get(headMap.firstEntry().getKey(), -1);
    }

    /**
     * Validate whether a specified position is valid for the current managed ledger.
     *
     * @param position
     *            the position to validate
     * @return true if the position is valid, false otherwise
     */
    boolean isValidPosition(DlogBasedPosition position) {
        DlogBasedPosition last = lastConfirmedEntry;
        if (log.isDebugEnabled()) {
            log.debug("IsValid position: {} -- last: {}", position, last);
        }

        if (position.getEntryId() < 0) {
            return false;
        } else if (position.getLedgerId() > last.getLedgerId()) {
            return false;
        } else if (position.getLedgerId() == last.getLedgerId()) {
            return position.getEntryId() <= (last.getEntryId() + 1);
        } else {
            // Look in the ledgers map
            LedgerInfo ls = ledgers.get(position.getLedgerId());

            if (ls == null) {
                if (position.getLedgerId() < last.getLedgerId()) {
                    // Pointing to a non existing ledger that is older than the current ledger is invalid
                    return false;
                } else {
                    // Pointing to a non existing ledger is only legitimate if the ledger was empty
                    return position.getEntryId() == 0;
                }
            }

            return position.getEntryId() < ls.getEntries();
        }

    }

    //todo use dlog's to is the ledger exists?
    boolean ledgerExists(long ledgerId) {
        return ledgers.get(ledgerId) != null;
    }

    //todo deal time interval between open writer and logSegment meta update
    long getNextValidLedger(long ledgerId) {
        // this can handle the circuation where open dlog first time and the logsegment meta hasn't update
        if(ledgers.ceilingKey(ledgerId + 1) == null)
            return 0L;
        return ledgers.ceilingKey(ledgerId + 1);
    }


    DlogBasedPosition getNextValidPosition(final DlogBasedPosition position) {
        DlogBasedPosition nextPosition = (DlogBasedPosition) position.getNext();
        while (!isValidPosition(nextPosition)) {
            Long nextLedgerId = ledgers.ceilingKey(nextPosition.getLedgerId() + 1);
            if (nextLedgerId == null) {
                return null;
            }
            nextPosition = DlogBasedPosition.get(nextLedgerId.longValue(), 0);
        }
        return nextPosition;
    }

    /**
     * get first position that can be mark delete, used by cursor.
     *
     * @return DlogBasedPosition before the first valid position
     */
    DlogBasedPosition getFirstPosition() {
        Long ledgerId = ledgers.firstKey();
        return ledgerId == null ? null : new DlogBasedPosition(ledgerId, -1, 0);
//        DLSN firstDLSN = null;
//        try{
//            firstDLSN = dlm.getFirstDLSNAsync().get();
//
//        }catch (Exception e){
//            log.error("getFirstDLSNAsync exception in getFirstPosition");
//        }
//        if(firstDLSN != null)
//            return new DlogBasedPosition(firstDLSN);
//        else
//            return null;
    }

    DlogBasedPosition getLastPosition() {
        return lastConfirmedEntry;
    }

    @Override
    public ManagedCursor getSlowestConsumer() {
        return cursors.getSlowestReader();
    }

    DlogBasedPosition getMarkDeletePositionOfSlowestConsumer() {
        ManagedCursor slowestCursor = getSlowestConsumer();
        return slowestCursor == null ? null : (DlogBasedPosition) slowestCursor.getMarkDeletedPosition();
    }

    /**
     * Get the last position written in the managed ledger, alongside with the associated counter
     */
    Pair<DlogBasedPosition, Long> getLastPositionAndCounter() {
        DlogBasedPosition pos;
        long count;

        do {
            pos = lastConfirmedEntry;
            count = ENTRIES_ADDED_COUNTER_UPDATER.get(this);

            // Ensure no entry was written while reading the two values
        } while (pos.compareTo(lastConfirmedEntry) != 0);

        return Pair.create(pos, count);
    }

    public void activateCursor(ManagedCursor cursor) {
        if (activeCursors.get(cursor.getName()) == null) {
            activeCursors.add(cursor);
        }
    }

    public void deactivateCursor(ManagedCursor cursor) {
        if (activeCursors.get(cursor.getName()) != null) {
            activeCursors.removeCursor(cursor.getName());
            if (activeCursors.isEmpty()) {
                // cleanup cache if there is no active subscription
                entryCache.clear();
            } else {
                // if removed subscription was the slowest subscription : update cursor and let it clear cache: till
                // new slowest-cursor's read-position
                discardEntriesFromCache((DlogBasedManagedCursor) activeCursors.getSlowestReader(),
                        getPreviousPosition((DlogBasedPosition) activeCursors.getSlowestReader().getReadPosition()));
            }
        }
    }

    public boolean isCursorActive(ManagedCursor cursor) {
        return cursor.isDurable() && activeCursors.get(cursor.getName()) != null;
    }


    public List<LedgerInfo> getLedgersInfoAsList() {
        return Lists.newArrayList(ledgers.values());
    }

    public NavigableMap<Long, LedgerInfo> getLedgersInfo() {
        return ledgers;
    }

    ScheduledExecutorService getScheduledExecutor() {
        return scheduledExecutor;
    }

    OrderedSafeExecutor getExecutor() {
        return executor;
    }

    /**
     * Provide ManagedLedgerInfo to update to meta store.
     *
     */
    private ManagedLedgerInfo getManagedLedgerInfo() {
        //dont need to include ledgers info when using dlog.
//        ManagedLedgerInfo.Builder mlInfo = ManagedLedgerInfo.newBuilder().addAllLedgerInfo(ledgers.values());
        ManagedLedgerInfo.Builder mlInfo = ManagedLedgerInfo.newBuilder();
        if (state == State.Terminated) {
            mlInfo.setTerminatedPosition(NestedPositionInfo.newBuilder().setLedgerId(lastConfirmedEntry.getLedgerId())
                    .setEntryId(lastConfirmedEntry.getEntryId()));
        }

        return mlInfo.build();
    }

    /**
     * Throws an exception if the managed ledger has been previously fenced
     *
     * @throws ManagedLedgerException
     */
    private void checkFenced() throws ManagedLedgerException {
        if (STATE_UPDATER.get(this) == State.Fenced) {
            log.error("[{}] Attempted to use a fenced managed ledger", name);
            throw new ManagedLedgerFencedException();
        }
    }

    private void checkManagedLedgerIsOpen() throws ManagedLedgerException {
        if (STATE_UPDATER.get(this) == State.Closed) {
            throw new ManagedLedgerException("ManagedLedger " + name + " has already been closed");
        }
    }

    synchronized void setFenced() {
        STATE_UPDATER.set(this, State.Fenced);
    }

    MetaStore getStore() {
        return store;
    }

    DlogBasedManagedLedgerConfig getConfig() {
        return config;
    }

    interface ManagedLedgerInitializeLedgerCallback {
        void initializeComplete();

        void initializeFailed(ManagedLedgerException e);
    }

    public DlogBasedManagedLedgerMBean getMBean() {
        return mbean;
    }


    // Expose internal values for debugging purposes, most of them are used by PersisticTopic to get stats.
    public long getEntriesAddedCounter() {
        return ENTRIES_ADDED_COUNTER_UPDATER.get(this);
    }


    public long getCurrentLedgerEntries() {
        return currentLedgerEntries;
    }
    /**
     * reserved just to keep consistent with pulsar original impl.
     * these two stats infos are not necessary for dlog.
     */
    public long getCurrentLedgerSize() {
        return currentLedgerSize;
    }

    /**
     * equivalent to log segment create time  approximately.
     * When the log segment sequence number change, update the lastLedgerCreatedTimestamp
     *
     */
    public long getLastLedgerCreatedTimestamp() {
        return lastLedgerCreatedTimestamp;
    }

    /**
     * reserved just to keep consistent with pulsar original impl.
     * just return -1.
     *
     */
    public long getLastLedgerCreationFailureTimestamp() {
        return lastLedgerCreationFailureTimestamp;
    }

    public int getWaitingCursorsCount() {
        return waitingCursors.size();
    }

    public int getPendingAddEntriesCount() {
        return pendingAddEntries.size();
    }

    public DlogBasedPosition getLastConfirmedEntry() {
        return lastConfirmedEntry;
    }

    public String getState() {
        return STATE_UPDATER.get(this).toString();
    }

    public long getCacheSize() {
        return entryCache.getSize();
    }

    private static final Logger log = LoggerFactory.getLogger(DlogBasedManagedLedger.class);

}
