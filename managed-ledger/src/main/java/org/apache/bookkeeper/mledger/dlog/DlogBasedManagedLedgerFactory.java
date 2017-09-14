package org.apache.bookkeeper.mledger.dlog;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ManagedLedgerInfoCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryMXBean;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo.CursorInfo;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo.MessageRangeInfo;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo.PositionInfo;
import org.apache.bookkeeper.mledger.dlog.DlogBasedManagedLedger.ManagedLedgerInitializeLedgerCallback;
import org.apache.bookkeeper.mledger.dlog.DlogBasedManagedLedger.State;
import org.apache.bookkeeper.mledger.impl.MetaStore;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.impl.MetaStore.Stat;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.MessageRange;
import org.apache.bookkeeper.mledger.util.Futures;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicates;
import com.google.common.collect.Maps;

import io.netty.util.concurrent.DefaultThreadFactory;

public class DlogBasedManagedLedgerFactory implements ManagedLedgerFactory {

    protected final ConcurrentHashMap<String, Namespace> dlNamespaces = new ConcurrentHashMap<>();
    private final DistributedLogConfiguration dlconfig;
    private String ZKS = "";
    private final String defaultNS = "default_namespace";
    private final MetaStore store;
    private final BookKeeper bookKeeper;
    private final boolean isBookkeeperManaged;
    private final ZooKeeper zookeeper;
    private final ManagedLedgerFactoryConfig mlconfig;
    protected final ScheduledExecutorService executor = Executors.newScheduledThreadPool(16,
            new DefaultThreadFactory("bookkeeper-ml"));
    private final OrderedSafeExecutor orderedExecutor = new OrderedSafeExecutor(16, "bookkeeper-ml-workers");

    protected final DlogBasedManagedLedgerFactoryMBean mbean;
    protected final ConcurrentHashMap<String, CompletableFuture<DlogBasedManagedLedger>> ledgers = new ConcurrentHashMap<>();
    private final DlogBasedEntryCacheManager entryCacheManager;

    private long lastStatTimestamp = System.nanoTime();
    private final ScheduledFuture<?> statsTask;
    private static final int StatsPeriodSeconds = 60;

    //todo transfer zk server uri using config, dlog namespace need zk server uri.
    public DlogBasedManagedLedgerFactory(ClientConfiguration bkClientConfiguration) throws Exception {
        this(bkClientConfiguration, new ManagedLedgerFactoryConfig());
    }

    public DlogBasedManagedLedgerFactory(ClientConfiguration bkClientConfiguration, ManagedLedgerFactoryConfig mlconfig)
            throws Exception {
        final CountDownLatch counter = new CountDownLatch(1);
        final String zookeeperQuorum = checkNotNull(bkClientConfiguration.getZkServers());

        zookeeper = new ZooKeeper(zookeeperQuorum, bkClientConfiguration.getZkTimeout(), event -> {
            if (event.getState().equals(Watcher.Event.KeeperState.SyncConnected)) {
                log.info("Connected to zookeeper");
                counter.countDown();
            } else {
                log.error("Error connecting to zookeeper {}", event);
            }
        });

        if (!counter.await(bkClientConfiguration.getZkTimeout(), TimeUnit.MILLISECONDS)
                || zookeeper.getState() != States.CONNECTED) {
            throw new ManagedLedgerException("Error connecting to ZooKeeper at '" + zookeeperQuorum + "'");
        }

        this.bookKeeper = new BookKeeper(bkClientConfiguration, zookeeper);
        this.isBookkeeperManaged = true;

        this.store = new DlogBasedMetaStoreImplZookeeper(zookeeper, orderedExecutor);

        this.mlconfig = mlconfig;
        this.mbean = new DlogBasedManagedLedgerFactoryMBean(this);
        this.entryCacheManager = new DlogBasedEntryCacheManager(this);
        this.statsTask = executor.scheduleAtFixedRate(() -> refreshStats(), 0, StatsPeriodSeconds, TimeUnit.SECONDS);
        this.dlconfig = new DistributedLogConfiguration();
        this.ZKS = "127.0.0.1:2181";
    }

    public DlogBasedManagedLedgerFactory(BookKeeper bookKeeper, ZooKeeper zooKeeper) throws Exception {
        this(bookKeeper, zooKeeper, new ManagedLedgerFactoryConfig());
    }

    public DlogBasedManagedLedgerFactory(BookKeeper bookKeeper, ZooKeeper zooKeeper, ManagedLedgerFactoryConfig mlconfig)
            throws Exception {
        this.bookKeeper = bookKeeper;
        this.isBookkeeperManaged = false;
        this.zookeeper = zooKeeper;
        this.store = new DlogBasedMetaStoreImplZookeeper(zooKeeper, orderedExecutor);
        this.mlconfig = mlconfig;
        this.mbean = new DlogBasedManagedLedgerFactoryMBean(this);
        this.entryCacheManager = new DlogBasedEntryCacheManager(this);
        this.statsTask = executor.scheduleAtFixedRate(() -> refreshStats(), 0, StatsPeriodSeconds, TimeUnit.SECONDS);
        this.dlconfig = new DistributedLogConfiguration();
        this.ZKS = "127.0.0.1:2181";

    }

    private synchronized void refreshStats() {
        long now = System.nanoTime();
        long period = now - lastStatTimestamp;

        mbean.refreshStats(period, TimeUnit.NANOSECONDS);
        ledgers.values().forEach(mlfuture -> {
            DlogBasedManagedLedger ml = mlfuture.getNow(null);
            if (ml != null) {
                ml.mbean.refreshStats(period, TimeUnit.NANOSECONDS);
            }
        });

        lastStatTimestamp = now;
    }

    /**
     * Helper for getting stats
     *
     * @return
     */
    public Map<String, DlogBasedManagedLedger> getManagedLedgers() {
        // Return a view of already created ledger by filtering futures not yet completed
        return Maps.filterValues(Maps.transformValues(ledgers, future -> future.getNow(null)), Predicates.notNull());
    }

    @Override
    public ManagedLedger open(String name) throws InterruptedException, ManagedLedgerException {
        return open(name, new ManagedLedgerConfig());
    }

    @Override
    public ManagedLedger open(String name, ManagedLedgerConfig config)
            throws InterruptedException, ManagedLedgerException {
        class Result {
            ManagedLedger l = null;
            ManagedLedgerException e = null;
        }
        final Result r = new Result();
        final CountDownLatch latch = new CountDownLatch(1);
        asyncOpen(name, config, new OpenLedgerCallback() {
            @Override
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                r.l = ledger;
                latch.countDown();
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                r.e = exception;
                latch.countDown();
            }
        }, null);

        latch.await();

        if (r.e != null) {
            throw r.e;
        }
        return r.l;
    }

    @Override
    public void asyncOpen(String name, OpenLedgerCallback callback, Object ctx) {
        asyncOpen(name, new ManagedLedgerConfig(), callback, ctx);
    }

    @Override
    public void asyncOpen(final String name, final ManagedLedgerConfig config, final OpenLedgerCallback callback,
                          final Object ctx) {

        // If the ledger state is bad, remove it from the map.
        CompletableFuture<DlogBasedManagedLedger> existingFuture = ledgers.get(name);
        if (existingFuture != null && existingFuture.isDone()) {
            try {
                DlogBasedManagedLedger l = existingFuture.get();
                if (l.getState().equals(State.Fenced.toString()) || l.getState().equals(State.Closed.toString())) {
                    // Managed ledger is in unusable state. Recreate it.
                    log.warn("[{}] Attempted to open ledger in {} state. Removing from the map to recreate it", name,
                            l.getState());
                    ledgers.remove(name, existingFuture);
                }
            } catch (Exception e) {
                // Unable to get the future
                log.warn("[{}] Got exception while trying to retrieve ledger", name, e);
            }
        }
        //get corresponding dlog namespace to pulsar namespace
         String parts[] = name.split("/");
        String namespace = "";
        for(int i = 0; i < parts.length - 1; i++)
            namespace += parts[i];
        if(namespace.equals(""))
            namespace = defaultNS;
        //todo check namespace str
//        String dlUri = "Distributedlog://" + zookeeper.toString() + "/" + "persistent://test-property/cl1/ns1";
        final String uri = "distributedlog://" + ZKS + "/" + namespace;
//        log.info("ML name is {}, Pulsar topic uri is {} ", name, uri);

        //todo how to update dlog namespace's config, such as rolling time
        dlNamespaces.computeIfAbsent(uri,(dlogNamespace) ->{
            // Create the namespace ledger
            dlconfig.setLogSegmentRollingIntervalMinutes((int) (config.getMaximumRolloverTimeMs() / 60000));
            Namespace namespace1 = null;
            try{
                namespace1 = NamespaceBuilder.newBuilder()
                        .conf(dlconfig)
                        .uri(new URI(uri))
                        .build();

            }catch (Exception e){
                // Clean the map if initialization fails
                dlNamespaces.remove(uri, namespace1);
                log.error("[{}] Got exception while trying to initialize namespace", uri, e);

            }


            return namespace1;
        });
        // Ensure only one managed ledger is created and initialized
        ledgers.computeIfAbsent(name, (mlName) -> {
            // Create the managed ledger
            CompletableFuture<DlogBasedManagedLedger> future = new CompletableFuture<>();
            final DlogBasedManagedLedger newledger = new DlogBasedManagedLedger(this, bookKeeper,dlNamespaces.get(uri), store, config, executor,
                    orderedExecutor, name);
            try{
                newledger.initialize(new ManagedLedgerInitializeLedgerCallback() {
                    @Override
                    public void initializeComplete() {
                        future.complete(newledger);
                    }

                    @Override
                    public void initializeFailed(ManagedLedgerException e) {
                        // Clean the map if initialization fails
                        ledgers.remove(name, future);
                        future.completeExceptionally(e);
                    }
                }, null);
            }catch (IOException ioe){
                log.error("[{}] Got exception while trying to initialize manged-ledger", name, ioe);
            }
            return future;
        }).thenAccept(ml -> {
            callback.openLedgerComplete(ml, ctx);
        }).exceptionally(exception -> {
            callback.openLedgerFailed((ManagedLedgerException) exception.getCause(), ctx);
            return null;
        });
    }

    void close(ManagedLedger ledger) {
        // Remove the ledger from the internal factory cache
        ledgers.remove(ledger.getName());
        entryCacheManager.removeEntryCache(ledger.getName());
    }

    @Override
    public void shutdown() throws InterruptedException, ManagedLedgerException {
        statsTask.cancel(true);

        int numLedgers = ledgers.size();
        final CountDownLatch latch = new CountDownLatch(numLedgers);
        log.info("Closing {} ledgers", numLedgers);

        for (CompletableFuture<DlogBasedManagedLedger> ledgerFuture : ledgers.values()) {
            DlogBasedManagedLedger ledger = ledgerFuture.getNow(null);
            if (ledger == null) {
                continue;
            }

            ledger.asyncClose(new AsyncCallbacks.CloseCallback() {
                @Override
                public void closeComplete(Object ctx) {
                    latch.countDown();
                }

                @Override
                public void closeFailed(ManagedLedgerException exception, Object ctx) {
                    log.warn("[{}] Got exception when closing managed ledger: {}", ledger.getName(), exception);
                    latch.countDown();
                }
            }, null);
        }

        latch.await();
        log.info("{} ledgers closed", numLedgers);

        if (zookeeper != null) {
            zookeeper.close();
        }

        if (isBookkeeperManaged) {
            try {
                bookKeeper.close();
            } catch (BKException e) {
                throw new ManagedLedgerException(e);
            }
        }

        executor.shutdown();
        orderedExecutor.shutdown();

        entryCacheManager.clear();
    }

    @Override
    public ManagedLedgerInfo getManagedLedgerInfo(String name) throws InterruptedException, ManagedLedgerException {
        class Result {
            ManagedLedgerInfo info = null;
            ManagedLedgerException e = null;
        }
        final Result r = new Result();
        final CountDownLatch latch = new CountDownLatch(1);
        asyncGetManagedLedgerInfo(name, new ManagedLedgerInfoCallback() {
            @Override
            public void getInfoComplete(ManagedLedgerInfo info, Object ctx) {
                r.info = info;
                latch.countDown();
            }

            @Override
            public void getInfoFailed(ManagedLedgerException exception, Object ctx) {
                r.e = exception;
                latch.countDown();
            }
        }, null);

        latch.await();

        if (r.e != null) {
            throw r.e;
        }
        return r.info;
    }

    @Override
    public void asyncGetManagedLedgerInfo(String name, ManagedLedgerInfoCallback callback, Object ctx) {
        store.getManagedLedgerInfo(name, new MetaStoreCallback<MLDataFormats.ManagedLedgerInfo>() {
            @Override
            public void operationComplete(MLDataFormats.ManagedLedgerInfo pbInfo, Stat stat) {
                ManagedLedgerInfo info = new ManagedLedgerInfo();
                info.version = stat.getVersion();
                info.creationDate = DATE_FORMAT.format(Instant.ofEpochMilli(stat.getCreationTimestamp()));
                info.modificationDate = DATE_FORMAT.format(Instant.ofEpochMilli(stat.getModificationTimestamp()));

                info.ledgers = new ArrayList<>(pbInfo.getLedgerInfoCount());
                if (pbInfo.hasTerminatedPosition()) {
                    info.terminatedPosition = new PositionInfo();
                    info.terminatedPosition.ledgerId = pbInfo.getTerminatedPosition().getLedgerId();
                    info.terminatedPosition.entryId = pbInfo.getTerminatedPosition().getEntryId();
                }

                for (int i = 0; i < pbInfo.getLedgerInfoCount(); i++) {
                    MLDataFormats.ManagedLedgerInfo.LedgerInfo pbLedgerInfo = pbInfo.getLedgerInfo(i);
                    LedgerInfo ledgerInfo = new LedgerInfo();
                    ledgerInfo.ledgerId = pbLedgerInfo.getLedgerId();
                    ledgerInfo.entries = pbLedgerInfo.hasEntries() ? pbLedgerInfo.getEntries() : null;
                    ledgerInfo.size = pbLedgerInfo.hasSize() ? pbLedgerInfo.getSize() : null;
                    info.ledgers.add(ledgerInfo);
                }

                store.getCursors(name, new MetaStoreCallback<List<String>>() {
                    @Override
                    public void operationComplete(List<String> cursorsList, Stat stat) {
                        // Get the info for each cursor
                        info.cursors = new ConcurrentSkipListMap<>();
                        List<CompletableFuture<Void>> cursorsFutures = new ArrayList<>();

                        for (String cursorName : cursorsList) {
                            CompletableFuture<Void> cursorFuture = new CompletableFuture<>();
                            cursorsFutures.add(cursorFuture);
                            store.asyncGetCursorInfo(name, cursorName,
                                    new MetaStoreCallback<MLDataFormats.ManagedCursorInfo>() {
                                        @Override
                                        public void operationComplete(ManagedCursorInfo pbCursorInfo, Stat stat) {
                                            CursorInfo cursorInfo = new CursorInfo();
                                            cursorInfo.version = stat.getVersion();
                                            cursorInfo.creationDate = DATE_FORMAT
                                                    .format(Instant.ofEpochMilli(stat.getCreationTimestamp()));
                                            cursorInfo.modificationDate = DATE_FORMAT
                                                    .format(Instant.ofEpochMilli(stat.getModificationTimestamp()));

                                            cursorInfo.cursorsLedgerId = pbCursorInfo.getCursorsLedgerId();

                                            if (pbCursorInfo.hasMarkDeleteLedgerId()) {
                                                cursorInfo.markDelete = new PositionInfo();
                                                cursorInfo.markDelete.ledgerId = pbCursorInfo.getMarkDeleteLedgerId();
                                                cursorInfo.markDelete.entryId = pbCursorInfo.getMarkDeleteEntryId();
                                            }

                                            if (pbCursorInfo.getIndividualDeletedMessagesCount() > 0) {
                                                cursorInfo.individualDeletedMessages = new ArrayList<>();
                                                for (int i = 0; i < pbCursorInfo
                                                        .getIndividualDeletedMessagesCount(); i++) {
                                                    MessageRange range = pbCursorInfo.getIndividualDeletedMessages(i);
                                                    MessageRangeInfo rangeInfo = new MessageRangeInfo();
                                                    rangeInfo.from.ledgerId = range.getLowerEndpoint().getLedgerId();
                                                    rangeInfo.from.entryId = range.getLowerEndpoint().getEntryId();
                                                    rangeInfo.to.ledgerId = range.getUpperEndpoint().getLedgerId();
                                                    rangeInfo.to.entryId = range.getUpperEndpoint().getEntryId();
                                                    cursorInfo.individualDeletedMessages.add(rangeInfo);
                                                }
                                            }

                                            info.cursors.put(cursorName, cursorInfo);
                                            cursorFuture.complete(null);
                                        }

                                        @Override
                                        public void operationFailed(MetaStoreException e) {
                                            cursorFuture.completeExceptionally(e);
                                        }
                                    });
                        }

                        Futures.waitForAll(cursorsFutures).thenRun(() -> {
                            // Completed all the cursors info
                            callback.getInfoComplete(info, ctx);
                        }).exceptionally((ex) -> {
                            callback.getInfoFailed(new ManagedLedgerException(ex), ctx);
                            return null;
                        });
                    }

                    @Override
                    public void operationFailed(MetaStoreException e) {
                        callback.getInfoFailed(e, ctx);
                    }
                });
            }

            @Override
            public void operationFailed(MetaStoreException e) {
                callback.getInfoFailed(e, ctx);
            }
        });
    }

    public MetaStore getMetaStore() {
        return store;
    }

    public ManagedLedgerFactoryConfig getConfig() {
        return mlconfig;
    }

    public DlogBasedEntryCacheManager getEntryCacheManager() {
        return entryCacheManager;
    }

    public ManagedLedgerFactoryMXBean getCacheStats() {
        return this.mbean;
    }

    public BookKeeper getBookKeeper() {
        return bookKeeper;
    }

    private static final Logger log = LoggerFactory.getLogger(DlogBasedManagedLedgerFactory.class);

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSZ").withZone(ZoneId.systemDefault());
}
