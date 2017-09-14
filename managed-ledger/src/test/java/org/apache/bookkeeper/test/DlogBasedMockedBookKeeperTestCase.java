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
package org.apache.bookkeeper.test;

import dlshade.org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.client.MockBookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.dlog.DlogBasedManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.distributedlog.LocalDLMEmulator;
import org.apache.zookeeper.LocalZooKeeperServer;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A class runs several bookie servers for testing.
 */
public abstract class DlogBasedMockedBookKeeperTestCase {

    static final Logger LOG = LoggerFactory.getLogger(DlogBasedMockedBookKeeperTestCase.class);

    //zk related variables
    protected ZooKeeper zkc;
    protected LocalZooKeeperServer zks;


    // BookKeeper related variables
    protected MockBookKeeper bkc;
    protected int numBookies;

    protected DlogBasedManagedLedgerFactory factory;

    protected ClientConfiguration baseClientConf = new ClientConfiguration();

    protected OrderedSafeExecutor executor;
    protected ExecutorService cachedExecutor;

    public DlogBasedMockedBookKeeperTestCase() {
        // By default start a 3 bookies cluster
        this(3);
    }

    public DlogBasedMockedBookKeeperTestCase(int numBookies) {
        this.numBookies = numBookies;
    }

    @BeforeMethod
    public void setUp(Method method) throws Exception {
        LOG.info(">>>>>> starting {}", method);
        try {
            // start bookkeeper service
            startBookKeeper();
        } catch (Exception e) {
            LOG.error("Error setting up", e);
            throw e;
        }

        executor = new OrderedSafeExecutor(2, "test");
        cachedExecutor = Executors.newCachedThreadPool();
        ManagedLedgerFactoryConfig conf = new ManagedLedgerFactoryConfig();
        factory = new DlogBasedManagedLedgerFactory(bkc, zkc, conf);
    }

    @AfterMethod
    public void tearDown(Method method) throws Exception {
        LOG.info("@@@@@@@@@ stopping " + method);
        factory.shutdown();
        factory = null;
        stopBookKeeper();
        stopZooKeeper();
        executor.shutdown();
        cachedExecutor.shutdown();
        LOG.info("--------- stopped {}", method);
    }

    /**
     * Start cluster,include mock bk server,local zk server, mock zk client.
     *
     * @throws Exception
     */
    protected void startBookKeeper() throws Exception {


        zks = new LocalZooKeeperServer(2181);
        zks.start(1000);
        zkc = MockZooKeeper.newInstance();

        for (int i = 0; i < numBookies; i++) {
            ZkUtils.createFullPathOptimistic(zkc, "/ledgers/available/192.168.1.1:" + (5000 + i), "".getBytes(), null,
                    null);
        }

        zkc.create("/ledgers/LAYOUT", "1\nflat:1".getBytes(), null, null);

        bkc = new MockBookKeeper(baseClientConf, zkc);
    }

    protected void stopBookKeeper() throws Exception {
        bkc.shutdown();
    }

    protected void stopZooKeeper() throws Exception {
        zks.shutdown();
        zkc.close();

    }


}
