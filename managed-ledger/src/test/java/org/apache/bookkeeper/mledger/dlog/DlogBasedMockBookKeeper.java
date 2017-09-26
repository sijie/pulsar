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

import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import dlshade.org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import dlshade.org.apache.bookkeeper.client.BKException;
import dlshade.org.apache.bookkeeper.client.BookKeeper;
import dlshade.org.apache.bookkeeper.client.LedgerHandle;
import dlshade.org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test BookKeeperClient which allows access to members we don't wish to expose in the public API.
 */
public class DlogBasedMockBookKeeper extends BookKeeper {

    final ExecutorService executor = Executors.newFixedThreadPool(1, new DefaultThreadFactory("mock-bookkeeper"));


    public ClientConfiguration getConf() {
        return super.getConf();
    }

    AtomicLong sequence = new AtomicLong(3);
    AtomicBoolean stopped = new AtomicBoolean(false);
    AtomicInteger stepsToFail = new AtomicInteger(-1);
    int failReturnCode = BKException.Code.OK;
    int nextFailReturnCode = BKException.Code.OK;

    public DlogBasedMockBookKeeper(ClientConfiguration conf, ZooKeeper zk) throws Exception {
        super(conf, zk, new OioEventLoopGroup());
    }






    void checkProgrammedFail() throws BKException {
        int steps = stepsToFail.getAndDecrement();
        log.debug("Steps to fail: {}", steps);
        if (steps <= 0) {
            if (failReturnCode != BKException.Code.OK) {
                int rc = failReturnCode;
                failReturnCode = nextFailReturnCode;
                nextFailReturnCode = BKException.Code.OK;
                throw BKException.create(rc);
            }
        }
    }

    boolean getProgrammedFailStatus() {
        int steps = stepsToFail.getAndDecrement();
        log.debug("Steps to fail: {}", steps);
        return steps == 0;
    }

    public void failNow(int rc) {
        failNow(rc, BKException.Code.OK);
    }

    public void failNow(int rc, int nextErrorCode) {
        failAfter(0, rc);
    }

    public void failAfter(int steps, int rc) {
        failAfter(steps, rc, BKException.Code.OK);
    }

    public void failAfter(int steps, int rc, int nextErrorCode) {
        stepsToFail.set(steps);
        failReturnCode = rc;
        this.nextFailReturnCode = nextErrorCode;
    }

    public void timeoutAfter(int steps) {
        stepsToFail.set(steps);
        failReturnCode = BkTimeoutOperation;
    }

    private static final int BkTimeoutOperation = 1000;

    private static final Logger log = LoggerFactory.getLogger(DlogBasedMockBookKeeper.class);
}
