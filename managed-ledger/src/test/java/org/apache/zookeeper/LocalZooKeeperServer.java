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
package org.apache.zookeeper;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.mledger.util.Pair;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.bookkeeper.util.LocalBookKeeper.waitForServerUp;

@SuppressWarnings({ "deprecation", "restriction", "rawtypes" })
public class LocalZooKeeperServer {
    NIOServerCnxnFactory serverFactory;
    ZooKeeperServer zks;
    int ZooKeeperDefaultPort = 2181;
    File ZkTmpDir;

    public LocalZooKeeperServer(int port){
        ZooKeeperDefaultPort = port;
    }

    public void start(int maxCC) throws IOException {
        log.info("Starting ZK server");
        this.ZkTmpDir = IOUtils.createTempDir("zookeeper", "localzookeeper");

        try {
            this.zks = new ZooKeeperServer(this.ZkTmpDir, this.ZkTmpDir, this.ZooKeeperDefaultPort);
            this.serverFactory = new NIOServerCnxnFactory();
            this.serverFactory.configure(new InetSocketAddress(this.ZooKeeperDefaultPort), maxCC);
            this.serverFactory.startup(this.zks);
        } catch (Exception var3) {
            log.error("Exception while instantiating ZooKeeper", var3);
        }

        boolean b = waitForServerUp("127.0.0.1:"+ZooKeeperDefaultPort, 30000L);
        log.debug("ZooKeeper server up: {}", Boolean.valueOf(b));
    }


    public  void shutdown() throws InterruptedException {
     try {
         zks.shutdown();
     } catch (Exception e){
         log.debug("ZooKeeper server shutdown error: {}", e);
        }
    }





    @Override
    public String toString() {
        return "LocalZookeeperServer";
    }


    private static final Logger log = LoggerFactory.getLogger(LocalZooKeeperServer.class);
}
