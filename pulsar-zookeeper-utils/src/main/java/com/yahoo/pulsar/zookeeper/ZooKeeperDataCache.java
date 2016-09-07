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
package com.yahoo.pulsar.zookeeper;

import java.util.List;
import java.util.Map.Entry;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.yahoo.pulsar.zookeeper.ZooKeeperCache.CacheUpdater;
import com.yahoo.pulsar.zookeeper.ZooKeeperCache.Deserializer;

/**
 * Provides a generic cache for reading objects stored in zookeeper.
 *
 * Maintains the objects already serialized in memory and sets watches to receive changes notifications.
 *
 * @param <T>
 */
public abstract class ZooKeeperDataCache<T> implements Deserializer<T>, CacheUpdater<T>, Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperDataCache.class);

    private final ZooKeeperCache cache;
    private final List<ZooKeeperCacheListener<T>> listeners = Lists.newCopyOnWriteArrayList();

    public ZooKeeperDataCache(final ZooKeeperCache cache) {
        this.cache = cache;
    }

    public T get(final String path) throws Exception {
        return getWithStat(path).getKey();
    }

    public Entry<T, Stat> getWithStat(final String path) throws Exception {
        return cache.getData(path, this, this);
    }

    /**
     * Only for UTs (for now), as this clears the whole ZK data cache.
     */
    public void clear() {
        cache.invalidateAllData();
    }

    public void invalidate(final String path) {
        cache.invalidateData(path);
    }

    @Override
    public void reloadCache(final String path) {
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Reloading ZooKeeperDataCache at path {}", path);
            }
            cache.invalidate(path);
            Entry<T, Stat> cacheEntry = cache.getData(path, this, this);
            for (ZooKeeperCacheListener<T> listener : listeners) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Notifying listener {} at path {}", listener, path);
                }
                listener.onUpdate(path, cacheEntry.getKey(), cacheEntry.getValue());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Notified listener {} at path {}", listener, path);
                }
            }
        } catch (KeeperException.NoNodeException nne) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Node [{}] does not exist", nne.getPath());
            }
        } catch (Exception e) {
            LOG.warn("Reloading ZooKeeperDataCache failed at path: {}", path, e);
        }
    }

    @Override
    public void registerListener(ZooKeeperCacheListener<T> listener) {
        listeners.add(listener);
    }

    @Override
    public void unregisterListener(ZooKeeperCacheListener<T> listener) {
        listeners.remove(listener);
    }

    @Override
    public void process(WatchedEvent event) {
        LOG.info("[{}] Received ZooKeeper watch event: {}", cache.zkSession.get(), event);
        cache.process(event, this);
    }
}
