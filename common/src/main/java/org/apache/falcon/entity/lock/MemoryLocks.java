/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.falcon.entity.lock;

import org.apache.falcon.entity.v0.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * In memory resource locking that provides lock capabilities.
 */
public final class MemoryLocks {
    private static final Logger LOG = LoggerFactory.getLogger(MemoryLocks.class);
    private static ConcurrentHashMap<String, Boolean> locks = new ConcurrentHashMap<String, Boolean>();

    private static MemoryLocks instance = new MemoryLocks();

    private MemoryLocks() {
    }

    public static MemoryLocks getInstance() {
        return instance;
    }

    /**
     * Obtain a lock for an entity.
     *
     * @param entity entity object.
     * @return the lock token for the resource, or <code>null</code> if the lock could not be obtained.
     */
    public boolean acquireLock(Entity entity) {
        boolean lockObtained = false;
        String entityName = getLockKey(entity);

        Boolean putResponse = locks.putIfAbsent(entityName, true);
        if (putResponse == null || !putResponse) {
            LOG.info("Lock obtained for schedule/update of {} by {}",
                    entity.toShortString(), Thread.currentThread().getName());
            lockObtained = true;
        }
        return lockObtained;
    }

    /**
     * Release the lock for an entity.
     *
     * @param entity entity object.
     */
    public void releaseLock(Entity entity) {
        String entityName = getLockKey(entity);

        locks.remove(entityName);
        LOG.info("Successfully released lock for {} by {}",
                entity.toShortString(), Thread.currentThread().getName());
    }

    private String getLockKey(Entity entity) {
        return entity.getEntityType().toString() + "." + entity.getName();
    }


}
