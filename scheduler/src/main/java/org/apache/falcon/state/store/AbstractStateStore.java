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
package org.apache.falcon.state.store;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.service.ConfigurationChangeListener;
import org.apache.falcon.state.EntityID;
import org.apache.falcon.state.EntityState;
import org.apache.falcon.util.ReflectionUtils;
import org.apache.falcon.util.StartupProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This abstract class listens to config store changes and keeps the state store in sync with the config store.
 */
public abstract class AbstractStateStore implements StateStore, ConfigurationChangeListener {
    private static StateStore stateStore;
    private static final Logger LOG = LoggerFactory.getLogger(AbstractStateStore.class);

    @Override
    public void onAdd(Entity entity) throws FalconException {
        if (entity.getEntityType() != EntityType.CLUSTER) {
            putEntity(new EntityState(entity));
        }
    }

    @Override
    public void onRemove(Entity entity) throws FalconException {
        // Delete entity should remove its instances too.
        if (entity.getEntityType() != EntityType.CLUSTER) {
            deleteEntity(new EntityID(entity));
        }
    }

    @Override
    public void onChange(Entity oldEntity, Entity newEntity) throws FalconException {
        if (newEntity.getEntityType() != EntityType.CLUSTER) {
            EntityState entityState = getEntity(new EntityID(oldEntity));
            if (entityState == null) {
                onAdd(newEntity);
            } else {
                entityState.setEntity(newEntity);
                updateEntity(entityState);
            }
        }
    }

    @Override
    public void onReload(Entity entity) throws FalconException {
        if (entity.getEntityType() != EntityType.CLUSTER) {
            // To ensure the config store and state store are in sync
            if (!entityExists(new EntityID(entity))) {
                LOG.info("State store missing entity {}. Adding it.", entity.getName());
                onAdd(entity);
            }
        }
    }

    /**
     * @return Singleton instance of an implementation of State Store based on the startup properties.
     */
    public static synchronized StateStore get() {
        if (stateStore == null) {
            String storeImpl = StartupProperties.get().getProperty("falcon.state.store.impl",
                    "org.apache.falcon.state.store.InMemoryStateStore");
            try {
                stateStore = ReflectionUtils.getInstanceByClassName(storeImpl);
            } catch (FalconException e) {
                throw new RuntimeException("Unable to load state store impl. : " + storeImpl, e);
            }
        }
        return stateStore;
    }
}
