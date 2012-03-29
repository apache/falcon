/*
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

package org.apache.ivory.entity.store;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import javax.xml.bind.Unmarshaller;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.ivory.entity.AbstractTestBase;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.transaction.TransactionManager;
import org.testng.annotations.Test;

@Test
public class ConfigurationStoreActionTest extends AbstractTestBase {

    public void testPublishRollback() throws Exception {
        Entity entity = getEntity();

        TransactionManager.startTransaction();
        ConfigurationStore.get().publish(entity.getEntityType(), entity);
        TransactionManager.rollback();

        assertNull(ConfigurationStore.get().get(entity.getEntityType(), entity.getName()));
    }

    public void testPublishCommit() throws Exception {
        Entity entity = getEntity();

        TransactionManager.startTransaction();
        ConfigurationStore.get().publish(entity.getEntityType(), entity);
        TransactionManager.commit();

        assertNotNull(ConfigurationStore.get().get(entity.getEntityType(), entity.getName()));
    }

    private Entity getEntity(String name) throws Exception {
        Unmarshaller unmarshaller = EntityType.CLUSTER.getUnmarshaller();
        Cluster cluster = (Cluster) unmarshaller.unmarshal(this.getClass().getResource(CLUSTER_XML));
        cluster.setName(name);
        return cluster;
    }

    private Entity getEntity() throws Exception {
        return getEntity(RandomStringUtils.randomAlphabetic(10));
    }
    
    public void testRemoveRollback() throws Exception {
        Entity entity = getEntity();
        ConfigurationStore store = ConfigurationStore.get();
        store.publish(entity.getEntityType(), entity);

        TransactionManager.startTransaction();
        store.remove(entity.getEntityType(), entity.getName());
        TransactionManager.rollback();

        assertNotNull(store.get(entity.getEntityType(), entity.getName()));
    }

    public void testRemoveCommit() throws Exception {
        Entity entity = getEntity();
        ConfigurationStore store = ConfigurationStore.get();
        store.publish(entity.getEntityType(), entity);

        TransactionManager.startTransaction();
        store.remove(entity.getEntityType(), entity.getName());
        TransactionManager.commit();

        assertNull(store.get(entity.getEntityType(), entity.getName()));
    }
    
    public void testUpdateRollback() throws Exception {
        Entity entity = getEntity();
        ConfigurationStore store = ConfigurationStore.get();
        store.publish(entity.getEntityType(), entity);
        Entity newEntity = getEntity(entity.getName());
        
        TransactionManager.startTransaction();
        store.initiateUpdate(newEntity);
        store.update(entity.getEntityType(), newEntity);
        TransactionManager.rollback();
        
        assertEquals(entity, store.get(entity.getEntityType(), newEntity.getName()));
    }
    
    public void testUpdateCommit() throws Exception {
        Entity entity = getEntity();
        ConfigurationStore store = ConfigurationStore.get();
        store.publish(entity.getEntityType(), entity);
        Entity newEntity = getEntity(entity.getName());
        
        TransactionManager.startTransaction();
        store.initiateUpdate(newEntity);
        store.update(entity.getEntityType(), newEntity);
        TransactionManager.commit();
        
        assertEquals(newEntity, store.get(entity.getEntityType(), newEntity.getName()));
    }
}