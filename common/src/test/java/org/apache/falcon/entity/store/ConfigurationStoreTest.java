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

package org.apache.falcon.entity.store;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.service.ConfigurationChangeListener;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Tests for validating configuration store.
 */
public class ConfigurationStoreTest {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationStoreTest.class);
    private static final String PROCESS1NAME = "process1";
    private static final String PROCESS2NAME = "process2";
    private static final String PROCESS3NAME = "process3";

    private ConfigurationStore store = ConfigurationStore.get();
    private TestListener listener = new TestListener();

    private class TestListener implements ConfigurationChangeListener {
        @Override
        public void onAdd(Entity entity) throws FalconException {
            throw new FalconException("For test");
        }

        @Override
        public void onRemove(Entity entity) throws FalconException {
            throw new FalconException("For test");
        }

        @Override
        public void onChange(Entity oldEntity, Entity newEntity) throws FalconException {
            throw new FalconException("For test");
        }

        @Override
        public void onReload(Entity entity) throws FalconException {
            throw new FalconException("For test");
        }
    }


    @BeforeClass
    public void setUp() throws Exception {
        System.out.println("in beforeMethod");
        Process process1 = new Process();
        process1.setName(PROCESS1NAME);
        store.publish(EntityType.PROCESS, process1);

        Process process2 = new Process();
        process2.setName(PROCESS2NAME);
        store.publish(EntityType.PROCESS, process2);

        Process process3 = new Process();
        process3.setName(PROCESS3NAME);
        store.publish(EntityType.PROCESS, process3);
    }

    @Test
    public void testPublish() throws Exception {
        Process process = new Process();
        process.setName("hello");
        store.publish(EntityType.PROCESS, process);
        Process p = store.get(EntityType.PROCESS, "hello");
        Assert.assertEquals(p, process);

        store.registerListener(listener);
        process.setName("world");
        try {
            store.publish(EntityType.PROCESS, process);
            throw new AssertionError("Expected exception");
        } catch(FalconException expected) {
            //expected
        }
        store.unregisterListener(listener);
    }

    @Test
    public void testGet() throws Exception {
        Process p = store.get(EntityType.PROCESS, "notfound");
        Assert.assertNull(p);
    }

    @Test
    public void testRemove() throws Exception {
        Process process = new Process();
        process.setName("remove");
        store.publish(EntityType.PROCESS, process);

        Process p = store.get(EntityType.PROCESS, "remove");
        Assert.assertEquals(p, process);
        store.remove(EntityType.PROCESS, "remove");
        p = store.get(EntityType.PROCESS, "remove");
        Assert.assertNull(p);

        store.publish(EntityType.PROCESS, process);
        store.registerListener(listener);
        try {
            store.remove(EntityType.PROCESS, "remove");
            throw new AssertionError("Expected exception");
        } catch(FalconException expected) {
            //expected
        }
        store.unregisterListener(listener);
    }


    @Test(threadPoolSize = 3, invocationCount = 6)
    public void testConcurrentRemoveOfSameProcess() throws Exception {
        store.remove(EntityType.PROCESS, PROCESS1NAME);
        Process p = store.get(EntityType.PROCESS, PROCESS1NAME);
        Assert.assertNull(p);
    }

    @Test(threadPoolSize = 3, invocationCount = 6)
    public void testConcurrentRemove() throws Exception {
        store.remove(EntityType.PROCESS, PROCESS2NAME);
        Process p1 = store.get(EntityType.PROCESS, PROCESS2NAME);
        Assert.assertNull(p1);

        store.remove(EntityType.PROCESS, PROCESS3NAME);
        Process p2 = store.get(EntityType.PROCESS, PROCESS3NAME);
        Assert.assertNull(p2);
    }

    @BeforeSuite
    @AfterSuite
    public void cleanup() throws IOException {
        Path path = new Path(StartupProperties.get().
                getProperty("config.store.uri"));
        FileSystem fs = FileSystem.get(path.toUri(), new Configuration());
        fs.delete(path, true);
        LOG.info("Cleaned up {}", path);
    }
}
