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
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.Process;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test for Memory Locking mechanism used for schedule/update of entities.
 */

public class MemoryLocksTest {
    private static final String FEED_XML = "/config/feed/feed-0.1.xml";
    private static final String PROCESS_XML = "/config/process/process-0.1.xml";

    @Test
    public void testSuccessfulMemoryLockAcquisition() throws Exception {
        MemoryLocks memoryLocks = MemoryLocks.getInstance();
        Entity feed = (Entity) EntityType.FEED.getUnmarshaller().unmarshal(this.getClass().getResource(FEED_XML));
        Assert.assertEquals(memoryLocks.acquireLock(feed, "test"), true);
        memoryLocks.releaseLock(feed);
    }

    @Test
    public void testUnsuccessfulMemoryLockAcquisition() throws Exception {
        MemoryLocks memoryLocks = MemoryLocks.getInstance();
        Entity feed = (Entity) EntityType.FEED.getUnmarshaller().unmarshal(this.getClass().getResource(FEED_XML));
        Assert.assertEquals(memoryLocks.acquireLock(feed, "test"), true);
        Assert.assertEquals(memoryLocks.acquireLock(feed, "test"), false);
        memoryLocks.releaseLock(feed);
    }

    @Test
    public void testDuplicateEntityNameLockAcquisition() throws Exception {
        MemoryLocks memoryLocks = MemoryLocks.getInstance();
        //In case both feed & process have identical names, they shouldn't clash during updates
        Entity feed = (Entity) EntityType.FEED.getUnmarshaller().unmarshal(this.getClass().getResource(FEED_XML));
        org.apache.falcon.entity.v0.process.Process process = (Process) EntityType.PROCESS.getUnmarshaller().
                unmarshal(this.getClass().getResource(PROCESS_XML));
        process.setName(feed.getName());
        Assert.assertEquals(memoryLocks.acquireLock(feed, "test"), true);
        Assert.assertEquals(memoryLocks.acquireLock(process, "test"), true);
        memoryLocks.releaseLock(feed);
    }
}
