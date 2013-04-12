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
package org.apache.falcon.rerun.queue;

import org.apache.falcon.FalconException;
import org.apache.falcon.rerun.event.RerunEvent;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.util.LinkedList;

public class InMemoryQueueTest {

    @Test (timeOut = 10000)
    public void testDelayedQueue() throws Exception {
        runTest();
    }

    private void runTest() throws InterruptedException, FalconException {
        InMemoryQueue<MyEvent> queue = new InMemoryQueue<MyEvent>(new File("target"));

        LinkedList<MyEvent> events = new LinkedList<MyEvent>();

        for (int index = 0; index < 5; index++) {
            Thread.sleep(30);
            long time = System.currentTimeMillis();
            int delay = ((5 - index) / 2) * 50;
            MyEvent event = new MyEvent("someCluster", Integer.toString(index),
                    time, delay, "someType", "someName", "someInstance", 0);
            queue.offer(event);
            boolean inserted = false;
            for (int posn = 0; posn < events.size(); posn++) {
                MyEvent thisEvent = events.get(posn);
                if (thisEvent.getDelayInMilliSec() + thisEvent.getMsgInsertTime() >
                        event.getDelayInMilliSec() + event.getMsgInsertTime()) {
                    events.add(posn, event);
                    inserted = true;
                    break;
                }
            }
            if (!inserted) {
                events.add(event);
            }
        }

        for (MyEvent event : events) {
            MyEvent queueEvent = queue.take();
            Assert.assertEquals(queueEvent.getWfId(), event.getWfId());
        }
    }

    private class MyEvent extends RerunEvent {

        public MyEvent(String clusterName, String wfId,
                       long msgInsertTime, long delay, String entityType,
                       String entityName, String instance, int runId) {
            super(clusterName, wfId, msgInsertTime, delay,
                    entityType, entityName, instance, runId);
        }

        @Override
        public RerunType getType() {
            RerunType type = super.getType();
            return type == null ? RerunType.RETRY : type;
        }
    }
}
