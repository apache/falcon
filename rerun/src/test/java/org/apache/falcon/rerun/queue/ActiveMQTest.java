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

import org.apache.activemq.broker.BrokerService;
import org.apache.falcon.rerun.event.LaterunEvent;
import org.apache.falcon.rerun.event.RerunEvent;
import org.apache.falcon.util.FalconTestUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test class for ActiveMQ delayed queue implementation.
 */
public class ActiveMQTest {

    private static final String BROKER_URL = "vm://localhost?broker.useJmx=false&broker.persistent=true";
    private BrokerService broker;
    private static final String DESTINATION = "activemq.topic";

    @BeforeClass
    private void setup() throws Exception {
        broker = new BrokerService();
        broker.setDataDirectory("target/activemq");
        broker.addConnector(BROKER_URL);
        broker.setBrokerName("localhost");
        broker.setSchedulerSupport(true);
        broker.start();
    }

    @Test
    public void testBrokerStartAndEnqueue() {
        ActiveMQueue<RerunEvent> activeMQueue = new ActiveMQueue<RerunEvent>(
                BROKER_URL, DESTINATION);
        activeMQueue.init();

        RerunEvent event = new LaterunEvent("clusterName", "wfId",
                System.currentTimeMillis(), 60 * 1000, "entityType",
                "entityName", "instance", 0, FalconTestUtil.TEST_USER_1);

        try {
            activeMQueue.offer(event);
            broker.stop();
            broker.start();
            activeMQueue.reconnect();
            activeMQueue.offer(event);
        } catch (Exception e) {
            Assert.fail();
        }
    }
}
