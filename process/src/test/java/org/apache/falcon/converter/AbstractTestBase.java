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

package org.apache.falcon.converter;

import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Process;

import javax.xml.bind.Unmarshaller;

/**
 * Base for falcon unit tests involving configuration store.
 */
public class AbstractTestBase {
    private static final String PROCESS_XML = "/config/process/process-0.1.xml";
    private static final String FEED_XML = "/config/feed/feed-0.1.xml";
    private static final String CLUSTER_XML = "/config/cluster/cluster-0.1.xml";
    private static final String PIG_PROCESS_XML = "/config/process/pig-process-0.1.xml";

    protected void storeEntity(EntityType type, String name, String resource) throws Exception {
        Unmarshaller unmarshaller = type.getUnmarshaller();
        ConfigurationStore store = ConfigurationStore.get();
        store.remove(type, name);
        switch (type) {
        case CLUSTER:
            Cluster cluster = (Cluster) unmarshaller.unmarshal(this.getClass().getResource(resource));
            cluster.setName(name);
            store.publish(type, cluster);
            break;

        case FEED:
            Feed feed = (Feed) unmarshaller.unmarshal(this.getClass().getResource(resource));
            feed.setName(name);
            store.publish(type, feed);
            break;

        case PROCESS:
            Process process = (Process) unmarshaller.unmarshal(this.getClass().getResource(resource));
            process.setName(name);
            store.publish(type, process);
            break;

        default:
        }
    }

    public void setup() throws Exception {
        storeEntity(EntityType.CLUSTER, "corp", CLUSTER_XML);
        storeEntity(EntityType.FEED, "clicks", FEED_XML);
        storeEntity(EntityType.FEED, "impressions", FEED_XML);
        storeEntity(EntityType.FEED, "clicksummary", FEED_XML);
        storeEntity(EntityType.PROCESS, "clicksummary", PROCESS_XML);
        storeEntity(EntityType.PROCESS, "pig-process", PIG_PROCESS_XML);
    }

    public void cleanup() throws Exception {
        ConfigurationStore store = ConfigurationStore.get();
        store.remove(EntityType.PROCESS, "pig-process");
        store.remove(EntityType.PROCESS, "clicksummary");
        store.remove(EntityType.FEED, "clicksummary");
        store.remove(EntityType.FEED, "impressions");
        store.remove(EntityType.FEED, "clicks");
        store.remove(EntityType.CLUSTER, "corp");
    }
}
