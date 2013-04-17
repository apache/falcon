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


    private void storeEntity(EntityType type, String name) throws Exception {
        Unmarshaller unmarshaller = type.getUnmarshaller();
        ConfigurationStore store = ConfigurationStore.get();
        store.remove(type, name);
        switch (type) {
        case CLUSTER:
            Cluster cluster = (Cluster) unmarshaller.unmarshal(this.getClass().getResource(CLUSTER_XML));
            cluster.setName(name);
            store.publish(type, cluster);
            break;

        case FEED:
            Feed feed = (Feed) unmarshaller.unmarshal(this.getClass().getResource(FEED_XML));
            feed.setName(name);
            store.publish(type, feed);
            break;

        case PROCESS:
            Process process = (Process) unmarshaller.unmarshal(this.getClass().getResource(PROCESS_XML));
            process.setName(name);
            store.publish(type, process);
            break;
        default:
        }
    }

    public void setup() throws Exception {
        storeEntity(EntityType.CLUSTER, "corp");
        storeEntity(EntityType.FEED, "clicks");
        storeEntity(EntityType.FEED, "impressions");
        storeEntity(EntityType.FEED, "clicksummary");
        storeEntity(EntityType.PROCESS, "clicksummary");
    }

    public void cleanup() throws Exception {
        ConfigurationStore store = ConfigurationStore.get();
        store.remove(EntityType.PROCESS, "clicksummary");
        store.remove(EntityType.FEED, "clicksummary");
        store.remove(EntityType.FEED, "impressions");
        store.remove(EntityType.FEED, "clicks");
        store.remove(EntityType.CLUSTER, "corp");
    }
}
