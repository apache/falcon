package org.apache.ivory.entity;

import javax.xml.bind.Unmarshaller;

import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.process.Process;

public class AbstractTestBase {
    protected static final String PROCESS_XML = "/config/process/process-0.1.xml";
    protected static final String FEED_XML = "/config/feed/feed-0.1.xml";
    protected static final String CLUSTER_XML = "/config/cluster/cluster-0.1.xml";

    protected void storeEntity(EntityType type, String name) throws Exception {
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
        }
    }

    public void setup() throws Exception {
        storeEntity(EntityType.CLUSTER, "corp");
        storeEntity(EntityType.FEED, "clicks");
        storeEntity(EntityType.FEED, "impressions");
        storeEntity(EntityType.FEED, "clicksummary");
        storeEntity(EntityType.PROCESS, "clicksummary");
    }
}
