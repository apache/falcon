package org.apache.ivory.entity;

import java.io.StringWriter;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.cluster.Interface;
import org.apache.ivory.entity.v0.cluster.Interfacetype;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.process.Process;

public class AbstractTestBase {
    protected static final String PROCESS_XML = "/config/process/process-0.1.xml";
    protected static final String FEED_XML = "/config/feed/feed-0.1.xml";
    protected static final String CLUSTER_XML = "/config/cluster/cluster-0.1.xml";
    protected MiniDFSCluster dfsCluster;
    protected Configuration conf= new Configuration();

    protected void storeEntity(EntityType type, String name) throws Exception {
        Unmarshaller unmarshaller = type.getUnmarshaller();
        ConfigurationStore store = ConfigurationStore.get();
        store.remove(type, name);
		switch (type) {
		case CLUSTER:
                Cluster cluster = (Cluster) unmarshaller.unmarshal(this.getClass().getResource(CLUSTER_XML));
                cluster.setName(name);
                cluster.getInterfaces().put(Interfacetype.WRITE,
                        newInterface(Interfacetype.WRITE, conf.get("fs.default.name"), "0.1"));
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
                FileSystem fs =dfsCluster.getFileSystem();
                fs.mkdirs(new Path(process.getWorkflow().getPath()));
                if (!fs.exists(new Path(process.getWorkflow()+"/lib"))) {
                	fs.mkdirs(new Path(process.getWorkflow()+"/lib"));
                }
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

	public String marshallEntity(final Entity entity) throws IvoryException,
			JAXBException {
		Marshaller marshaller = entity.getEntityType().getMarshaller();
		StringWriter stringWriter = new StringWriter();
		marshaller.marshal(entity, stringWriter);
		return stringWriter.toString();
	}
	
	private Interface newInterface(Interfacetype type, String endPoint,
			String version) {
		Interface iface = new Interface();
		iface.setType(type);
		iface.setEndpoint(endPoint);
		iface.setVersion(version);
		return iface;
	}
}
