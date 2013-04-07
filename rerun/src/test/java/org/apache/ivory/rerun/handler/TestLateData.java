package org.apache.ivory.rerun.handler;

import com.sun.jersey.api.client.WebResource;
import org.apache.hadoop.conf.Configuration;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.cluster.Interfacetype;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.util.StartupProperties;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.StringWriter;
import java.util.Collection;

public class TestLateData {
	
	protected static final String FEED_XML = "/feed-template.xml";
    protected static String CLUSTER_XML = "/cluster-template.xml";
    protected static final String PROCESS_XML = "/process-template.xml";
    protected static final String PROCESS_XML2 = "/process-template2.xml";
    
    protected WebResource service = null;
    protected Configuration conf = new Configuration();

    @BeforeClass
    public void initConfigStore() throws Exception {
    	MockitoAnnotations.initMocks(this);
        cleanupStore();
        String listeners = StartupProperties.get().getProperty("configstore.listeners");
        StartupProperties.get().setProperty("configstore.listeners", 
                listeners.replace("org.apache.ivory.service.SharedLibraryHostingService", ""));
        ConfigurationStore.get().init();
        
    }
    
    protected void cleanupStore() throws IvoryException {
        ConfigurationStore store = ConfigurationStore.get();
        for(EntityType type:EntityType.values()) {
            Collection<String> entities = store.getEntities(type);
            for(String entity:entities)
                store.remove(type, entity);
        }
    }

    protected void storeEntity(EntityType type, String name) throws Exception {
        Unmarshaller unmarshaller = type.getUnmarshaller();
        ConfigurationStore store = ConfigurationStore.get();
        store.remove(type, name);
		switch (type) {
		case CLUSTER:
                Cluster cluster = (Cluster) unmarshaller.unmarshal(this.getClass().getResource(CLUSTER_XML));
                cluster.setName(name);
                ClusterHelper.getInterface(cluster, Interfacetype.WRITE).setEndpoint(conf.get("fs.default.name"));
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
		ConfigurationStore store = ConfigurationStore.get();
		for (EntityType type : EntityType.values()) {
			for (String name : store.getEntities(type)) {
				store.remove(type, name);
			}
		}
		storeEntity(EntityType.CLUSTER , "testCluster");
		storeEntity(EntityType.PROCESS, "sample");
        storeEntity(EntityType.FEED, "raw-logs");
        storeEntity(EntityType.FEED, "clicks");
        Unmarshaller unmarshaller = EntityType.PROCESS.getUnmarshaller();
        Process process = (Process) unmarshaller.unmarshal(this.getClass().getResource(PROCESS_XML2));
        process.setName("sample2");
        store.publish(EntityType.PROCESS, process);
    }

	public String marshallEntity(final Entity entity) throws IvoryException,
			JAXBException {
		Marshaller marshaller = entity.getEntityType().getMarshaller();
		StringWriter stringWriter = new StringWriter();
		marshaller.marshal(entity, stringWriter);
		return stringWriter.toString();
	}
	
//	@Test
//	private void TestLateWhenInstanceRunning() throws Exception
//	{
//		try{
//        WorkflowEngine engine = Mockito.mock(WorkflowEngine.class);
//        when(engine.instanceStatus("testCluster", "123")).thenReturn("RUNNING");
//        
//		ConfigurationStore store = ConfigurationStore.get();
//		setup();
//		String nominalTime = EntityUtil.formatDateUTC(new Date(System.currentTimeMillis() - 1800000));
//        InMemoryQueue<LaterunEvent> queue = new InMemoryQueue<LaterunEvent>(new File("target/late"));
//        latedataHandler.init(queue);
//        
//        AbstractRerunHandler handle = RerunHandlerFactory.getRerunHandler(RerunEvent.RerunType.LATE);
//        handle.handleRerun("sample", nominalTime, "123", "123", engine, System.currentTimeMillis());
//        
//        File directory = new File("target/late");
//        File[] files = directory.listFiles();
//        int noFilesBefore = files.length;
//        
//        Thread.sleep(90000);
//        
//        files = directory.listFiles();
//        int noFilesAfterRetry = files.length;        
//        Assert.assertNotSame(noFilesBefore, noFilesAfterRetry);
//		}
//		catch (Exception e){
//			Assert.fail("Not expecting any exception");
//		}
//        
//	}
//	
//	
//	@Test
//	private void TestLateWhenDataPresent() throws Exception {
//		WorkflowEngine engine = Mockito.mock(WorkflowEngine.class);
//		when(engine.instanceStatus("testCluster", "123")).thenReturn(
//				"SUCCEEDED");
//
//		LateRerunConsumer consumer = Mockito.mock(LateRerunConsumer.class);
//		when(consumer.detectLate(Mockito.any(LaterunEvent.class))).thenReturn(
//				"new data found");
//
//		String nominalTime = EntityUtil.formatDateUTC(new Date(System
//				.currentTimeMillis() - 1800000));
//		AbstractRerunHandler handle = RerunHandlerFactory
//				.getRerunHandler(RerunEvent.RerunType.LATE);
//
//		ConfigurationStore store = ConfigurationStore.get();
//		setup();
//
//		InMemoryQueue<LaterunEvent> queue = new InMemoryQueue<LaterunEvent>(
//				new File("target/late"));
//		latedataHandler.init(queue);
//
//		handle.handleRerun("sample", nominalTime, "123", "123", engine,
//				System.currentTimeMillis());
//
//		File directory = new File("target/late");
//		File[] files = directory.listFiles();
//		int noFilesBefore = files.length;
//
//		Thread.sleep(90000);
//
//		files = directory.listFiles();
//		int noFilesAfterRetry = files.length;
//		Assert.assertNotSame(noFilesBefore, noFilesAfterRetry);
//
//	}
	
}
