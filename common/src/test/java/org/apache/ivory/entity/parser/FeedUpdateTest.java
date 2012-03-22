package org.apache.ivory.entity.parser;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.AbstractTestBase;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.feed.Feed;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class FeedUpdateTest extends AbstractTestBase {

    private final FeedEntityParser parser = (FeedEntityParser)
            EntityParserFactory.getParser(EntityType.FEED);

    @BeforeClass
    public void init() throws Exception {
        ProcessEntityParser.init();
        conf.set("hadoop.log.dir", "/tmp");
        this.dfsCluster = new MiniDFSCluster(conf, 1, true, null);
        setup();
    }

	@AfterClass
	public void tearDown() {
		this.dfsCluster.shutdown();
	}

    public void setup() throws Exception {
        storeEntity(EntityType.CLUSTER, "testCluster");
        storeEntity(EntityType.CLUSTER, "backupCluster");
        storeEntity(EntityType.CLUSTER, "corp");
        storeEntity(EntityType.FEED, "impressions");
    }

    @Test
    public void testFeedUpdateWithNoDependentProcess() {
        try {
            parser.parseAndValidate(this.getClass()
                    .getResourceAsStream(FEED_XML));
        } catch (IvoryException e) {
            Assert.fail("Didn't expect feed parsing to fail", e);
        }

    }

    @Test
    public void testFeedUpdateWithOneDependentProcess() {
        try {
            ConfigurationStore.get().remove(EntityType.FEED, "clicks");
            Feed feed = parser.parseAndValidate(this.getClass()
                    .getResourceAsStream(FEED_XML));
            ConfigurationStore.get().publish(EntityType.FEED, feed);
            storeEntity(EntityType.PROCESS, "sample");

            //Try parsing the same feed xml
            parser.parseAndValidate(this.getClass()
                    .getResourceAsStream(FEED_XML));
        } catch (Exception e) {
            Assert.fail("Didn't expect feed parsing to fail", e);
        }
    }

    @Test
    public void testFeedUpdateWithMultipleDependentProcess() {
        try {
            ConfigurationStore.get().remove(EntityType.FEED, "clicks");
            Feed feed = parser.parseAndValidate(this.getClass()
                    .getResourceAsStream(FEED_XML));
            ConfigurationStore.get().publish(EntityType.FEED, feed);
            storeEntity(EntityType.PROCESS, "sample");
            storeEntity(EntityType.PROCESS, "sample2");
            storeEntity(EntityType.PROCESS, "sample3");

            //Try parsing the same feed xml
            parser.parseAndValidate(this.getClass()
                    .getResourceAsStream(FEED_XML));
        } catch (Exception e) {
            Assert.fail("Didn't expect feed parsing to fail", e);
        }
    }
}
