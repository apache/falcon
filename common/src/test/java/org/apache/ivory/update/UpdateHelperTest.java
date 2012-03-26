package org.apache.ivory.update;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.ivory.entity.AbstractTestBase;
import org.apache.ivory.entity.parser.EntityParserFactory;
import org.apache.ivory.entity.parser.FeedEntityParser;
import org.apache.ivory.entity.parser.ProcessEntityParser;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.feed.LocationType;
import org.apache.ivory.entity.v0.feed.Partition;
import org.apache.ivory.entity.v0.feed.Property;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class UpdateHelperTest extends AbstractTestBase {
    private static final Logger LOG = Logger.getLogger(UpdateHelperTest.class);

    private final FeedEntityParser parser = (FeedEntityParser)
            EntityParserFactory.getParser(EntityType.FEED);
    private final ProcessEntityParser processParser = (ProcessEntityParser)
            EntityParserFactory.getParser(EntityType.PROCESS);

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

    @Test
    public void testShouldUpdate() throws Exception {
        storeEntity(EntityType.CLUSTER, "testCluster");
        storeEntity(EntityType.CLUSTER, "backupCluster");
        storeEntity(EntityType.FEED, "clicksFeed");
        storeEntity(EntityType.FEED, "impressionFeed");
        storeEntity(EntityType.FEED, "imp-click-join1");
        storeEntity(EntityType.FEED, "imp-click-join2");
        Feed oldFeed = parser.parseAndValidate(this.getClass()
                .getResourceAsStream(FEED_XML));

        Feed newFeed = (Feed)oldFeed.clone();
        Process process = processParser.parseAndValidate(this.getClass().
                getResourceAsStream(PROCESS_XML));

        Assert.assertFalse(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));

        newFeed.getLateArrival().setCutOff("hours(1)");
        Assert.assertTrue(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));

        newFeed.getLateArrival().setCutOff(oldFeed.getLateArrival().getCutOff());
        Assert.assertFalse(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));

        newFeed.getLocations().get(LocationType.DATA).setPath("/test");
        Assert.assertTrue(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));

        newFeed.getLocations().get(LocationType.DATA).setPath(
                oldFeed.getLocations().get(LocationType.DATA).getPath());
        Assert.assertFalse(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));

        newFeed.setFrequency("year");
        Assert.assertTrue(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));

        newFeed.setFrequency(oldFeed.getFrequency());
        Assert.assertFalse(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));

        newFeed.setPeriodicity(500);
        Assert.assertTrue(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));

        newFeed.setPeriodicity(oldFeed.getPeriodicity());
        Assert.assertFalse(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));

        Partition partition = new Partition("1");
        newFeed.getPartitions().getPartition().add(partition);
        Assert.assertFalse(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));

        Property property = new Property();
        property.setName("1");
        property.setValue("1");
        newFeed.getProperties().put("1", property);
        Assert.assertTrue(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));

        newFeed.getProperties().remove("1");
        Assert.assertFalse(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));

        newFeed.getCluster(process.getCluster().getName()).getValidity().setStart("123");
        Assert.assertTrue(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));

        newFeed.getCluster(process.getCluster().getName()).getValidity().
                setStart(oldFeed.getCluster(process.getCluster().getName()).getValidity().getStart());
        Assert.assertFalse(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));
    }
}
