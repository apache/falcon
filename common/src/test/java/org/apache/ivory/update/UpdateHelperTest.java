/*
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

package org.apache.ivory.update;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.ivory.entity.AbstractTestBase;
import org.apache.ivory.entity.FeedHelper;
import org.apache.ivory.entity.parser.EntityParserFactory;
import org.apache.ivory.entity.parser.FeedEntityParser;
import org.apache.ivory.entity.parser.ProcessEntityParser;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.Frequency;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.feed.LocationType;
import org.apache.ivory.entity.v0.feed.Partition;
import org.apache.ivory.entity.v0.feed.Properties;
import org.apache.ivory.entity.v0.feed.Property;
import org.apache.ivory.entity.v0.process.Process;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class UpdateHelperTest extends AbstractTestBase {
    private final FeedEntityParser parser = (FeedEntityParser)
            EntityParserFactory.getParser(EntityType.FEED);
    private final ProcessEntityParser processParser = (ProcessEntityParser)
            EntityParserFactory.getParser(EntityType.PROCESS);

    @BeforeClass
    public void init() throws Exception {
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

        newFeed.getLateArrival().setCutOff(Frequency.fromString("hours(1)"));
        Assert.assertTrue(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));

        newFeed.getLateArrival().setCutOff(oldFeed.getLateArrival().getCutOff());
        Assert.assertFalse(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));

        FeedHelper.getLocation(newFeed, LocationType.DATA).setPath("/test");
        Assert.assertTrue(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));

        FeedHelper.getLocation(newFeed, LocationType.DATA).setPath(
                FeedHelper.getLocation(oldFeed, LocationType.DATA).getPath());
        Assert.assertFalse(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));

        newFeed.setFrequency(Frequency.fromString("months(1)"));
        Assert.assertTrue(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));

        newFeed.setFrequency(oldFeed.getFrequency());
        Assert.assertFalse(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));

        Partition partition = new Partition();
        partition.setName("1");
        newFeed.getPartitions().getPartitions().add(partition);
        Assert.assertFalse(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));

        Property property = new Property();
        property.setName("1");
        property.setValue("1");
        newFeed.setProperties(new Properties());
        newFeed.getProperties().getProperties().add(property);
        Assert.assertTrue(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));

        newFeed.getProperties().getProperties().remove(0);
        Assert.assertFalse(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));

        FeedHelper.getCluster(newFeed, process.getCluster().getName()).getValidity().setStart("123");
        Assert.assertTrue(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));

        FeedHelper.getCluster(newFeed, process.getCluster().getName()).getValidity().
                setStart(FeedHelper.getCluster(oldFeed, process.getCluster().getName()).getValidity().getStart());
        Assert.assertFalse(UpdateHelper.shouldUpdate(oldFeed, newFeed, process));
    }
}
