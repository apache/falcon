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

package org.apache.ivory.entity.parser;

/**
 * Test Cases for ProcessEntityParser
 */
import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.io.StringWriter;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.AbstractTestBase;
import org.apache.ivory.entity.FeedHelper;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.Frequency;
import org.apache.ivory.entity.v0.SchemaHelper;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.feed.ActionType;
import org.apache.ivory.entity.v0.feed.ClusterType;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.feed.Location;
import org.apache.ivory.entity.v0.feed.LocationType;
import org.apache.ivory.entity.v0.feed.Locations;
import org.apache.ivory.entity.v0.feed.Validity;
import org.apache.ivory.group.FeedGroupMapTest;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class FeedEntityParserTest extends AbstractTestBase {

	private final FeedEntityParser parser = (FeedEntityParser) EntityParserFactory
			.getParser(EntityType.FEED);

	private Feed modifiableFeed;

	@BeforeMethod
	public void setUp() throws Exception {
	    cleanupStore();
	    ConfigurationStore store = ConfigurationStore.get();
	    
		Unmarshaller unmarshaller = EntityType.CLUSTER.getUnmarshaller();
		Cluster cluster = (Cluster) unmarshaller.unmarshal(this.getClass()
				.getResourceAsStream(CLUSTER_XML));
		cluster.setName("testCluster");
		store.publish(EntityType.CLUSTER, cluster);

		cluster = (Cluster) unmarshaller.unmarshal(this.getClass()
				.getResourceAsStream(CLUSTER_XML));
		cluster.setName("backupCluster");
		store.publish(EntityType.CLUSTER, cluster);

		modifiableFeed = (Feed) parser.parseAndValidate(this.getClass()
				.getResourceAsStream(FEED_XML));
	}

	@Test(expectedExceptions = ValidationException.class)
	public void testValidations() throws Exception {
		ConfigurationStore.get().remove(EntityType.CLUSTER, "backupCluster");
		parser.parseAndValidate(this.getClass().getResourceAsStream(FEED_XML));
	}

	@Test
	public void testParse() throws IOException, IvoryException, JAXBException {

		Feed feed = parser.parseAndValidate(this.getClass()
				.getResourceAsStream(FEED_XML));

		Assert.assertNotNull(feed);
		assertEquals(feed.getName(), "clicks");
		assertEquals(feed.getDescription(), "clicks log");
		assertEquals(feed.getFrequency().toString(), "hours(1)");
		assertEquals(feed.getGroups(), "online,bi");

		assertEquals(feed.getClusters().getClusters().get(0).getName(),
				"testCluster");
		assertEquals(feed.getClusters().getClusters().get(0).getType(),
				ClusterType.SOURCE);
		assertEquals(SchemaHelper.formatDateUTC(feed.getClusters().getClusters().get(0).getValidity()
				.getStart()), "2011-11-01T00:00Z");
		assertEquals(SchemaHelper.formatDateUTC(feed.getClusters().getClusters().get(0).getValidity()
				.getEnd()), "2011-12-31T00:00Z");
		assertEquals(feed.getTimezone().getID(), "UTC");
		assertEquals(feed.getClusters().getClusters().get(0).getRetention()
				.getAction(), ActionType.DELETE);
		assertEquals(feed.getClusters().getClusters().get(0).getRetention()
				.getLimit().toString(), "hours(48)");

		assertEquals(feed.getClusters().getClusters().get(1).getName(),
				"backupCluster");
		assertEquals(feed.getClusters().getClusters().get(1).getType(),
				ClusterType.TARGET);
		assertEquals(SchemaHelper.formatDateUTC(feed.getClusters().getClusters().get(1).getValidity()
				.getStart()), "2011-11-01T00:00Z");
		assertEquals(SchemaHelper.formatDateUTC(feed.getClusters().getClusters().get(1).getValidity()
				.getEnd()), "2011-12-31T00:00Z");
		assertEquals(feed.getClusters().getClusters().get(1).getRetention()
				.getAction(), ActionType.ARCHIVE);
		assertEquals(feed.getClusters().getClusters().get(1).getRetention()
				.getLimit().toString(), "hours(6)");

		assertEquals(FeedHelper.getLocation(feed, LocationType.DATA).getPath(),
				"/projects/ivory/clicks");
		assertEquals(FeedHelper.getLocation(feed, LocationType.META).getPath(),
				"/projects/ivory/clicksMetaData");
		assertEquals(FeedHelper.getLocation(feed, LocationType.STATS).getPath(),
				"/projects/ivory/clicksStats");

		assertEquals(feed.getACL().getGroup(), "group");
		assertEquals(feed.getACL().getOwner(), "testuser");
		assertEquals(feed.getACL().getPermission(), "0x755");

		assertEquals(feed.getSchema().getLocation(), "/schema/clicks");
		assertEquals(feed.getSchema().getProvider(), "protobuf");

		StringWriter stringWriter = new StringWriter();
		Marshaller marshaller = EntityType.FEED.getMarshaller();
		marshaller.marshal(feed, stringWriter);
		System.out.println(stringWriter.toString());
	}

	@Test(expectedExceptions = ValidationException.class)
	public void applyValidationInvalidFeed() throws Exception {
		Feed feed = (Feed) parser
				.parseAndValidate(ProcessEntityParserTest.class
						.getResourceAsStream(FEED_XML));
		feed.getClusters().getClusters().get(0).setName("invalid cluster");
		parser.validate(feed);
	}

	
	@Test
	public void testPartitionExpression() throws IvoryException {
        Feed feed = (Feed) parser.parseAndValidate(ProcessEntityParserTest.class
                .getResourceAsStream(FEED_XML));
        
        //When there are more than 1 src clusters, there should be partition expression
        org.apache.ivory.entity.v0.feed.Cluster newCluster = new org.apache.ivory.entity.v0.feed.Cluster();
        newCluster.setName("newCluster");
        newCluster.setType(ClusterType.SOURCE);
        newCluster.setPartition("${cluster.colo}");
        feed.getClusters().getClusters().add(newCluster);
        try {
            parser.validate(feed);
            Assert.fail("Expected ValidationException");
        } catch(ValidationException e) { }
        
        //When there are more than 1 src clusters, the partition expression should contain cluster variable
        feed.getClusters().getClusters().get(0).setPartition("*");
        try {
            parser.validate(feed);
            Assert.fail("Expected ValidationException");
        } catch(ValidationException e) { }
        
        //When there are more than 1 target cluster, there should be partition expre
        newCluster.setType(ClusterType.TARGET);
        try {
            parser.validate(feed);
            Assert.fail("Expected ValidationException");
        } catch(ValidationException e) { }
        
        //When there are more than 1 target clusters, the partition expression should contain cluster variable
        feed.getClusters().getClusters().get(1).setPartition("*");
        try {
            parser.validate(feed);
            Assert.fail("Expected ValidationException");
        } catch(ValidationException e) { }
        
        //Number of parts in partition expression < number of partitions defined for feed
        feed.getClusters().getClusters().get(1).setPartition("*/*");
        try {
            parser.validate(feed);
            Assert.fail("Expected ValidationException");
        } catch(ValidationException e) { }

        feed.getClusters().getClusters().get(0).setPartition(null);
        feed.getClusters().getClusters().get(1).setPartition(null);
        feed.getClusters().getClusters().remove(2);
        feed.setPartitions(null);
        parser.validate(feed);
	}

	@Test
	public void testInvalidClusterValidityTime() {
		Validity validity = modifiableFeed.getClusters().getClusters().get(0)
				.getValidity();
		try {
			validity.setStart(SchemaHelper.parseDateUTC("2007-02-29T00:00Z"));
			modifiableFeed.getClusters().getClusters().get(0)
					.setValidity(validity);
			parser.parseAndValidate(marshallEntity(modifiableFeed));
			Assert.fail("Cluster validity failed");
		} catch (Exception e) {
			System.out.println(e.getMessage());
			validity.setStart(SchemaHelper.parseDateUTC("2011-11-01T00:00Z"));
			modifiableFeed.getClusters().getClusters().get(0)
					.setValidity(validity);
		}

		try {
			validity.setEnd(SchemaHelper.parseDateUTC("2010-04-31T00:00Z"));
			modifiableFeed.getClusters().getClusters().get(0)
					.setValidity(validity);
			parser.parseAndValidate(marshallEntity(modifiableFeed));
			Assert.fail("Cluster validity failed");
		} catch (Exception e) {
			System.out.println(e.getMessage());
			validity.setEnd(SchemaHelper.parseDateUTC("2011-12-31T00:00Z"));
			modifiableFeed.getClusters().getClusters().get(0)
					.setValidity(validity);
		}
	}

	@Test(expectedExceptions = ValidationException.class)
	public void testInvalidProcessValidity() throws Exception {
		Feed feed = parser.parseAndValidate((FeedEntityParserTest.class
				.getResourceAsStream(FEED_XML)));
		feed.getClusters().getClusters().get(0).getValidity()
				.setStart(SchemaHelper.parseDateUTC("2012-11-01T00:00Z"));
		parser.validate(feed);
	}

	@Test
	public void testValidFeedGroup() throws IvoryException, JAXBException {
		Feed feed1 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
				(FeedEntityParserTest.class.getResourceAsStream(FEED_XML)));
		feed1.setName("f1" + System.currentTimeMillis());
		feed1.setGroups("group1,group2,group3");
		feed1.setLocations(new Locations());
		Location location = new Location();
		location.setPath("/projects/bi/rmc/daily/ad/${YEAR}/fraud/${MONTH}-${DAY}/ad");
		location.setType(LocationType.DATA);
		feed1.getLocations().getLocations().add(location);
		parser.parseAndValidate(feed1.toString());
		ConfigurationStore.get().publish(EntityType.FEED, feed1);

		Feed feed2 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
				(FeedEntityParserTest.class.getResourceAsStream(FEED_XML)));
		feed2.setName("f2" + System.currentTimeMillis());
		feed2.setGroups("group1,group2,group5");
		feed2.setLocations(new Locations());
		Location location2 = new Location();
		location2
				.setPath("/projects/bi/rmc/daily/ad/${YEAR}/fraud/${MONTH}-${DAY}/ad");
		location2.setType(LocationType.DATA);
		feed2.getLocations().getLocations().add(location2);
		parser.parseAndValidate(feed2.toString());
	}

	@Test(expectedExceptions = ValidationException.class)
	public void testInvalidFeedGroup() throws IvoryException, JAXBException {
		Feed feed1 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
				(FeedEntityParserTest.class.getResourceAsStream(FEED_XML)));
		feed1.setName("f1" + System.currentTimeMillis());
		feed1.setGroups("group1,group2,group3");
		feed1.setLocations(new Locations());
		Location location = new Location();
		location.setPath("/projects/bi/rmc/daily/ad/${YEAR}/fraud/${MONTH}-${DAY}/ad");
		location.setType(LocationType.DATA);
		feed1.getLocations().getLocations().add(location);
		parser.parseAndValidate(feed1.toString());
		ConfigurationStore.get().publish(EntityType.FEED, feed1);

		Feed feed2 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
				(FeedEntityParserTest.class.getResourceAsStream(FEED_XML)));
		feed2.setName("f2" + System.currentTimeMillis());
		feed2.setGroups("group1,group2,group5");
		feed2.setLocations(new Locations());
		Location location2 = new Location();
		location2
				.setPath("/projects/bi/rmc/daily/ad/${YEAR}/fraud/${MONTH}/${HOUR}/ad");
		location2.setType(LocationType.DATA);
		feed2.getLocations().getLocations().add(location2);
		parser.parseAndValidate(feed2.toString());
	}

	@Test
	public void testValidGroupNames() throws IvoryException, JAXBException {
		Feed feed1 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
				FeedGroupMapTest.class
						.getResourceAsStream("/config/feed/feed-0.1.xml"));
		feed1.setName("f1" + System.currentTimeMillis());
		feed1.setGroups("group7,group8");
		parser.parseAndValidate(feed1.toString());

		feed1.setGroups("group7");
		parser.parseAndValidate(feed1.toString());

		feed1.setGroups(null);
		parser.parseAndValidate(feed1.toString());
		ConfigurationStore.get().publish(EntityType.FEED, feed1);
	}

	@Test
	public void testInvalidGroupNames() throws IvoryException, JAXBException {
		Feed feed1 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
				FeedGroupMapTest.class
						.getResourceAsStream("/config/feed/feed-0.1.xml"));
		feed1.setName("f1" + System.currentTimeMillis());

		try {
			feed1.setGroups("commaend,");
			parser.parseAndValidate(feed1.toString());
			Assert.fail("Expected exception");
		} catch (IvoryException e) {

		}
		try {
			feed1.setGroups("group8,   group9");
			parser.parseAndValidate(feed1.toString());
			Assert.fail("Expected exception");
		} catch (IvoryException e) {

		}
		try {
			feed1.setGroups("space in group,group9");
			parser.parseAndValidate(feed1.toString());
			Assert.fail("Expected exception");
		} catch (IvoryException e) {

		}
	}
	
	@Test
	public void testClusterPartitionExp() throws IvoryException {
		Cluster cluster = ConfigurationStore.get().get(EntityType.CLUSTER,
				"testCluster");
		Assert.assertEquals(FeedHelper.evaluateClusterExp(cluster,
				"/*/${cluster.colo}"), "/*/" + cluster.getColo());
		Assert.assertEquals(FeedHelper.evaluateClusterExp(cluster,
				"/*/${cluster.name}/Local"), "/*/" + cluster.getName()+"/Local");
		Assert.assertEquals(FeedHelper.evaluateClusterExp(cluster,
				"/*/${cluster.field1}/Local"), "/*/value1/Local");
	}
	
	@Test(expectedExceptions=IvoryException.class)
	public void testInvalidFeedName() throws JAXBException, IvoryException  {
		Feed feed1 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
				FeedGroupMapTest.class
						.getResourceAsStream("/config/feed/feed-0.1.xml"));
		feed1.setName("Feed_name");
		parser.parseAndValidate(feed1.toString());
	}
	
	@Test(expectedExceptions=IvoryException.class)
	public void testInvalidFeedGroupName() throws JAXBException, IvoryException  {
		Feed feed1 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
				FeedGroupMapTest.class
						.getResourceAsStream("/config/feed/feed-0.1.xml"));
		feed1.setName("feed1");
		feed1.getLocations().getLocations().get(0).setPath("/data/clicks/${YEAR}/${MONTH}/${DAY}/${HOUR}");
		ConfigurationStore.get().publish(EntityType.FEED, feed1);
		
		Feed feed2 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
				FeedGroupMapTest.class
						.getResourceAsStream("/config/feed/feed-0.1.xml"));
		feed2.setName("feed2");
		feed2.getLocations().getLocations().get(0).setPath("/data/clicks/${YEAR}/${MONTH}/${DAY}/${HOUR}");
		feed2.setFrequency(new Frequency("hours(1)"));
		try{
			parser.parseAndValidate(feed2.toString());
		}catch(IvoryException e){
			e.printStackTrace();
			Assert.fail("Not expecting exception for same frequency");
		}
		feed2.setFrequency(new Frequency("hours(2)"));
		//expecting exception
		parser.parseAndValidate(feed2.toString());
	}
	
	@Test
	public void testNullFeedLateArrival() throws JAXBException, IvoryException  {
		Feed feed = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
				FeedGroupMapTest.class
						.getResourceAsStream("/config/feed/feed-0.1.xml"));
		
		feed.setLateArrival(null);
		parser.parseAndValidate(feed.toString());
		
	}

}
