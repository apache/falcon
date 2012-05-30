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
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.feed.ActionType;
import org.apache.ivory.entity.v0.feed.ClusterType;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.feed.LateArrival;
import org.apache.ivory.entity.v0.feed.Location;
import org.apache.ivory.entity.v0.feed.LocationType;
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
		assertEquals(feed.getFrequency(), "hours");
		assertEquals(feed.getPeriodicity(), 1);
		assertEquals(feed.getGroups(), "online,bi");

		assertEquals(feed.getClusters().getCluster().get(0).getName(),
				"testCluster");
		assertEquals(feed.getClusters().getCluster().get(0).getType(),
				ClusterType.SOURCE);
		assertEquals(feed.getClusters().getCluster().get(0).getValidity()
				.getStart(), "2011-11-01T00:00Z");
		assertEquals(feed.getClusters().getCluster().get(0).getValidity()
				.getEnd(), "2011-12-31T00:00Z");
		assertEquals(feed.getClusters().getCluster().get(0).getValidity()
				.getTimezone(), "UTC");
		assertEquals(feed.getClusters().getCluster().get(0).getRetention()
				.getAction(), ActionType.DELETE);
		assertEquals(feed.getClusters().getCluster().get(0).getRetention()
				.getLimit(), "hours(48)");

		assertEquals(feed.getClusters().getCluster().get(1).getName(),
				"backupCluster");
		assertEquals(feed.getClusters().getCluster().get(1).getType(),
				ClusterType.TARGET);
		assertEquals(feed.getClusters().getCluster().get(1).getValidity()
				.getStart(), "2011-11-01T00:00Z");
		assertEquals(feed.getClusters().getCluster().get(1).getValidity()
				.getEnd(), "2011-12-31T00:00Z");
		assertEquals(feed.getClusters().getCluster().get(1).getValidity()
				.getTimezone(), "UTC");
		assertEquals(feed.getClusters().getCluster().get(1).getRetention()
				.getAction(), ActionType.ARCHIVE);
		assertEquals(feed.getClusters().getCluster().get(1).getRetention()
				.getLimit(), "hours(6)");

		assertEquals(feed.getLocations().get(LocationType.DATA).getType(),
				"data");
		assertEquals(feed.getLocations().get(LocationType.DATA).getPath(),
				"/projects/ivory/clicks");
		assertEquals(feed.getLocations().get(LocationType.META).getType(),
				"meta");
		assertEquals(feed.getLocations().get(LocationType.META).getPath(),
				"/projects/ivory/clicksMetaData");
		assertEquals(feed.getLocations().get(LocationType.STATS).getType(),
				"stats");
		assertEquals(feed.getLocations().get(LocationType.STATS).getPath(),
				"/projects/ivory/clicksStats");

		assertEquals(feed.getACL().getGroup(), "group");
		assertEquals(feed.getACL().getOwner(), "testuser");
		assertEquals(feed.getACL().getPermission(), "0x755");

		assertEquals(feed.getSchema().getLocation(), "/schema/clicks");
		assertEquals(feed.getSchema().getProvider(), "protobuf");

		assertEquals(feed.getProperties().get("field1").getName(), "field1");
		assertEquals(feed.getProperties().get("field1").getValue(), "value1");
		assertEquals(feed.getProperties().get("field2").getName(), "field2");
		assertEquals(feed.getProperties().get("field2").getValue(), "value2");

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
		feed.getClusters().getCluster().get(0).setName("invalid cluster");
		parser.validate(feed);
	}

	@Test
	public void testInvalidFrequency() {
		try {
			modifiableFeed.setFrequency("hour");
			parser.parseAndValidate(marshallEntity(modifiableFeed));
			Assert.fail("Frequency validation failed");
		} catch (Exception e) {
			modifiableFeed.setFrequency("hours");
			System.out.println(e.getMessage());
		}
	}
	
	@Test
	public void testPartitionExpression() throws IvoryException {
		Feed feed = (Feed) parser
				.parseAndValidate(ProcessEntityParserTest.class
						.getResourceAsStream(FEED_XML));
		feed.setPartitions(null);
		parser.validate(feed);
	}

	@Test
	public void testInvalidPeriodicity() throws IvoryException, JAXBException {
		try {
			modifiableFeed.setPeriodicity(-1);
			parser.parseAndValidate(marshallEntity(modifiableFeed));
			Assert.fail("Periodicity validation failed");
		} catch (Exception e) {
			System.out.println(e.getMessage());
			modifiableFeed.setPeriodicity(1);
		}
	}

	@Test
	public void testInvalidClusterValidityTime() {
		Validity validity = modifiableFeed.getClusters().getCluster().get(0)
				.getValidity();
		try {
			validity.setStart("2007-02-29T00:00Z");
			modifiableFeed.getClusters().getCluster().get(0)
					.setValidity(validity);
			parser.parseAndValidate(marshallEntity(modifiableFeed));
			Assert.fail("Cluster validity failed");
		} catch (Exception e) {
			System.out.println(e.getMessage());
			validity.setStart("2011-11-01T00:00Z");
			modifiableFeed.getClusters().getCluster().get(0)
					.setValidity(validity);
		}

		try {
			validity.setEnd("2010-04-31T00:00Z");
			modifiableFeed.getClusters().getCluster().get(0)
					.setValidity(validity);
			parser.parseAndValidate(marshallEntity(modifiableFeed));
			Assert.fail("Cluster validity failed");
		} catch (Exception e) {
			System.out.println(e.getMessage());
			validity.setEnd("2011-12-31T00:00Z");
			modifiableFeed.getClusters().getCluster().get(0)
					.setValidity(validity);
		}
	}

	@Test
	public void testInvalidLateArrival() throws IvoryException, JAXBException {
		try {
			LateArrival lateArrival = modifiableFeed.getLateArrival();
			lateArrival.setCutOff("hourss(6)");
			modifiableFeed.setLateArrival(lateArrival);
			parser.parseAndValidate(marshallEntity(modifiableFeed));
			Assert.fail("Periodicity validation failed");
		} catch (Exception e) {
			System.out.println(e.getMessage());
			LateArrival lateArrival = new LateArrival();
			lateArrival.setCutOff("hours(6)");
			modifiableFeed.setLateArrival(lateArrival);
		}
	}

	@Test(expectedExceptions = ValidationException.class)
	public void testInvalidProcessValidity() throws Exception {
		Feed feed = parser.parseAndValidate((FeedEntityParserTest.class
				.getResourceAsStream(FEED_XML)));
		feed.getClusters().getCluster().get(0).getValidity()
				.setStart("2012-11-01T00:00Z");
		parser.validate(feed);
	}

	@Test(expectedExceptions = ValidationException.class)
	public void testInvalidFeedLateCutoffPeriod() throws IvoryException {
		Feed feed = parser.parseAndValidate((FeedEntityParserTest.class
				.getResourceAsStream(FEED_XML)));
		feed.getLateArrival().setCutOff("hours(7)");
		parser.validate(feed);
	}

	@Test
	public void testValidFeedGroup() throws IvoryException, JAXBException {
		Feed feed1 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
				(FeedEntityParserTest.class.getResourceAsStream(FEED_XML)));
		feed1.setName("f1" + System.currentTimeMillis());
		feed1.setGroups("group1,group2,group3");
		Location location = new Location();
		location.setPath("/projects/bi/rmc/daily/ad/${YEAR}/fraud/${MONTH}-${DAY}/ad");
		location.setType("data");
		feed1.getLocations().put(LocationType.DATA, location);
		parser.parseAndValidate(feed1.toString());
		ConfigurationStore.get().publish(EntityType.FEED, feed1);

		Feed feed2 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
				(FeedEntityParserTest.class.getResourceAsStream(FEED_XML)));
		feed2.setName("f2" + System.currentTimeMillis());
		feed2.setGroups("group1,group2,group5");
		Location location2 = new Location();
		location2
				.setPath("/projects/bi/rmc/daily/ad/${YEAR}/fraud/${MONTH}-${DAY}/ad");
		location2.setType("data");
		feed2.getLocations().put(LocationType.DATA, location2);
		parser.parseAndValidate(feed2.toString());
	}

	@Test(expectedExceptions = ValidationException.class)
	public void testInvalidFeedGroup() throws IvoryException, JAXBException {
		Feed feed1 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
				(FeedEntityParserTest.class.getResourceAsStream(FEED_XML)));
		feed1.setName("f1" + System.currentTimeMillis());
		feed1.setGroups("group1,group2,group3");
		Location location = new Location();
		location.setPath("/projects/bi/rmc/daily/ad/${YEAR}/fraud/${MONTH}-${DAY}/ad");
		location.setType("data");
		feed1.getLocations().put(LocationType.DATA, location);
		parser.parseAndValidate(feed1.toString());
		ConfigurationStore.get().publish(EntityType.FEED, feed1);

		Feed feed2 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
				(FeedEntityParserTest.class.getResourceAsStream(FEED_XML)));
		feed2.setName("f2" + System.currentTimeMillis());
		feed2.setGroups("group1,group2,group5");
		Location location2 = new Location();
		location2
				.setPath("/projects/bi/rmc/daily/ad/${YEAR}/fraud/${MONTH}/${HOUR}/ad");
		location2.setType("data");
		feed2.getLocations().put(LocationType.DATA, location2);
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
		Assert.assertEquals(FeedEntityParser.getPartitionExpValue(cluster,
				"/*/${cluster.colo}"), "/*/" + cluster.getColo());
		Assert.assertEquals(FeedEntityParser.getPartitionExpValue(cluster,
				"/*/${cluster.name}/Local"), "/*/" + cluster.getName()+"/Local");
		Assert.assertEquals(FeedEntityParser.getPartitionExpValue(cluster,
				"/*/${cluster.field1}/Local"), "/*/value1/Local");
	}
}
