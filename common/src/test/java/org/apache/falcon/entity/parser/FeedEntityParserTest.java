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

package org.apache.falcon.entity.parser;

import org.apache.falcon.FalconException;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.AbstractTestBase;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.datasource.Datasource;
import org.apache.falcon.entity.v0.feed.ActionType;
import org.apache.falcon.entity.v0.feed.Argument;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.entity.v0.feed.ExtractMethod;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.entity.v0.feed.MergeType;
import org.apache.falcon.entity.v0.feed.Partition;
import org.apache.falcon.entity.v0.feed.Partitions;
import org.apache.falcon.entity.v0.feed.Property;
import org.apache.falcon.entity.v0.feed.Validity;
import org.apache.falcon.group.FeedGroupMapTest;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.service.LifecyclePolicyMap;
import org.apache.falcon.util.FalconTestUtil;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Map;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Test Cases for Feed entity parser.
 */
public class FeedEntityParserTest extends AbstractTestBase {

    private final FeedEntityParser parser = (FeedEntityParser) EntityParserFactory
            .getParser(EntityType.FEED);

    private Feed modifiableFeed;

    @BeforeMethod
    public void setUp() throws Exception {
        cleanupStore();
        ConfigurationStore store = ConfigurationStore.get();

        this.dfsCluster = EmbeddedCluster.newCluster("testCluster");
        this.conf = dfsCluster.getConf();

        Unmarshaller unmarshaller = EntityType.CLUSTER.getUnmarshaller();
        Cluster cluster = (Cluster) unmarshaller.unmarshal(this.getClass()
                .getResourceAsStream(CLUSTER_XML));
        cluster.setName("testCluster");
        cluster.setVersion(0);
        store.publish(EntityType.CLUSTER, cluster);

        cluster = (Cluster) unmarshaller.unmarshal(this.getClass()
                .getResourceAsStream(CLUSTER_XML));
        cluster.setName("backupCluster");
        cluster.setVersion(1);
        store.publish(EntityType.CLUSTER, cluster);

        LifecyclePolicyMap.get().init();
        CurrentUser.authenticate(FalconTestUtil.TEST_USER_2);
        modifiableFeed = parser.parseAndValidate(this.getClass().getResourceAsStream(FEED_XML));
        Unmarshaller dsUnmarshaller = EntityType.DATASOURCE.getUnmarshaller();
        Datasource ds = (Datasource) dsUnmarshaller.unmarshal(this.getClass()
                .getResourceAsStream(DATASOURCE_XML));
        ds.setName("test-hsql-db");
        store.publish(EntityType.DATASOURCE, ds);
    }

    @Test(expectedExceptions = ValidationException.class)
    public void testValidations() throws Exception {
        ConfigurationStore.get().remove(EntityType.CLUSTER, "backupCluster");
        parser.parseAndValidate(this.getClass().getResourceAsStream(FEED_XML));
    }

    @Test
    public void testParse() throws IOException, FalconException, JAXBException {

        Feed feed = parser.parseAndValidate(this.getClass()
                .getResourceAsStream(FEED_XML));

        Assert.assertNotNull(feed);
        assertEquals(feed.getName(), "clicks");
        assertEquals(feed.getDescription(), "clicks log");
        assertEquals(feed.getFrequency().toString(), "hours(1)");
        assertEquals(feed.getSla().getSlaHigh().toString(), "hours(3)");
        assertEquals(feed.getSla().getSlaLow().toString(), "hours(2)");
        assertEquals(feed.getGroups(), "online,bi");
        Assert.assertEquals(feed.getVersion(), 0);

        assertEquals(feed.getClusters().getClusters().get(0).getName(),
                "testCluster");
        assertEquals(feed.getClusters().getClusters().get(0).getSla().getSlaLow().toString(), "hours(3)");
        assertEquals(feed.getClusters().getClusters().get(0).getSla().getSlaHigh().toString(), "hours(4)");
        assertEquals(feed.getClusters().getClusters().get(0).getVersion(), 0);
        assertEquals(feed.getClusters().getClusters().get(1).getVersion(), 1);

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
                .getAction(), ActionType.DELETE);
        assertEquals(feed.getClusters().getClusters().get(1).getRetention()
                .getLimit().toString(), "hours(6)");

        assertEquals("${nameNode}/projects/falcon/clicks",
                FeedHelper.createStorage(feed).getUriTemplate(LocationType.DATA));
        assertEquals("${nameNode}/projects/falcon/clicksMetaData",
                FeedHelper.createStorage(feed).getUriTemplate(LocationType.META));
        assertEquals("${nameNode}/projects/falcon/clicksStats",
                FeedHelper.createStorage(feed).getUriTemplate(LocationType.STATS));

        assertEquals(feed.getACL().getGroup(), "group");
        assertEquals(feed.getACL().getOwner(), FalconTestUtil.TEST_USER_2);
        assertEquals(feed.getACL().getPermission(), "0x755");

        assertEquals(feed.getSchema().getLocation(), "/schema/clicks");
        assertEquals(feed.getSchema().getProvider(), "protobuf");

        StringWriter stringWriter = new StringWriter();
        Marshaller marshaller = EntityType.FEED.getMarshaller();
        marshaller.marshal(feed, stringWriter);
        System.out.println(stringWriter.toString());
    }

    @Test
    public void testLifecycleParse() throws Exception {
        Feed feed = parser.parseAndValidate(this.getClass()
                .getResourceAsStream(FEED3_XML));
        assertEquals("hours(17)", feed.getLifecycle().getRetentionStage().getFrequency().toString());
        assertEquals("AgeBasedDelete", FeedHelper.getPolicies(feed, "testCluster").get(0));
        assertEquals("reports", feed.getLifecycle().getRetentionStage().getQueue());
        assertEquals("NORMAL", feed.getLifecycle().getRetentionStage().getPriority());
    }

    @Test(expectedExceptions = ValidationException.class,
            expectedExceptionsMessageRegExp = ".*Retention is a mandatory stage.*")
    public void testMandatoryRetention() throws Exception {
        Feed feed = parser.parseAndValidate(this.getClass()
                .getResourceAsStream(FEED3_XML));
        feed.getLifecycle().setRetentionStage(null);
        parser.validate(feed);
    }

    @Test
    public void testValidRetentionFrequency() throws Exception {
        Feed feed = parser.parseAndValidate(this.getClass()
                .getResourceAsStream(FEED3_XML));

        feed.setFrequency(Frequency.fromString("minutes(30)"));
        Frequency frequency = Frequency.fromString("minutes(60)");
        feed.getLifecycle().getRetentionStage().setFrequency(frequency);
        parser.validate(feed); // no validation exception should be thrown

        frequency = Frequency.fromString("hours(1)");
        feed.getLifecycle().getRetentionStage().setFrequency(frequency);
        parser.validate(feed); // no validation exception should be thrown
    }

    @Test
    public void testDefaultRetentionFrequencyConflict() throws Exception {
        Feed feed = parser.parseAndValidate(this.getClass().getResourceAsStream(FEED3_XML));
        feed.getLifecycle().getRetentionStage().setFrequency(null);
        feed.getClusters().getClusters().get(0).getLifecycle().getRetentionStage().setFrequency(null);
        feed.setFrequency(Frequency.fromString("minutes(10)"));
        parser.validate(feed); // shouldn't throw a validation exception


        feed.setFrequency(Frequency.fromString("hours(7)"));
        parser.validate(feed); // shouldn't throw a validation exception

        feed.setFrequency(Frequency.fromString("days(2)"));
        parser.validate(feed); // shouldn't throw a validation exception
    }

    @Test(expectedExceptions = ValidationException.class,
        expectedExceptionsMessageRegExp = ".*Retention can not be more frequent than data availability.*")
    public void testRetentionFrequentThanFeed() throws Exception {
        Feed feed = parser.parseAndValidate(this.getClass()
                .getResourceAsStream(FEED3_XML));

        feed.setFrequency(Frequency.fromString("hours(2)"));
        Frequency frequency = Frequency.fromString("minutes(60)");
        feed.getLifecycle().getRetentionStage().setFrequency(frequency);
        parser.validate(feed);
    }

    @Test(expectedExceptions = ValidationException.class,
        expectedExceptionsMessageRegExp = ".*Feed Retention can not be more frequent than.*")
    public void testRetentionFrequency() throws Exception {
        Feed feed = parser.parseAndValidate(this.getClass()
                .getResourceAsStream(FEED3_XML));

        feed.setFrequency(Frequency.fromString("minutes(30)"));
        Frequency frequency = Frequency.fromString("minutes(59)");
        feed.getLifecycle().getRetentionStage().setFrequency(frequency);
        parser.validate(feed);
    }

    @Test(expectedExceptions = ValidationException.class)
    public void applyValidationInvalidFeed() throws Exception {
        Feed feed = parser.parseAndValidate(ProcessEntityParserTest.class
                .getResourceAsStream(FEED_XML));
        feed.getClusters().getClusters().get(0).setName("invalid cluster");
        parser.validate(feed);
    }

    @Test
    public void testPartitionExpression() throws FalconException {
        Feed feed = parser.parseAndValidate(ProcessEntityParserTest.class
                .getResourceAsStream(FEED_XML));

        //When there are more than 1 src clusters, there should be partition expression
        org.apache.falcon.entity.v0.feed.Cluster newCluster = new org.apache.falcon.entity.v0.feed.Cluster();
        newCluster.setName("newCluster");
        newCluster.setType(ClusterType.SOURCE);
        newCluster.setPartition("${cluster.colo}");
        feed.getClusters().getClusters().add(newCluster);
        try {
            parser.validate(feed);
            Assert.fail("Expected ValidationException");
        } catch (ValidationException ignore) {
            //ignore
        }

        //When there are more than 1 src clusters, the partition expression should contain cluster variable
        feed.getClusters().getClusters().get(0).setPartition("*");
        try {
            parser.validate(feed);
            Assert.fail("Expected ValidationException");
        } catch (ValidationException ignore) {
            //ignore
        }

        //When there are more than 1 target cluster, there should be partition expre
        newCluster.setType(ClusterType.TARGET);
        try {
            parser.validate(feed);
            Assert.fail("Expected ValidationException");
        } catch (ValidationException ignore) {
            //ignore
        }

        //When there are more than 1 target clusters, the partition expression should contain cluster variable
        feed.getClusters().getClusters().get(1).setPartition("*");
        try {
            parser.validate(feed);
            Assert.fail("Expected ValidationException");
        } catch (ValidationException ignore) {
            //ignore
        }

        //Number of parts in partition expression < number of partitions defined for feed
        feed.getClusters().getClusters().get(1).setPartition("*/*");
        try {
            parser.validate(feed);
            Assert.fail("Expected ValidationException");
        } catch (ValidationException ignore) {
            //ignore
        }

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

    @Test(expectedExceptions = ValidationException.class, expectedExceptionsMessageRegExp = "slaLow of Feed:.*")
    public void testInvalidSlaLow() throws Exception {
        Feed feed = parser.parseAndValidate((FeedEntityParserTest.class
                .getResourceAsStream(FEED_XML)));
        feed.getSla().setSlaLow(new Frequency("hours(4)"));
        feed.getSla().setSlaHigh(new Frequency("hours(2)"));
        parser.validate(feed);
    }


    @Test(expectedExceptions = ValidationException.class, expectedExceptionsMessageRegExp = "slaHigh of Feed:.*")
    public void testInvalidSlaHigh() throws Exception {
        Feed feed = parser.parseAndValidate((FeedEntityParserTest.class
                .getResourceAsStream(FEED_XML)));
        feed.getSla().setSlaLow(new Frequency("hours(2)"));
        feed.getSla().setSlaHigh(new Frequency("hours(10)"));
        feed.getClusters().getClusters().get(0).getRetention().setLimit(new Frequency("hours(9)"));
        parser.validate(feed);
    }


    @Test
    public void testValidFeedGroup() throws FalconException, JAXBException {
        Feed feed1 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
                (FeedEntityParserTest.class.getResourceAsStream(FEED_XML)));
        feed1.setName("f1" + System.currentTimeMillis());
        feed1.setGroups("group1,group2,group3");
        feed1.setLocations(new Locations());
        Location location = new Location();
        location.setPath("/projects/bi/rmc/daily/ad/${YEAR}/fraud/${MONTH}-${DAY}/ad");
        location.setType(LocationType.DATA);
        feed1.getLocations().getLocations().add(location);
        feed1.getClusters().getClusters().get(0).getLocations().getLocations().set(0, location);
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
        feed2.getClusters().getClusters().get(0).getLocations().getLocations().set(0, location);
        parser.parseAndValidate(feed2.toString());
    }

    // TODO Disabled the test since I do not see anything invalid in here.
    @Test(enabled = false, expectedExceptions = ValidationException.class)
    public void testInvalidFeedClusterDataLocation() throws JAXBException, FalconException {
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
    }

    @Test(expectedExceptions = ValidationException.class)
    public void testInvalidFeedGroup() throws FalconException, JAXBException {
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

        feed1.getClusters().getClusters().get(0).getLocations().getLocations().set(0, location);
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
        feed2.getClusters().getClusters().get(0).getLocations().getLocations().set(0, location);
        parser.parseAndValidate(feed2.toString());
    }

    @Test
    public void testValidGroupNames() throws FalconException, JAXBException {
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
    public void testInvalidGroupNames() throws FalconException, JAXBException {
        Feed feed1 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
                FeedGroupMapTest.class
                        .getResourceAsStream("/config/feed/feed-0.1.xml"));
        feed1.setName("f1" + System.currentTimeMillis());

        try {
            feed1.setGroups("commaend,");
            parser.parseAndValidate(feed1.toString());
            Assert.fail("Expected exception");
        } catch (FalconException ignore) {
            //ignore
        }
        try {
            feed1.setGroups("group8,   group9");
            parser.parseAndValidate(feed1.toString());
            Assert.fail("Expected exception");
        } catch (FalconException e) {
            //ignore
        }
        try {
            feed1.setGroups("space in group,group9");
            parser.parseAndValidate(feed1.toString());
            Assert.fail("Expected exception");
        } catch (FalconException e) {
            //ignore
        }
    }

    @Test
    public void testClusterPartitionExp() throws FalconException {
        Cluster cluster = ConfigurationStore.get().get(EntityType.CLUSTER,
                "testCluster");
        Assert.assertEquals(FeedHelper.evaluateClusterExp(cluster,
                "/*/${cluster.colo}"), "/*/" + cluster.getColo());
        Assert.assertEquals(FeedHelper.evaluateClusterExp(cluster,
                "/*/${cluster.name}/Local"), "/*/" + cluster.getName() + "/Local");
        Assert.assertEquals(FeedHelper.evaluateClusterExp(cluster,
                "/*/${cluster.field1}/Local"), "/*/value1/Local");
    }

    @Test(expectedExceptions = FalconException.class)
    public void testInvalidFeedName() throws JAXBException, FalconException {
        Feed feed1 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
                FeedGroupMapTest.class
                        .getResourceAsStream("/config/feed/feed-0.1.xml"));
        feed1.setName("Feed_name");
        parser.parseAndValidate(feed1.toString());
    }

    @Test(expectedExceptions = FalconException.class)
    public void testInvalidFeedGroupName() throws JAXBException, FalconException {
        Feed feed1 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
                FeedGroupMapTest.class
                        .getResourceAsStream("/config/feed/feed-0.1.xml"));
        feed1.setName("feed1");
        feed1.getLocations().getLocations().get(0)
                .setPath("/data/clicks/${YEAR}/${MONTH}/${DAY}/${HOUR}");
        feed1.getClusters().getClusters().get(0).getLocations().getLocations()
                .get(0).setPath("/data/clicks/${YEAR}/${MONTH}/${DAY}/${HOUR}");
        ConfigurationStore.get().publish(EntityType.FEED, feed1);

        Feed feed2 = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
                FeedGroupMapTest.class
                        .getResourceAsStream("/config/feed/feed-0.1.xml"));
        feed2.setName("feed2");
        feed2.getLocations().getLocations().get(0).setPath("/data/clicks/${YEAR}/${MONTH}/${DAY}/${HOUR}");
        feed2.getClusters().getClusters().get(0).getLocations().getLocations()
                .get(0).setPath("/data/clicks/${YEAR}/${MONTH}/${DAY}/${HOUR}");
        feed2.setFrequency(new Frequency("hours(1)"));
        try {
            parser.parseAndValidate(feed2.toString());
        } catch (FalconException e) {
            e.printStackTrace();
            Assert.fail("Not expecting exception for same frequency");
        }
        feed2.setFrequency(new Frequency("hours(2)"));
        //expecting exception
        parser.parseAndValidate(feed2.toString());
    }

    @Test
    public void testNullFeedLateArrival() throws JAXBException, FalconException {
        Feed feed = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
                FeedGroupMapTest.class
                        .getResourceAsStream("/config/feed/feed-0.1.xml"));

        feed.setLateArrival(null);
        parser.parseAndValidate(feed.toString());

    }

    /**
     * A negative test for validating tags key value pair regex: key=value, key=value.
     * @throws FalconException
     */
    @Test
    public void testFeedTags() throws FalconException {
        try {
            InputStream stream = this.getClass().getResourceAsStream("/config/feed/feed-tags-0.1.xml");
            parser.parse(stream);
            Assert.fail("org.xml.sax.SAXParseException should have been thrown.");
        } catch (FalconException e) {
            Assert.assertEquals(javax.xml.bind.UnmarshalException.class, e.getCause().getClass());
            Assert.assertEquals(org.xml.sax.SAXParseException.class, e.getCause().getCause().getClass());
        }
    }

    @Test
    public void testParseFeedWithTable() throws FalconException {
        final InputStream inputStream = getClass().getResourceAsStream("/config/feed/hive-table-feed.xml");
        Feed feedWithTable = parser.parse(inputStream);
        Assert.assertEquals(feedWithTable.getTable().getUri(),
                "catalog:default:clicks#ds=${YEAR}-${MONTH}-${DAY}-${HOUR}");
    }

    @Test (expectedExceptions = FalconException.class)
    public void testParseInvalidFeedWithTable() throws FalconException {
        parser.parse(FeedEntityParserTest.class.getResourceAsStream("/config/feed/invalid-feed.xml"));
    }

    @Test (expectedExceptions = FalconException.class)
    public void testValidateFeedWithTableAndMultipleSources() throws FalconException {
        parser.parseAndValidate(FeedEntityParserTest.class.getResourceAsStream(
                "/config/feed/table-with-multiple-sources-feed.xml"));
        Assert.fail("Should have thrown an exception:Multiple sources are not supported for feed with table storage");
    }

    @Test(expectedExceptions = ValidationException.class)
    public void testValidatePartitionsForTable() throws Exception {
        Feed feed = parser.parse(FeedEntityParserTest.class.getResourceAsStream("/config/feed/hive-table-feed.xml"));
        Assert.assertNull(feed.getPartitions());

        Partitions partitions = new Partitions();
        Partition partition = new Partition();
        partition.setName("colo");
        partitions.getPartitions().add(partition);
        feed.setPartitions(partitions);

        parser.validate(feed);
        Assert.fail("An exception should have been thrown:Partitions are not supported for feeds with table storage");
    }

    @Test(expectedExceptions = ValidationException.class)
    public void testValidateClusterHasRegistryWithNoRegistryInterfaceEndPoint() throws Exception {
        final InputStream inputStream = getClass().getResourceAsStream("/config/feed/hive-table-feed.xml");
        Feed feedWithTable = parser.parse(inputStream);

        org.apache.falcon.entity.v0.cluster.Cluster clusterEntity = EntityUtil.getEntity(EntityType.CLUSTER,
                feedWithTable.getClusters().getClusters().get(0).getName());
        ClusterHelper.getInterface(clusterEntity, Interfacetype.REGISTRY).setEndpoint(null);

        parser.validate(feedWithTable);
        Assert.fail("An exception should have been thrown: Cluster should have registry interface defined with table "
                + "storage");
    }

    @Test(expectedExceptions = ValidationException.class)
    public void testValidateClusterHasRegistryWithNoRegistryInterface() throws Exception {
        Unmarshaller unmarshaller = EntityType.CLUSTER.getUnmarshaller();
        Cluster cluster = (Cluster) unmarshaller.unmarshal(this.getClass()
                .getResourceAsStream(("/config/cluster/cluster-no-registry.xml")));
        cluster.setName("badTestCluster");
        cluster.setVersion(0);
        ConfigurationStore.get().publish(EntityType.CLUSTER, cluster);


        final InputStream inputStream = getClass().getResourceAsStream("/config/feed/hive-table-feed.xml");
        Feed feedWithTable = parser.parse(inputStream);
        Validity validity = modifiableFeed.getClusters().getClusters().get(0)
                .getValidity();
        feedWithTable.getClusters().getClusters().clear();

        org.apache.falcon.entity.v0.feed.Cluster feedCluster =
                new org.apache.falcon.entity.v0.feed.Cluster();
        feedCluster.setName(cluster.getName());
        feedCluster.setValidity(validity);
        feedWithTable.getClusters().getClusters().add(feedCluster);

        parser.validate(feedWithTable);
        Assert.fail("An exception should have been thrown: Cluster should have registry interface defined with table"
                + " storage");
    }

    @Test(expectedExceptions = ValidationException.class)
    public void testValidateOwner() throws Exception {
        CurrentUser.authenticate("unknown");
        StartupProperties.get().setProperty("falcon.security.authorization.enabled", "true");
        Assert.assertTrue(Boolean.valueOf(
                StartupProperties.get().getProperty("falcon.security.authorization.enabled")));
        try {
            // need a new parser since it caches authorization enabled flag
            FeedEntityParser feedEntityParser =
                    (FeedEntityParser) EntityParserFactory.getParser(EntityType.FEED);
            feedEntityParser.parseAndValidate(this.getClass().getResourceAsStream(FEED_XML));
        } finally {
            StartupProperties.get().setProperty("falcon.security.authorization.enabled", "false");
        }
    }

    @Test
    public void testValidateACLWithACLAndAuthorizationDisabled() throws Exception {
        InputStream stream = this.getClass().getResourceAsStream(FEED_XML);

        Feed feed = parser.parse(stream);
        Assert.assertNotNull(feed);
        Assert.assertNotNull(feed.getACL());
        Assert.assertNotNull(feed.getACL().getOwner());
        Assert.assertNotNull(feed.getACL().getGroup());
        Assert.assertNotNull(feed.getACL().getPermission());

        parser.validate(feed);
    }

    @Test
    public void testValidateACLOwner() throws Exception {
        StartupProperties.get().setProperty("falcon.security.authorization.enabled", "true");
        Assert.assertTrue(Boolean.valueOf(
                StartupProperties.get().getProperty("falcon.security.authorization.enabled")));

        CurrentUser.authenticate(USER);
        try {
            InputStream stream = this.getClass().getResourceAsStream(FEED_XML);

            // need a new parser since it caches authorization enabled flag
            FeedEntityParser feedEntityParser =
                    (FeedEntityParser) EntityParserFactory.getParser(EntityType.FEED);
            Feed feed = feedEntityParser.parse(stream);

            Assert.assertNotNull(feed);
            Assert.assertNotNull(feed.getACL());
            feed.getACL().setOwner(USER);
            feed.getACL().setGroup(getPrimaryGroupName());

            feedEntityParser.validate(feed);
        } finally {
            StartupProperties.get().setProperty("falcon.security.authorization.enabled", "false");
        }
    }

    @Test (expectedExceptions = ValidationException.class)
    public void testValidateACLBadOwner() throws Exception {
        StartupProperties.get().setProperty("falcon.security.authorization.enabled", "true");
        Assert.assertTrue(Boolean.valueOf(
                StartupProperties.get().getProperty("falcon.security.authorization.enabled")));
        CurrentUser.authenticate("blah");

        try {
            InputStream stream = this.getClass().getResourceAsStream(FEED_XML);

            // need a new parser since it caches authorization enabled flag
            FeedEntityParser feedEntityParser =
                    (FeedEntityParser) EntityParserFactory.getParser(EntityType.FEED);
            Feed feed = feedEntityParser.parse(stream);

            Assert.assertNotNull(feed);
            Assert.assertNotNull(feed.getACL());
            Assert.assertNotNull(feed.getACL().getOwner());
            Assert.assertNotNull(feed.getACL().getGroup());
            Assert.assertNotNull(feed.getACL().getPermission());

            feedEntityParser.validate(feed);
            Assert.fail("Validation exception should have been thrown for invalid owner");
        } finally {
            StartupProperties.get().setProperty("falcon.security.authorization.enabled", "false");
        }
    }

    @Test (expectedExceptions = ValidationException.class)
    public void testValidateACLBadOwnerAndGroup() throws Exception {
        StartupProperties.get().setProperty("falcon.security.authorization.enabled", "true");
        Assert.assertTrue(Boolean.valueOf(
                StartupProperties.get().getProperty("falcon.security.authorization.enabled")));
        CurrentUser.authenticate("blah");

        try {
            InputStream stream = this.getClass().getResourceAsStream(FEED_XML);

            // need a new parser since it caches authorization enabled flag
            FeedEntityParser feedEntityParser =
                    (FeedEntityParser) EntityParserFactory.getParser(EntityType.FEED);
            Feed feed = feedEntityParser.parse(stream);

            Assert.assertNotNull(feed);
            Assert.assertNotNull(feed.getACL());
            Assert.assertNotNull(feed.getACL().getOwner());
            Assert.assertNotNull(feed.getACL().getGroup());
            Assert.assertNotNull(feed.getACL().getPermission());

            feedEntityParser.validate(feed);
            Assert.fail("Validation exception should have been thrown for invalid owner");
        } finally {
            StartupProperties.get().setProperty("falcon.security.authorization.enabled", "false");
        }
    }

    @Test (expectedExceptions = ValidationException.class)
    public void testValidateACLAndStorageBadOwner() throws Exception {
        StartupProperties.get().setProperty("falcon.security.authorization.enabled", "true");
        Assert.assertTrue(Boolean.valueOf(
                StartupProperties.get().getProperty("falcon.security.authorization.enabled")));

        Feed feed = null;
        try {
            InputStream stream = this.getClass().getResourceAsStream(FEED_XML);

            // need a new parser since it caches authorization enabled flag
            FeedEntityParser feedEntityParser =
                    (FeedEntityParser) EntityParserFactory.getParser(EntityType.FEED);
            feed = feedEntityParser.parse(stream);
            Assert.assertNotNull(feed);
            Assert.assertNotNull(feed.getACL());
            Assert.assertNotNull(feed.getACL().getOwner());
            Assert.assertNotNull(feed.getACL().getGroup());
            Assert.assertNotNull(feed.getACL().getPermission());

            // create locations
            createLocations(feed);
            feedEntityParser.validate(feed);
        } finally {
            if (feed != null) {
                deleteLocations(feed);
            }
            StartupProperties.get().setProperty("falcon.security.authorization.enabled", "false");
        }
    }

    @Test (expectedExceptions = ValidationException.class)
    public void testValidateACLAndStorageBadOwnerAndGroup() throws Exception {
        StartupProperties.get().setProperty("falcon.security.authorization.enabled", "true");
        Assert.assertTrue(Boolean.valueOf(
                StartupProperties.get().getProperty("falcon.security.authorization.enabled")));

        Feed feed = null;
        try {
            InputStream stream = this.getClass().getResourceAsStream(FEED_XML);

            // need a new parser since it caches authorization enabled flag
            FeedEntityParser feedEntityParser =
                    (FeedEntityParser) EntityParserFactory.getParser(EntityType.FEED);
            feed = feedEntityParser.parse(stream);
            Assert.assertNotNull(feed);
            Assert.assertNotNull(feed.getACL());
            Assert.assertNotNull(feed.getACL().getOwner());
            Assert.assertNotNull(feed.getACL().getGroup());
            Assert.assertNotNull(feed.getACL().getPermission());

            // create locations
            createLocations(feed);
            feedEntityParser.validate(feed);
        } finally {
            if (feed != null) {
                deleteLocations(feed);
            }
            StartupProperties.get().setProperty("falcon.security.authorization.enabled", "false");
        }
    }

    @Test (expectedExceptions = ValidationException.class)
    public void testValidateACLAndStorageForValidOwnerBadGroup() throws Exception {
        CurrentUser.authenticate(USER);
        StartupProperties.get().setProperty("falcon.security.authorization.enabled", "true");
        Assert.assertTrue(Boolean.valueOf(
                StartupProperties.get().getProperty("falcon.security.authorization.enabled")));

        Feed feed = null;
        try {
            InputStream stream = this.getClass().getResourceAsStream(FEED_XML);

            // need a new parser since it caches authorization enabled flag
            FeedEntityParser feedEntityParser = (FeedEntityParser) EntityParserFactory.getParser(EntityType.FEED);
            feed = feedEntityParser.parse(stream);
            Assert.assertNotNull(feed);
            Assert.assertNotNull(feed.getACL());
            Assert.assertNotNull(feed.getACL().getOwner());
            Assert.assertNotNull(feed.getACL().getGroup());
            Assert.assertNotNull(feed.getACL().getPermission());

            feed.getACL().setOwner(USER);

            // create locations
            createLocations(feed);
            feedEntityParser.validate(feed);
        } finally {
            if (feed != null) {
                deleteLocations(feed);
            }
            StartupProperties.get().setProperty("falcon.security.authorization.enabled", "false");
        }
    }

    @Test (expectedExceptions = ValidationException.class)
    public void testValidateACLValidGroupBadOwner() throws Exception {
        CurrentUser.authenticate(USER);
        StartupProperties.get().setProperty("falcon.security.authorization.enabled", "true");
        Assert.assertTrue(Boolean.valueOf(
                StartupProperties.get().getProperty("falcon.security.authorization.enabled")));

        try {
            InputStream stream = this.getClass().getResourceAsStream(FEED_XML);

            // need a new parser since it caches authorization enabled flag
            FeedEntityParser feedEntityParser = (FeedEntityParser) EntityParserFactory.getParser(
                    EntityType.FEED);
            Feed feed = feedEntityParser.parse(stream);
            Assert.assertNotNull(feed);
            Assert.assertNotNull(feed.getACL());
            Assert.assertNotNull(feed.getACL().getOwner());
            Assert.assertNotNull(feed.getACL().getGroup());
            Assert.assertNotNull(feed.getACL().getPermission());

            feed.getACL().setGroup(getPrimaryGroupName());

            feedEntityParser.validate(feed);
        } finally {
            StartupProperties.get().setProperty("falcon.security.authorization.enabled", "false");
        }
    }

    @Test (expectedExceptions = ValidationException.class)
    public void testValidateACLAndStorageForInvalidOwnerAndGroup() throws Exception {
        StartupProperties.get().setProperty("falcon.security.authorization.enabled", "true");
        Assert.assertTrue(Boolean.valueOf(
                StartupProperties.get().getProperty("falcon.security.authorization.enabled")));

        Feed feed = null;
        try {
            InputStream stream = this.getClass().getResourceAsStream(FEED_XML);

            // need a new parser since it caches authorization enabled flag
            FeedEntityParser feedEntityParser = (FeedEntityParser) EntityParserFactory.getParser(
                    EntityType.FEED);
            feed = feedEntityParser.parse(stream);
            Assert.assertNotNull(feed);
            Assert.assertNotNull(feed.getACL());
            Assert.assertNotNull(feed.getACL().getOwner());
            Assert.assertNotNull(feed.getACL().getGroup());
            Assert.assertNotNull(feed.getACL().getPermission());

            // create locations
            createLocations(feed);
            feedEntityParser.validate(feed);
        } finally {
            if (feed != null) {
                deleteLocations(feed);
            }
            StartupProperties.get().setProperty("falcon.security.authorization.enabled", "false");
        }
    }

    @Test (expectedExceptions = ValidationException.class)
    public void testValidateACLAndStorageForValidGroupBadOwner() throws Exception {
        CurrentUser.authenticate(USER);
        StartupProperties.get().setProperty("falcon.security.authorization.enabled", "true");
        Assert.assertTrue(Boolean.valueOf(
                StartupProperties.get().getProperty("falcon.security.authorization.enabled")));

        Feed feed = null;
        try {
            InputStream stream = this.getClass().getResourceAsStream(FEED_XML);

            // need a new parser since it caches authorization enabled flag
            FeedEntityParser feedEntityParser = (FeedEntityParser) EntityParserFactory.getParser(
                    EntityType.FEED);
            feed = feedEntityParser.parse(stream);
            Assert.assertNotNull(feed);
            Assert.assertNotNull(feed.getACL());
            Assert.assertNotNull(feed.getACL().getOwner());
            Assert.assertNotNull(feed.getACL().getGroup());
            Assert.assertNotNull(feed.getACL().getPermission());

            feed.getACL().setGroup(getPrimaryGroupName());

            // create locations
            createLocations(feed);
            feedEntityParser.validate(feed);
        } finally {
            if (feed != null) {
                deleteLocations(feed);
            }
            StartupProperties.get().setProperty("falcon.security.authorization.enabled", "false");
        }
    }

    private void createLocations(Feed feed) throws IOException {
        for (Location location : feed.getLocations().getLocations()) {
            if (location.getType() == LocationType.DATA) {
                dfsCluster.getFileSystem().create(new Path(location.getPath()));
                break;
            }
        }
    }

    private void deleteLocations(Feed feed) throws IOException {
        for (Location location : feed.getLocations().getLocations()) {
            if (location.getType() == LocationType.DATA) {
                dfsCluster.getFileSystem().delete(new Path(location.getPath()), true);
                break;
            }
        }
    }

    // disable this test due to its validation of dummy s3 url no longer supported by latest hdfs (2.7.2 or above)
    @Test (enabled = false)
    public void testValidateACLForArchiveReplication() throws Exception {
        StartupProperties.get().setProperty("falcon.security.authorization.enabled", "true");
        Assert.assertTrue(Boolean.valueOf(
                StartupProperties.get().getProperty("falcon.security.authorization.enabled")));

        CurrentUser.authenticate(USER);
        try {
            InputStream stream = this.getClass().getResourceAsStream(FEED_XML);

            // need a new parser since it caches authorization enabled flag
            FeedEntityParser feedEntityParser =
                (FeedEntityParser) EntityParserFactory.getParser(EntityType.FEED);
            Feed feed = feedEntityParser.parse(stream);

            org.apache.falcon.entity.v0.feed.Cluster feedCluster =
                FeedHelper.getCluster(feed, "backupCluster");
            Location location = new Location();
            location.setType(LocationType.DATA);
            location.setPath(
                "s3://falcontesting@hwxasvtesting.blob.core.windows.net/${YEAR}-${MONTH}-${DAY}-${HOUR}-${MINUTE}");
            Locations locations = new Locations();
            locations.getLocations().add(location);
            feedCluster.setLocations(locations);

            Assert.assertNotNull(feed);
            Assert.assertNotNull(feed.getACL());
            feed.getACL().setOwner(USER);
            feed.getACL().setGroup(getPrimaryGroupName());

            try {
                feedEntityParser.validate(feed);
            } catch (IllegalArgumentException e) {
                // this is normal since AWS Secret Access Key is not specified as the password of a s3 URL
            }
        } finally {
            StartupProperties.get().setProperty("falcon.security.authorization.enabled", "false");
        }
    }

    @Test
    public void testImportFeedSqoop() throws Exception {

        storeEntity(EntityType.CLUSTER, "testCluster");
        InputStream feedStream = this.getClass().getResourceAsStream("/config/feed/feed-import-0.1.xml");
        Feed feed = parser.parseAndValidate(feedStream);
        final org.apache.falcon.entity.v0.feed.Cluster srcCluster = feed.getClusters().getClusters().get(0);
        Assert.assertEquals("test-hsql-db", FeedHelper.getImportDatasourceName(srcCluster));
        Assert.assertEquals("customer", FeedHelper.getImportDataSourceTableName(srcCluster));
        Assert.assertEquals(2, srcCluster.getImport().getSource().getFields().getIncludes().getFields().size());
    }

    @Test
    public void testImportFeedSqoopMinimal() throws Exception {

        storeEntity(EntityType.CLUSTER, "testCluster");
        InputStream feedStream = this.getClass().getResourceAsStream("/config/feed/feed-import-noargs-0.1.xml");
        Feed feed = parser.parseAndValidate(feedStream);
        final org.apache.falcon.entity.v0.feed.Cluster srcCluster = feed.getClusters().getClusters().get(0);
        Assert.assertEquals("test-hsql-db", FeedHelper.getImportDatasourceName(srcCluster));
        Assert.assertEquals("customer", FeedHelper.getImportDataSourceTableName(srcCluster));
        Map<String, String> args = FeedHelper.getImportArguments(srcCluster);
        Assert.assertEquals(0, args.size());
    }

    @Test (expectedExceptions = ValidationException.class)
    public void testImportFeedSqoopExcludeFields() throws Exception {

        storeEntity(EntityType.CLUSTER, "testCluster");
        InputStream feedStream = this.getClass().getResourceAsStream("/config/feed/feed-import-exclude-fields-0.1.xml");
        Feed feed = parser.parseAndValidate(feedStream);
        Assert.fail("An exception should have been thrown: Feed Import policy not yet implement Field exclusion.");
    }

    @Test
    public void testImportFeedSqoopArgs() throws Exception {
        final InputStream inputStream = this.getClass().getResourceAsStream("/config/feed/feed-import-0.1.xml");
        Feed importFeed = parser.parse(inputStream);

        org.apache.falcon.entity.v0.feed.Arguments args =
                importFeed.getClusters().getClusters().get(0).getImport().getArguments();

        Argument splitByArg = new Argument();
        splitByArg.setName("--split-by");
        splitByArg.setValue("id");

        Argument numMappersArg = new Argument();
        numMappersArg.setName("--num-mappers");
        numMappersArg.setValue("3");

        args.getArguments().clear();
        args.getArguments().add(numMappersArg);
        args.getArguments().add(splitByArg);

        parser.validate(importFeed);
    }

    @Test
    public void testImportFeedSqoopArgsSplitBy() throws Exception {
        final InputStream inputStream = this.getClass().getResourceAsStream("/config/feed/feed-import-0.1.xml");
        Feed importFeed = parser.parse(inputStream);

        org.apache.falcon.entity.v0.feed.Arguments args =
                importFeed.getClusters().getClusters().get(0).getImport().getArguments();
        Argument splitByArg = new Argument();
        splitByArg.setName("--split-by");
        splitByArg.setValue("id");

        args.getArguments().clear();
        args.getArguments().add(splitByArg);

        parser.validate(importFeed);
    }

    @Test (expectedExceptions = ValidationException.class)
    public void testImportFeedSqoopArgsNumMapper() throws Exception {
        final InputStream inputStream = this.getClass().getResourceAsStream("/config/feed/feed-import-0.1.xml");
        Feed importFeed = parser.parse(inputStream);

        org.apache.falcon.entity.v0.feed.Arguments args =
                importFeed.getClusters().getClusters().get(0).getImport().getArguments();
        Argument numMappersArg = new Argument();
        numMappersArg.setName("--num-mappers");
        numMappersArg.setValue("2");

        args.getArguments().clear();
        args.getArguments().add(numMappersArg);

        parser.validate(importFeed);
        Assert.fail("An exception should have been thrown: Feed Import should specify "
                + "--split-by column along with --num-mappers");
    }

    @Test
    public void testImportFeedExtractionType1() throws Exception {
        final InputStream inputStream = this.getClass().getResourceAsStream("/config/feed/feed-import-0.1.xml");
        Feed importFeed = parser.parse(inputStream);

        org.apache.falcon.entity.v0.feed.Extract extract =
                importFeed.getClusters().getClusters().get(0).getImport().getSource().getExtract();

        extract.setType(ExtractMethod.FULL);
        extract.setMergepolicy(MergeType.SNAPSHOT);

        parser.validate(importFeed);
    }

    @Test (expectedExceptions = ValidationException.class)
    public void testImportFeedExtractionType2() throws Exception {
        final InputStream inputStream = this.getClass().getResourceAsStream("/config/feed/feed-import-0.1.xml");
        Feed importFeed = parser.parse(inputStream);

        org.apache.falcon.entity.v0.feed.Extract extract =
                importFeed.getClusters().getClusters().get(0).getImport().getSource().getExtract();

        extract.setType(ExtractMethod.FULL);
        extract.setMergepolicy(MergeType.APPEND);

        parser.validate(importFeed);
    }

    @Test (expectedExceptions = ValidationException.class)
    public void testImportFeedExtractionType3() throws Exception {
        final InputStream inputStream = this.getClass().getResourceAsStream("/config/feed/feed-import-0.1.xml");
        Feed importFeed = parser.parse(inputStream);

        org.apache.falcon.entity.v0.feed.Extract extract =
                importFeed.getClusters().getClusters().get(0).getImport().getSource().getExtract();

        extract.setType(ExtractMethod.INCREMENTAL);
        extract.setMergepolicy(MergeType.APPEND);

        parser.validate(importFeed);
    }

    @Test (expectedExceptions = {ValidationException.class, FalconException.class})
    public void testImportFeedSqoopInvalid() throws Exception {

        InputStream feedStream = this.getClass().getResourceAsStream("/config/feed/feed-import-invalid-0.1.xml");
        parser.parseAndValidate(feedStream);
        Assert.fail("ValidationException should have been thrown");
    }

    public void testValidateEmailNotification() throws Exception {
        Feed feedNotification = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
                (FeedEntityParserTest.class.getResourceAsStream(FEED_XML)));
        Assert.assertNotNull(feedNotification.getNotification());
        Assert.assertEquals(feedNotification.getNotification().getTo(), "falcon@localhost");
        Assert.assertEquals(feedNotification.getNotification().getType(), "email");
    }

    @Test
    public void testValidateFeedProperties() throws Exception {
        FeedEntityParser feedEntityParser = Mockito
                .spy((FeedEntityParser) EntityParserFactory.getParser(EntityType.FEED));
        InputStream stream = this.getClass().getResourceAsStream("/config/feed/feed-0.1.xml");
        Feed feed = parser.parse(stream);

        Mockito.doNothing().when(feedEntityParser).validateACL(feed);

        // Good set of properties, should work
        feedEntityParser.validate(feed);

        // add duplicate property, should throw validation exception.
        Property property1 = new Property();
        property1.setName("field1");
        property1.setValue("any value");
        feed.getProperties().getProperties().add(property1);
        try {
            feedEntityParser.validate(feed);
            Assert.fail(); // should not reach here
        } catch (ValidationException e) {
            // Do nothing
        }

        // Remove duplicate property. It should not throw exception anymore
        feed.getProperties().getProperties().remove(property1);
        feedEntityParser.validate(feed);

        // add empty property name, should throw validation exception.
        property1.setName("");
        feed.getProperties().getProperties().add(property1);
        try {
            feedEntityParser.validate(feed);
            Assert.fail(); // should not reach here
        } catch (ValidationException e) {
            // Do nothing
        }
    }

    @Test
    public void testFeedEndTimeOptional() throws Exception {
        Feed feed = parser.parseAndValidate(ProcessEntityParserTest.class
                .getResourceAsStream(FEED_XML));
        feed.getClusters().getClusters().get(0).getValidity().setEnd(null);
        parser.validate(feed);
    }

    @Test (expectedExceptions = ValidationException.class)
    public void testExportFeedSqoopExcludeFields() throws Exception {

        storeEntity(EntityType.CLUSTER, "testCluster");
        InputStream feedStream = this.getClass().getResourceAsStream("/config/feed/feed-export-fields-0.1.xml");
        Feed feed = parser.parseAndValidate(feedStream);
        Assert.fail("An exception should have been thrown: Feed Export policy does not require Fields specification.");
    }

    @Test (expectedExceptions = ValidationException.class)
    public void testExportFeedSqoopArgsNumMapper() throws Exception {
        final InputStream inputStream = this.getClass().getResourceAsStream("/config/feed/feed-export-0.1.xml");
        Feed exportFeed = parser.parse(inputStream);

        org.apache.falcon.entity.v0.feed.Arguments args =
                exportFeed.getClusters().getClusters().get(0).getExport().getArguments();
        Argument numMappersArg = new Argument();
        numMappersArg.setName("--split-by");
        numMappersArg.setValue("id");

        args.getArguments().clear();
        args.getArguments().add(numMappersArg);

        parser.validate(exportFeed);
        Assert.fail("An exception should have been thrown: Feed export should specify --split-by");
    }
}
