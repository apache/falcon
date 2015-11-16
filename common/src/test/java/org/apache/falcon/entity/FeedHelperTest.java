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

package org.apache.falcon.entity;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.parser.EntityParserFactory;
import org.apache.falcon.entity.parser.FeedEntityParser;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Properties;
import org.apache.falcon.entity.v0.cluster.Property;
import org.apache.falcon.entity.v0.feed.Argument;
import org.apache.falcon.entity.v0.feed.Arguments;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.entity.v0.feed.Clusters;
import org.apache.falcon.entity.v0.feed.Extract;
import org.apache.falcon.entity.v0.feed.ExtractMethod;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.FieldIncludeExclude;
import org.apache.falcon.entity.v0.feed.FieldsType;
import org.apache.falcon.entity.v0.feed.Import;
import org.apache.falcon.entity.v0.feed.Lifecycle;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.entity.v0.feed.MergeType;
import org.apache.falcon.entity.v0.feed.RetentionStage;
import org.apache.falcon.entity.v0.feed.Source;
import org.apache.falcon.entity.v0.feed.Validity;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Inputs;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Outputs;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.resource.SchedulableEntityInstance;
import org.apache.falcon.service.LifecyclePolicyMap;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

/**
 * Test for feed helper methods.
 */
public class FeedHelperTest extends AbstractTestBase {
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    private ConfigurationStore store;

    @BeforeClass
    public void init() throws Exception {
        initConfigStore();
        LifecyclePolicyMap.get().init();
    }

    @BeforeMethod
    public void setUp() throws Exception {
        cleanupStore();
        store = getStore();
    }

    @Test
    public void testPartitionExpression() {
        Assert.assertEquals(FeedHelper.normalizePartitionExpression(" /a// ", "  /b// "), "a/b");
        Assert.assertEquals(FeedHelper.normalizePartitionExpression(null, "  /b// "), "b");
        Assert.assertEquals(FeedHelper.normalizePartitionExpression(null, null), "");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInstanceBeforeStart() throws Exception {
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "minutes(5)", "2011-02-28 10:00 UTC", "2016-02-28 10:00 UTC");
        Process process = prepareProcess(cluster, "minutes(10)", "2012-02-28 10:37 UTC", "2012-02-28 10:47 UTC");
        Outputs outputs = new Outputs();
        Output outFeed = new Output();
        outFeed.setName("outputFeed");
        outFeed.setFeed(feed.getName());
        outFeed.setInstance("now(0,0)");
        outputs.getOutputs().add(outFeed);
        process.setOutputs(outputs);
        store.publish(EntityType.PROCESS, process);
        FeedHelper.getProducerInstance(feed, getDate("2011-02-27 10:00 UTC"), cluster);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInstanceEqualsEnd() throws Exception {
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "minutes(5)", "2011-02-28 10:00 UTC", "2016-02-28 10:00 UTC");
        Process process = prepareProcess(cluster, "minutes(10)", "2012-02-28 10:37 UTC", "2012-02-28 10:47 UTC");
        Outputs outputs = new Outputs();
        Output outFeed = new Output();
        outFeed.setName("outputFeed");
        outFeed.setFeed(feed.getName());
        outFeed.setInstance("now(0,0)");
        outputs.getOutputs().add(outFeed);
        process.setOutputs(outputs);
        store.publish(EntityType.PROCESS, process);
        FeedHelper.getProducerInstance(feed, getDate("2016-02-28 10:00 UTC"), cluster);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInstanceOutOfSync() throws Exception {
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "minutes(5)", "2011-02-28 10:00 UTC", "2016-02-28 10:00 UTC");
        Process process = prepareProcess(cluster, "minutes(10)", "2012-02-28 10:37 UTC", "2012-02-28 10:47 UTC");
        Outputs outputs = new Outputs();
        Output outFeed = new Output();
        outFeed.setName("outputFeed");
        outFeed.setFeed(feed.getName());
        outFeed.setInstance("now(0,0)");
        outputs.getOutputs().add(outFeed);
        process.setOutputs(outputs);
        store.publish(EntityType.PROCESS, process);
        FeedHelper.getProducerInstance(feed, getDate("2016-02-28 09:04 UTC"), cluster);
    }

    @Test
    public void testInvalidProducerInstance() throws Exception {
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "minutes(5)", "2011-02-28 10:00 UTC", "2016-02-28 10:00 UTC");
        Process process = prepareProcess(cluster, "minutes(10)", "2012-02-28 10:37 UTC", "2012-02-28 10:47 UTC");
        Outputs outputs = new Outputs();
        Output outFeed = new Output();
        outFeed.setName("outputFeed");
        outFeed.setFeed(feed.getName());
        outFeed.setInstance("now(0,0)");
        outputs.getOutputs().add(outFeed);
        process.setOutputs(outputs);
        store.publish(EntityType.PROCESS, process);
        Assert.assertNull(FeedHelper.getProducerInstance(feed, getDate("2012-02-28 10:40 UTC"), cluster));
    }

    @Test
    public void testGetProducerOutOfValidity() throws FalconException, ParseException {
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "minutes(5)", "2011-02-28 10:00 UTC", "2016-02-28 10:00 UTC");
        Process process = prepareProcess(cluster, "minutes(10)", "2012-02-28 10:37 UTC", "2012-02-28 10:47 UTC");
        Outputs outputs = new Outputs();
        Output outFeed = new Output();
        outFeed.setName("outputFeed");
        outFeed.setFeed(feed.getName());
        outFeed.setInstance("now(0,0)");
        outputs.getOutputs().add(outFeed);
        process.setOutputs(outputs);
        store.publish(EntityType.PROCESS, process);
        Assert.assertEquals(FeedHelper.getProducerProcess(feed).getName(), process.getName());
        SchedulableEntityInstance result = FeedHelper.getProducerInstance(feed, getDate("2012-02-28 10:45 UTC"),
                cluster);
        Assert.assertNull(result);
    }

    @Test
    public void testGetConsumersOutOfValidity() throws Exception {
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "minutes(5)", "2011-02-28 10:00 UTC", "2016-02-28 10:00 UTC");
        Process process = prepareProcess(cluster, "minutes(10)", "2012-02-28 10:37 UTC", "2012-02-28 10:47 UTC");
        Inputs inputs = new Inputs();
        Input inFeed = new Input();
        inFeed.setName("inputFeed");
        inFeed.setFeed(feed.getName());
        inFeed.setStart("now(0, -20)");
        inFeed.setEnd("now(0, 0)");
        inputs.getInputs().add(inFeed);
        process.setInputs(inputs);
        store.publish(EntityType.PROCESS, process);
        Set<SchedulableEntityInstance> result = FeedHelper.getConsumerInstances(feed, getDate("2016-02-28 09:00 UTC"),
                cluster);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testGetConsumersFirstInstance() throws Exception {
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "minutes(5)", "2011-02-28 10:00 UTC", "2016-02-28 10:00 UTC");
        Process process = prepareProcess(cluster, "minutes(10)", "2012-02-28 10:37 UTC", "2012-02-28 10:47 UTC");
        Inputs inputs = new Inputs();
        Input inFeed = new Input();
        inFeed.setName("inputFeed");
        inFeed.setFeed(feed.getName());
        inFeed.setStart("now(0, -20)");
        inFeed.setEnd("now(0, 0)");
        inputs.getInputs().add(inFeed);
        process.setInputs(inputs);
        store.publish(EntityType.PROCESS, process);
        Set<SchedulableEntityInstance> result = FeedHelper.getConsumerInstances(feed, getDate("2012-02-28 10:15 UTC"),
                cluster);
        Set<SchedulableEntityInstance> expected = new HashSet<>();
        SchedulableEntityInstance consumer = new SchedulableEntityInstance(process.getName(), cluster.getName(),
                getDate("2012-02-28 10:37 UTC"), EntityType.PROCESS);
        consumer.setTags(SchedulableEntityInstance.INPUT);
        expected.add(consumer);
        Assert.assertEquals(result, expected);
    }

    @Test
    public void testGetConsumersLastInstance() throws Exception {
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "minutes(5)", "2011-02-28 10:00 UTC", "2016-02-28 10:00 UTC");
        Process process = prepareProcess(cluster, "minutes(10)", "2012-02-28 10:20 UTC", "2016-02-28 10:00 UTC");
        Inputs inputs = new Inputs();
        Input inFeed = new Input();
        inFeed.setName("inputFeed");
        inFeed.setFeed(feed.getName());
        inFeed.setStart("now(0, -20)");
        inFeed.setEnd("now(0, 0)");
        inputs.getInputs().add(inFeed);
        process.setInputs(inputs);
        store.publish(EntityType.PROCESS, process);
        Set<SchedulableEntityInstance> result = FeedHelper.getConsumerInstances(feed, getDate("2012-02-28 10:15 UTC"),
                cluster);
        Set<SchedulableEntityInstance> expected = new HashSet<>();
        String[] consumers = { "2012-02-28 10:20 UTC", "2012-02-28 10:30 UTC", };
        for (String d : consumers) {
            SchedulableEntityInstance i = new SchedulableEntityInstance(process.getName(), cluster.getName(),
                    getDate(d), EntityType.PROCESS);
            i.setTags(SchedulableEntityInstance.INPUT);
            expected.add(i);
        }
        Assert.assertEquals(result, expected);
    }

    @Test
    public void testGetPolicies() throws Exception {
        FeedEntityParser parser = (FeedEntityParser) EntityParserFactory
                .getParser(EntityType.FEED);
        Feed feed = parser.parse(this.getClass().getResourceAsStream(FEED3_XML));
        List<String> policies = FeedHelper.getPolicies(feed, "testCluster");
        Assert.assertEquals(policies.size(), 1);
        Assert.assertEquals(policies.get(0), "AgeBasedDelete");
    }

    @Test
    public void testFeedWithNoDependencies() throws Exception {
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "minutes(5)", "2011-02-28 10:00 UTC", "2016-02-28 10:00 UTC");
        Set<SchedulableEntityInstance> result = FeedHelper.getConsumerInstances(feed, getDate("2016-02-28 09:00 UTC"),
                cluster);
        Assert.assertTrue(result.isEmpty());
        SchedulableEntityInstance res = FeedHelper.getProducerInstance(feed, getDate("2012-02-28 10:45 UTC"),
                cluster);
        Assert.assertNull(res);
    }

    @Test
    public void testEvaluateExpression() throws Exception {
        Cluster cluster = new Cluster();
        cluster.setName("name");
        cluster.setColo("colo");
        cluster.setProperties(new Properties());
        Property prop = new Property();
        prop.setName("pname");
        prop.setValue("pvalue");
        cluster.getProperties().getProperties().add(prop);

        Assert.assertEquals(FeedHelper.evaluateClusterExp(cluster, "${cluster.colo}/*/US"), "colo/*/US");
        Assert.assertEquals(FeedHelper.evaluateClusterExp(cluster, "${cluster.name}/*/${cluster.pname}"),
                "name/*/pvalue");
        Assert.assertEquals(FeedHelper.evaluateClusterExp(cluster, "IN"), "IN");
    }

    @DataProvider(name = "fsPathsforDate")
    public Object[][] createPathsForGetDate() {
        final TimeZone utc = TimeZone.getTimeZone("UTC");
        final TimeZone pacificTime = TimeZone.getTimeZone("America/Los_Angeles");
        final TimeZone ist = TimeZone.getTimeZone("IST");

        return new Object[][] {
            {"/data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}", "/data/2015/01/01/00/30", utc, "2015-01-01T00:30Z"},
            {"/data/${YEAR}-${MONTH}-${DAY}-${HOUR}-${MINUTE}", "/data/2015-01-01-01-00", utc, "2015-01-01T01:00Z"},
            {"/data/${YEAR}/${MONTH}/${DAY}", "/data/2015/01/01", utc, "2015-01-01T00:00Z"},
            {"/data/${YEAR}/${MONTH}/${DAY}/data", "/data/2015/01/01/data", utc, "2015-01-01T00:00Z"},
            {"/data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}", "/data/2015-01-01/00/30", utc, null},
            {"/data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/data", "/data/2015-01-01/00/30", utc, null},
            {"/d/${YEAR}/${MONTH}/${DAY}/${HOUR}/data", "/d/2015/05/25/00/data/{p1}/p2", utc, "2015-05-25T00:00Z"},
            {"/data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/data", "/data/2015/05/25/00/00/{p1}/p2", utc, null},
            {"/d/${YEAR}/${MONTH}/M", "/d/2015/11/M", utc, "2015-11-01T00:00Z"},
            {"/d/${YEAR}/${MONTH}/${DAY}/M", "/d/2015/11/02/M", utc, "2015-11-02T00:00Z"},
            {"/d/${YEAR}/${MONTH}/${DAY}/${HOUR}/M", "/d/2015/11/01/04/M", utc, "2015-11-01T04:00Z"},
            {"/d/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/M", "/d/2015/11/01/04/15/M", utc, "2015-11-01T04:15Z"},
            {"/d/${YEAR}/${MONTH}/M", "/d/2015/11/M", pacificTime, "2015-11-01T07:00Z"},
            {"/d/${YEAR}/${MONTH}/${DAY}/M", "/d/2015/11/02/M", pacificTime, "2015-11-02T08:00Z"},
            {"/d/${YEAR}/${MONTH}/${DAY}/${HOUR}/M", "/d/2015/11/01/04/M", pacificTime, "2015-11-01T12:00Z"},
            {"/d/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/M", "/d/2015/11/01/04/15/M", ist, "2015-10-31T22:45Z"},
        };
    }

    @Test(dataProvider = "fsPathsforDate")
    public void testGetDateFromPath(String template, String path, TimeZone tz, String expectedDate) throws Exception {
        Date date = FeedHelper.getDate(template, new Path(path), tz);
        Assert.assertEquals(SchemaHelper.formatDateUTC(date), expectedDate);
    }

    @Test
    public void testGetLocations() {
        Cluster cluster = new Cluster();
        cluster.setName("name");
        Feed feed = new Feed();
        Location location1 = new Location();
        location1.setType(LocationType.META);
        Locations locations = new Locations();
        locations.getLocations().add(location1);

        Location location2 = new Location();
        location2.setType(LocationType.DATA);
        locations.getLocations().add(location2);

        org.apache.falcon.entity.v0.feed.Cluster feedCluster = new org.apache.falcon.entity.v0.feed.Cluster();
        feedCluster.setName("name");

        feed.setLocations(locations);
        Clusters clusters = new Clusters();
        feed.setClusters(clusters);
        feed.getClusters().getClusters().add(feedCluster);

        Assert.assertEquals(FeedHelper.getLocations(feedCluster, feed),
                locations.getLocations());
        Assert.assertEquals(FeedHelper.getLocation(feed, cluster, LocationType.DATA), location2);
    }

    @Test
    public void testGetProducerProcessWithOffset() throws FalconException, ParseException {
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "minutes(5)", "2011-02-28 10:00 UTC", "2016-02-28 10:00 UTC");
        Assert.assertNull(FeedHelper.getProducerProcess(feed));
        Process process = prepareProcess(cluster, "minutes(10)", "2012-02-28 10:37 UTC", "2016-02-28 10:37 UTC");
        Outputs outputs = new Outputs();
        Output outFeed = new Output();
        outFeed.setName("outputFeed");
        outFeed.setFeed(feed.getName());
        outFeed.setInstance("now(0,0)");
        outputs.getOutputs().add(outFeed);
        process.setOutputs(outputs);
        store.publish(EntityType.PROCESS, process);
        Assert.assertEquals(FeedHelper.getProducerProcess(feed).getName(), process.getName());
        SchedulableEntityInstance result = FeedHelper.getProducerInstance(feed, getDate("2013-02-28 10:35 UTC"),
                cluster);
        SchedulableEntityInstance expected = new SchedulableEntityInstance(process.getName(), cluster.getName(),
                getDate("2013-02-28 10:37 UTC"), EntityType.PROCESS);
        expected.setTags(SchedulableEntityInstance.OUTPUT);
        Assert.assertEquals(result, expected);
    }

    @Test
    public void testGetProducerProcessForNow() throws FalconException, ParseException {
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "days(1)", "2011-02-28 10:00 UTC", "2016-02-28 10:00 UTC");
        Assert.assertNull(FeedHelper.getProducerProcess(feed));

        // create it's producer process submit it, test it's ProducerProcess
        Process process = prepareProcess(cluster, "days(1)", "2012-02-28 10:00 UTC", "2016-02-28 10:00 UTC");
        Outputs outputs = new Outputs();
        Output outFeed = new Output();
        outFeed.setName("outputFeed");
        outFeed.setFeed(feed.getName());
        outFeed.setInstance("now(0,0)");
        outputs.getOutputs().add(outFeed);
        process.setOutputs(outputs);
        store.publish(EntityType.PROCESS, process);

        Assert.assertEquals(FeedHelper.getProducerProcess(feed).getName(), process.getName());
        SchedulableEntityInstance result = FeedHelper.getProducerInstance(feed, getDate("2013-02-28 10:00 UTC"),
                cluster);
        SchedulableEntityInstance expected = new SchedulableEntityInstance(process.getName(), cluster.getName(),
                getDate("2013-02-28 10:00 UTC"), EntityType.PROCESS);
        expected.setTags(SchedulableEntityInstance.OUTPUT);
        Assert.assertEquals(result, expected);
    }

    @Test
    public void testGetProducerWithNowNegativeOffset() throws FalconException, ParseException {
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "days(1)", "2011-02-28 10:00 UTC", "2016-02-28 10:00 UTC");
        Assert.assertNull(FeedHelper.getProducerProcess(feed));

        // create it's producer process submit it, test it's ProducerProcess
        Process process = prepareProcess(cluster, "days(1)", "2012-02-28 10:00 UTC", "2016-02-28 10:00 UTC");
        Outputs outputs = new Outputs();
        Output outFeed = new Output();
        outFeed.setName("outputFeed");
        outFeed.setFeed(feed.getName());
        outFeed.setInstance("now(-4,0)");
        outputs.getOutputs().add(outFeed);
        process.setOutputs(outputs);
        store.publish(EntityType.PROCESS, process);

        Assert.assertEquals(FeedHelper.getProducerProcess(feed).getName(), process.getName());
        SchedulableEntityInstance result = FeedHelper.getProducerInstance(feed, getDate("2013-02-27 10:00 UTC"),
                cluster);
        SchedulableEntityInstance expected = new SchedulableEntityInstance(process.getName(), cluster.getName(),
                getDate("2013-02-28 10:00 UTC"), EntityType.PROCESS);
        expected.setTags(SchedulableEntityInstance.OUTPUT);
        Assert.assertEquals(result, expected);
    }


    @Test
    public void testGetProducerWithNowPositiveOffset() throws FalconException, ParseException {
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "days(1)", "2011-02-28 10:00 UTC", "2016-02-28 10:00 UTC");
        Assert.assertNull(FeedHelper.getProducerProcess(feed));

        // create it's producer process submit it, test it's ProducerProcess
        Process process = prepareProcess(cluster, "days(1)", "2012-02-28 10:00 UTC", "2016-02-28 10:00 UTC");
        Outputs outputs = new Outputs();
        Output outFeed = new Output();
        outFeed.setName("outputFeed");
        outFeed.setFeed(feed.getName());
        outFeed.setInstance("now(4,0)");
        outputs.getOutputs().add(outFeed);
        process.setOutputs(outputs);
        store.publish(EntityType.PROCESS, process);

        Assert.assertEquals(FeedHelper.getProducerProcess(feed).getName(), process.getName());
        SchedulableEntityInstance result = FeedHelper.getProducerInstance(feed, getDate("2013-02-28 10:00 UTC"),
                cluster);
        SchedulableEntityInstance expected = new SchedulableEntityInstance(process.getName(), cluster.getName(),
                getDate("2013-02-28 10:00 UTC"), EntityType.PROCESS);
        expected.setTags(SchedulableEntityInstance.OUTPUT);
        Assert.assertEquals(result, expected);
    }

    @Test
    public void testGetProducerProcessInstance() throws FalconException, ParseException {
        //create a feed, submit it, test that ProducerProcess is null
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "days(1)", "2011-02-28 00:00 UTC", "2016-02-28 10:00 UTC");

        // create it's producer process submit it, test it's ProducerProcess
        Process process = prepareProcess(cluster, "days(1)", "2012-02-28 10:00 UTC", "2016-02-28 10:00 UTC");
        Outputs outputs = new Outputs();
        Output outFeed = new Output();
        outFeed.setName("outputFeed");
        outFeed.setFeed(feed.getName());
        outFeed.setInstance("today(0,0)");
        outputs.getOutputs().add(outFeed);
        process.setOutputs(outputs);
        store.publish(EntityType.PROCESS, process);
        Assert.assertEquals(FeedHelper.getProducerProcess(feed).getName(), process.getName());
        SchedulableEntityInstance result = FeedHelper.getProducerInstance(feed, getDate("2013-02-28 00:00 UTC"),
                cluster);
        SchedulableEntityInstance expected = new SchedulableEntityInstance(process.getName(), cluster.getName(),
                getDate("2013-02-28 10:00 UTC"), EntityType.PROCESS);
        expected.setTags(SchedulableEntityInstance.OUTPUT);
        Assert.assertEquals(result, expected);
    }

    @Test
    public void testGetConsumerProcesses() throws FalconException, ParseException {
        //create a feed, submit it, test that ConsumerProcesses is blank list
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "days(1)", "2012-02-28 10:00 UTC", "2016-02-28 10:00 UTC");

        //create a consumer Process and submit it, assert that this is returned in ConsumerProcesses
        Process process = prepareProcess(cluster, "days(1)", "2012-02-28 10:00 UTC", "2016-02-28 10:00 UTC");
        Inputs inputs = new Inputs();
        Input inFeed = new Input();
        inFeed.setName("outputFeed");
        inFeed.setFeed(feed.getName());
        inFeed.setStart("today(0,0)");
        inFeed.setEnd("today(0,0)");
        inputs.getInputs().add(inFeed);
        process.setInputs(inputs);
        store.publish(EntityType.PROCESS, process);

        Set<Process> result = FeedHelper.getConsumerProcesses(feed);
        Assert.assertEquals(result.size(), 1);
        Assert.assertTrue(result.contains(process));
    }

    @Test
    public void testGetConsumerProcessInstances() throws Exception {
        //create a feed, submit it, test that ConsumerProcesses is blank list
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "hours(1)", "2012-02-28 00:00 UTC", "2016-02-28 00:00 UTC");

        //create a consumer Process and submit it, assert that this is returned in ConsumerProcesses
        Process process = prepareProcess(cluster, "days(1)", "2012-02-28 10:00 UTC", "2016-02-28 10:00 UTC");
        Inputs inputs = new Inputs();
        Input inFeed = new Input();
        inFeed.setName("inputFeed");
        inFeed.setFeed(feed.getName());
        inFeed.setStart("now(-4, 30)");
        inFeed.setEnd("now(4, 30)");
        inputs.getInputs().add(inFeed);
        process.setInputs(inputs);
        store.publish(EntityType.PROCESS, process);

        Set<SchedulableEntityInstance> result = FeedHelper.getConsumerInstances(feed,
                getDate("2012-02-28 09:00 UTC"), cluster);
        Assert.assertEquals(result.size(), 1);

        Set<SchedulableEntityInstance> expected = new HashSet<>();
        SchedulableEntityInstance ins = new SchedulableEntityInstance(process.getName(), cluster.getName(),
                getDate("2012-02-28 10:00 UTC"), EntityType.PROCESS);
        ins.setTags(SchedulableEntityInstance.INPUT);
        expected.add(ins);
        Assert.assertEquals(result, expected);
    }

    @Test
    public void testGetConsumerProcessInstancesWithNonUnitFrequency() throws Exception {
        //create a feed, submit it, test that ConsumerProcesses is blank list
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "minutes(5)", "2012-02-28 00:00 UTC", "2016-02-28 00:00 UTC");

        //create a consumer Process and submit it, assert that this is returned in ConsumerProcesses
        Process process = prepareProcess(cluster, "minutes(10)", "2012-02-28 09:37 UTC", "2016-02-28 10:00 UTC");
        Inputs inputs = new Inputs();
        Input inFeed = new Input();
        inFeed.setName("inputFeed");
        inFeed.setFeed(feed.getName());
        inFeed.setStart("now(0, -20)");
        inFeed.setEnd("now(0,0)");
        inputs.getInputs().add(inFeed);
        process.setInputs(inputs);
        store.publish(EntityType.PROCESS, process);

        Set<SchedulableEntityInstance> result = FeedHelper.getConsumerInstances(feed,
                getDate("2012-02-28 09:40 UTC"), cluster);

        Set<SchedulableEntityInstance> expected = new HashSet<>();
        String[] consumers = {"2012-02-28 09:47 UTC", "2012-02-28 09:57 UTC"};
        for (String d : consumers) {
            SchedulableEntityInstance i = new SchedulableEntityInstance(process.getName(), cluster.getName(),
                    getDate(d), EntityType.PROCESS);
            i.setTags(SchedulableEntityInstance.INPUT);
            expected.add(i);
        }
        Assert.assertEquals(result, expected);
    }

    @Test
    public void testGetConsumersOutOfValidityRange() throws Exception {
        //create a feed, submit it, test that ConsumerProcesses is blank list
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "minutes(5)", "2010-02-28 00:00 UTC", "2016-02-28 00:00 UTC");

        //create a consumer Process and submit it, assert that this is returned in ConsumerProcesses
        Process process = prepareProcess(cluster, "minutes(10)", "2012-02-28 09:37 UTC", "2016-02-28 10:00 UTC");
        Inputs inputs = new Inputs();
        Input inFeed = new Input();
        inFeed.setName("inputFeed");
        inFeed.setFeed(feed.getName());
        inFeed.setStart("now(0, -20)");
        inFeed.setEnd("now(0,0)");
        inputs.getInputs().add(inFeed);
        process.setInputs(inputs);
        store.publish(EntityType.PROCESS, process);

        Set<SchedulableEntityInstance> result = FeedHelper.getConsumerInstances(feed,
                getDate("2010-02-28 09:40 UTC"), cluster);
        Assert.assertEquals(result.size(), 0);
    }

    @Test
    public void testGetConsumersLargeOffsetShortValidity() throws Exception {
        //create a feed, submit it, test that ConsumerProcesses is blank list
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "minutes(5)", "2010-02-28 00:00 UTC", "2016-02-28 00:00 UTC");

        //create a consumer Process and submit it, assert that this is returned in ConsumerProcesses
        Process process = prepareProcess(cluster, "minutes(10)", "2012-02-28 09:37 UTC", "2012-02-28 09:47 UTC");
        Inputs inputs = new Inputs();
        Input inFeed = new Input();
        inFeed.setName("inputFeed");
        inFeed.setFeed(feed.getName());
        inFeed.setStart("today(-2, 0)");
        inFeed.setEnd("now(0,0)");
        inputs.getInputs().add(inFeed);
        process.setInputs(inputs);
        store.publish(EntityType.PROCESS, process);

        Set<SchedulableEntityInstance> result = FeedHelper.getConsumerInstances(feed,
                getDate("2012-02-28 09:35 UTC"), cluster);
        Set<SchedulableEntityInstance> expected = new HashSet<>();
        SchedulableEntityInstance consumer = new SchedulableEntityInstance(process.getName(), cluster.getName(),
                getDate("2012-02-28 09:37 UTC"), EntityType.PROCESS);
        consumer.setTags(SchedulableEntityInstance.INPUT);
        expected.add(consumer);
        Assert.assertEquals(result, expected);
    }

    @Test
    public void testGetMultipleConsumerInstances() throws Exception {
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "hours(1)", "2012-02-27 00:00 UTC", "2016-02-28 00:00 UTC");
        Process process = prepareProcess(cluster, "hours(1)", "2012-02-27 10:00 UTC", "2016-02-28 10:00 UTC");
        Inputs inputs = new Inputs();
        Input inFeed = new Input();
        inFeed.setName("inputFeed");
        inFeed.setFeed(feed.getName());
        inFeed.setStart("now(-4, 30)");
        inFeed.setEnd("now(4, 30)");
        inputs.getInputs().add(inFeed);
        process.setInputs(inputs);
        store.publish(EntityType.PROCESS, process);

        Set<SchedulableEntityInstance> result = FeedHelper.getConsumerInstances(feed,
                getDate("2012-02-28 09:00 UTC"), cluster);
        Assert.assertEquals(result.size(), 9);
        Set<SchedulableEntityInstance> expected = new HashSet<>();
        String[] consumers = { "2012-02-28 05:00 UTC", "2012-02-28 06:00 UTC", "2012-02-28 07:00 UTC",
            "2012-02-28 08:00 UTC", "2012-02-28 09:00 UTC", "2012-02-28 10:00 UTC", "2012-02-28 11:00 UTC",
            "2012-02-28 12:00 UTC", "2012-02-28 13:00 UTC", };
        for (String d : consumers) {
            SchedulableEntityInstance i = new SchedulableEntityInstance(process.getName(), cluster.getName(),
                    getDate(d), EntityType.PROCESS);
            i.setTags(SchedulableEntityInstance.INPUT);
            expected.add(i);
        }
        Assert.assertEquals(result, expected);
    }

    @Test
    public void testGetConsumerWithVariableEnd() throws Exception {
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "hours(1)", "2012-02-27 00:00 UTC", "2016-02-28 00:00 UTC");

        //create a consumer Process and submit it, assert that this is returned in ConsumerProcesses
        Process process = prepareProcess(cluster, "hours(1)", "2012-02-27 10:00 UTC", "2016-02-28 10:00 UTC");
        Inputs inputs = new Inputs();
        Input inFeed = new Input();
        inFeed.setName("inputFeed");
        inFeed.setFeed(feed.getName());
        inFeed.setStart("today(0, 0)");
        inFeed.setEnd("now(0, 0)");
        inputs.getInputs().add(inFeed);
        process.setInputs(inputs);
        store.publish(EntityType.PROCESS, process);
        Set<SchedulableEntityInstance> result = FeedHelper.getConsumerInstances(feed,
                getDate("2012-02-28 00:00 UTC"), cluster);
        Set<SchedulableEntityInstance> expected = new HashSet<>();
        String[] consumers =  {"2012-02-28 11:00 UTC", "2012-02-28 16:00 UTC", "2012-02-28 18:00 UTC",
            "2012-02-28 20:00 UTC", "2012-02-28 13:00 UTC", "2012-02-28 03:00 UTC", "2012-02-28 04:00 UTC",
            "2012-02-28 06:00 UTC", "2012-02-28 05:00 UTC", "2012-02-28 17:00 UTC", "2012-02-28 00:00 UTC",
            "2012-02-28 23:00 UTC", "2012-02-28 21:00 UTC", "2012-02-28 15:00 UTC", "2012-02-28 22:00 UTC",
            "2012-02-28 14:00 UTC", "2012-02-28 08:00 UTC", "2012-02-28 12:00 UTC", "2012-02-28 02:00 UTC",
            "2012-02-28 01:00 UTC", "2012-02-28 19:00 UTC", "2012-02-28 10:00 UTC", "2012-02-28 09:00 UTC",
            "2012-02-28 07:00 UTC", };
        for (String d : consumers) {
            SchedulableEntityInstance i = new SchedulableEntityInstance(process.getName(), cluster.getName(),
                    getDate(d), EntityType.PROCESS);
            i.setTags(SchedulableEntityInstance.INPUT);
            expected.add(i);
        }
        Assert.assertEquals(result, expected);
    }

    @Test
    public void testGetConsumerWithVariableStart() throws Exception {
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "hours(1)", "2012-02-27 00:00 UTC", "2016-02-28 00:00 UTC");

        //create a consumer Process and submit it, assert that this is returned in ConsumerProcesses
        Process process = prepareProcess(cluster, "hours(1)", "2012-02-27 10:00 UTC", "2016-02-28 10:00 UTC");
        Inputs inputs = new Inputs();
        Input inFeed = new Input();
        inFeed.setName("inputFeed");
        inFeed.setFeed(feed.getName());
        inFeed.setStart("now(0, 0)");
        inFeed.setEnd("today(24, 0)");
        inputs.getInputs().add(inFeed);
        process.setInputs(inputs);
        store.publish(EntityType.PROCESS, process);

        Set<SchedulableEntityInstance> result = FeedHelper.getConsumerInstances(feed,
                getDate("2012-03-28 00:00 UTC"), cluster);
        Set<SchedulableEntityInstance> expected = new HashSet<>();
        String[] consumers =  {"2012-03-27 16:00 UTC", "2012-03-27 01:00 UTC", "2012-03-27 10:00 UTC",
            "2012-03-27 03:00 UTC", "2012-03-27 08:00 UTC", "2012-03-27 07:00 UTC", "2012-03-27 19:00 UTC",
            "2012-03-27 22:00 UTC", "2012-03-27 12:00 UTC", "2012-03-27 20:00 UTC", "2012-03-27 09:00 UTC",
            "2012-03-27 04:00 UTC", "2012-03-27 14:00 UTC", "2012-03-27 05:00 UTC", "2012-03-27 23:00 UTC",
            "2012-03-27 17:00 UTC", "2012-03-27 13:00 UTC", "2012-03-27 18:00 UTC", "2012-03-27 15:00 UTC",
            "2012-03-28 00:00 UTC", "2012-03-27 02:00 UTC", "2012-03-27 11:00 UTC", "2012-03-27 21:00 UTC",
            "2012-03-27 00:00 UTC", "2012-03-27 06:00 UTC", };
        for (String d : consumers) {
            SchedulableEntityInstance i = new SchedulableEntityInstance(process.getName(), cluster.getName(),
                    getDate(d), EntityType.PROCESS);
            i.setTags(SchedulableEntityInstance.INPUT);
            expected.add(i);
        }
        Assert.assertEquals(result, expected);
    }

    @Test
    public void testGetConsumerWithLatest() throws Exception {
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "hours(1)", "2012-02-27 00:00 UTC", "2016-02-28 00:00 UTC");
        Process process = prepareProcess(cluster, "hours(1)", "2012-02-27 10:00 UTC", "2016-02-28 10:00 UTC");
        Inputs inputs = new Inputs();
        Input inFeed = new Input();
        inFeed.setName("inputFeed");
        inFeed.setFeed(feed.getName());
        inFeed.setStart("today(0, 0)");
        inFeed.setEnd("latest(0)");
        inputs.getInputs().add(inFeed);
        process.setInputs(inputs);
        store.publish(EntityType.PROCESS, process);

        Set<SchedulableEntityInstance> result = FeedHelper.getConsumerInstances(feed,
                getDate("2012-02-28 00:00 UTC"), cluster);
        Set<SchedulableEntityInstance> expected = new HashSet<>();
        String[] consumers =  {"2012-02-28 23:00 UTC", "2012-02-28 04:00 UTC", "2012-02-28 10:00 UTC",
            "2012-02-28 07:00 UTC", "2012-02-28 17:00 UTC", "2012-02-28 13:00 UTC", "2012-02-28 05:00 UTC",
            "2012-02-28 22:00 UTC", "2012-02-28 03:00 UTC", "2012-02-28 21:00 UTC", "2012-02-28 11:00 UTC",
            "2012-02-28 20:00 UTC", "2012-02-28 06:00 UTC", "2012-02-28 01:00 UTC", "2012-02-28 14:00 UTC",
            "2012-02-28 00:00 UTC", "2012-02-28 18:00 UTC", "2012-02-28 12:00 UTC", "2012-02-28 16:00 UTC",
            "2012-02-28 09:00 UTC", "2012-02-28 15:00 UTC", "2012-02-28 19:00 UTC", "2012-02-28 08:00 UTC",
            "2012-02-28 02:00 UTC", };
        for (String d : consumers) {
            SchedulableEntityInstance i = new SchedulableEntityInstance(process.getName(), cluster.getName(),
                    getDate(d), EntityType.PROCESS);
            i.setTags(SchedulableEntityInstance.INPUT);
            expected.add(i);
        }
        Assert.assertEquals(result, expected);
    }

    public void testIsLifeCycleEnabled() throws Exception {
        Feed feed = new Feed();

        // lifecycle is not defined
        Clusters clusters = new Clusters();
        org.apache.falcon.entity.v0.feed.Cluster cluster = new org.apache.falcon.entity.v0.feed.Cluster();
        cluster.setName("cluster1");
        clusters.getClusters().add(cluster);
        feed.setClusters(clusters);
        Assert.assertFalse(FeedHelper.isLifecycleEnabled(feed, cluster.getName()));

        // lifecycle is defined at global level
        Lifecycle globalLifecycle = new Lifecycle();
        RetentionStage retentionStage = new RetentionStage();
        retentionStage.setFrequency(new Frequency("hours(2)"));
        globalLifecycle.setRetentionStage(retentionStage);
        feed.setLifecycle(globalLifecycle);
        Assert.assertTrue(FeedHelper.isLifecycleEnabled(feed, cluster.getName()));

        // lifecycle is defined at both global and cluster level
        Lifecycle clusterLifecycle = new Lifecycle();
        retentionStage = new RetentionStage();
        retentionStage.setFrequency(new Frequency("hours(4)"));
        clusterLifecycle.setRetentionStage(retentionStage);
        feed.getClusters().getClusters().get(0).setLifecycle(clusterLifecycle);
        Assert.assertTrue(FeedHelper.isLifecycleEnabled(feed, cluster.getName()));

        // lifecycle is defined only at cluster level
        feed.setLifecycle(null);
        Assert.assertTrue(FeedHelper.isLifecycleEnabled(feed, cluster.getName()));
    }

    @Test
    public void testGetRetentionStage() throws Exception {
        Feed feed = new Feed();
        feed.setFrequency(new Frequency("days(1)"));

        // lifecycle is not defined
        Clusters clusters = new Clusters();
        org.apache.falcon.entity.v0.feed.Cluster cluster = new org.apache.falcon.entity.v0.feed.Cluster();
        cluster.setName("cluster1");
        clusters.getClusters().add(cluster);
        feed.setClusters(clusters);
        Assert.assertNull(FeedHelper.getRetentionStage(feed, cluster.getName()));
        Assert.assertEquals(FeedHelper.getRetentionFrequency(feed, cluster.getName()), new Frequency("days(1)"));

        // lifecycle is defined at global level
        Lifecycle globalLifecycle = new Lifecycle();
        RetentionStage globalRetentionStage = new RetentionStage();
        globalRetentionStage.setFrequency(new Frequency("hours(2)"));
        globalLifecycle.setRetentionStage(globalRetentionStage);
        feed.setLifecycle(globalLifecycle);
        Assert.assertNotNull(FeedHelper.getRetentionStage(feed, cluster.getName()));
        Assert.assertEquals(FeedHelper.getRetentionFrequency(feed, cluster.getName()),
                feed.getLifecycle().getRetentionStage().getFrequency());

        // lifecycle is defined at both global and cluster level
        Lifecycle clusterLifecycle = new Lifecycle();
        RetentionStage clusterRetentionStage = new RetentionStage();
        clusterRetentionStage.setFrequency(new Frequency("hours(4)"));
        clusterLifecycle.setRetentionStage(clusterRetentionStage);
        feed.getClusters().getClusters().get(0).setLifecycle(clusterLifecycle);
        Assert.assertNotNull(FeedHelper.getRetentionStage(feed, cluster.getName()));
        Assert.assertEquals(FeedHelper.getRetentionFrequency(feed, cluster.getName()),
                cluster.getLifecycle().getRetentionStage().getFrequency());

        // lifecycle at both level - retention only at cluster level.
        feed.getLifecycle().setRetentionStage(null);
        Assert.assertNotNull(FeedHelper.getRetentionStage(feed, cluster.getName()));
        Assert.assertEquals(FeedHelper.getRetentionFrequency(feed, cluster.getName()),
                cluster.getLifecycle().getRetentionStage().getFrequency());

        // lifecycle at both level - retention only at global level.
        feed.getLifecycle().setRetentionStage(globalRetentionStage);
        feed.getClusters().getClusters().get(0).getLifecycle().setRetentionStage(null);
        Assert.assertNotNull(FeedHelper.getRetentionStage(feed, cluster.getName()));
        Assert.assertEquals(FeedHelper.getRetentionFrequency(feed, cluster.getName()),
                feed.getLifecycle().getRetentionStage().getFrequency());

        // lifecycle is defined only at cluster level
        feed.setLifecycle(null);
        feed.getClusters().getClusters().get(0).getLifecycle().setRetentionStage(clusterRetentionStage);
        Assert.assertNotNull(FeedHelper.getRetentionStage(feed, cluster.getName()));
        Assert.assertEquals(FeedHelper.getRetentionFrequency(feed, cluster.getName()),
                cluster.getLifecycle().getRetentionStage().getFrequency());
    }

    @Test
    public void testGetRetentionFrequency() throws Exception {
        Feed feed = new Feed();
        feed.setFrequency(new Frequency("days(10)"));

        // no lifecycle defined - test both daily and monthly feeds
        Clusters clusters = new Clusters();
        org.apache.falcon.entity.v0.feed.Cluster cluster = new org.apache.falcon.entity.v0.feed.Cluster();
        cluster.setName("cluster1");
        clusters.getClusters().add(cluster);
        feed.setClusters(clusters);
        Assert.assertEquals(FeedHelper.getRetentionFrequency(feed, cluster.getName()), new Frequency("days(10)"));

        feed.setFrequency(new Frequency("hours(1)"));
        Assert.assertEquals(FeedHelper.getRetentionFrequency(feed, cluster.getName()), new Frequency("hours(6)"));

        feed.setFrequency(new Frequency("minutes(10)"));
        Assert.assertEquals(FeedHelper.getRetentionFrequency(feed, cluster.getName()), new Frequency("hours(6)"));

        feed.setFrequency(new Frequency("hours(7)"));
        Assert.assertEquals(FeedHelper.getRetentionFrequency(feed, cluster.getName()), new Frequency("hours(7)"));

        feed.setFrequency(new Frequency("days(2)"));
        Assert.assertEquals(FeedHelper.getRetentionFrequency(feed, cluster.getName()), new Frequency("days(2)"));

        // lifecycle at both level - retention only at global level.
        feed.setFrequency(new Frequency("hours(1)"));
        Lifecycle globalLifecycle = new Lifecycle();
        RetentionStage globalRetentionStage = new RetentionStage();
        globalRetentionStage.setFrequency(new Frequency("hours(2)"));
        globalLifecycle.setRetentionStage(globalRetentionStage);
        feed.setLifecycle(globalLifecycle);

        Lifecycle clusterLifecycle = new Lifecycle();
        RetentionStage clusterRetentionStage = new RetentionStage();
        clusterLifecycle.setRetentionStage(clusterRetentionStage);
        feed.getClusters().getClusters().get(0).setLifecycle(clusterLifecycle);
        Assert.assertEquals(FeedHelper.getRetentionFrequency(feed, cluster.getName()), new Frequency("hours(6)"));

        // lifecycle at both level - retention only at cluster level.
        feed.getLifecycle().getRetentionStage().setFrequency(null);
        clusterRetentionStage.setFrequency(new Frequency("hours(4)"));
        Assert.assertEquals(FeedHelper.getRetentionFrequency(feed, cluster.getName()), new Frequency("hours(4)"));
    }

    @Test
    public void testFeedImportSnapshot() throws Exception {
        Cluster cluster = publishCluster();
        Feed feed = importFeedSnapshot(cluster, "hours(1)", "2012-02-07 00:00 UTC", "2020-02-25 00:00 UTC");
        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(feed, cluster.getName());
        Date startInstResult = FeedHelper.getImportInitalInstance(feedCluster);
        Assert.assertNotNull(feed.getClusters().getClusters());
        Assert.assertNotNull(feed.getClusters().getClusters().get(0));
        Assert.assertNotNull(feed.getClusters().getClusters().get(0).getValidity());
        Assert.assertNotNull(feed.getClusters().getClusters().get(0).getValidity().getStart());
        Assert.assertNotNull(startInstResult);
        Assert.assertNotNull(feedCluster.getValidity().getStart());
        Assert.assertEquals(getDate("2012-02-07 00:00 UTC"), feedCluster.getValidity().getStart());
        Assert.assertTrue(FeedHelper.isImportEnabled(feedCluster));
        Assert.assertEquals(MergeType.SNAPSHOT, FeedHelper.getImportMergeType(feedCluster));
        Assert.assertNotEquals(startInstResult, feedCluster.getValidity().getStart());
    }

    @Test
    public void testFeedImportFields() throws Exception {
        Cluster cluster = publishCluster();
        Feed feed = importFeedSnapshot(cluster, "hours(1)", "2012-02-07 00:00 UTC", "2020-02-25 00:00 UTC");
        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(feed, cluster.getName());
        Date startInstResult = FeedHelper.getImportInitalInstance(feedCluster);
        List<String> fieldList = FeedHelper.getFieldList(feedCluster);
        Assert.assertEquals(2, fieldList.size());
        Assert.assertFalse(FeedHelper.isFieldExcludes(feedCluster));
    }

    @Test
    public void testFeedImportAppend() throws Exception {
        Cluster cluster = publishCluster();
        Feed feed = importFeedAppend(cluster, "hours(1)", "2012-02-07 00:00 UTC", "2020-02-25 00:00 UTC");
        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(feed, cluster.getName());
        Date startInstResult = FeedHelper.getImportInitalInstance(feedCluster);
        Assert.assertEquals(startInstResult, feed.getClusters().getClusters().get(0).getValidity().getStart());
    }

    private Validity getFeedValidity(String start, String end) throws ParseException {
        Validity validity = new Validity();
        validity.setStart(getDate(start));
        validity.setEnd(getDate(end));
        return validity;
    }

    private org.apache.falcon.entity.v0.process.Validity getProcessValidity(String start, String end) throws
            ParseException {

        org.apache.falcon.entity.v0.process.Validity validity = new org.apache.falcon.entity.v0.process.Validity();
        validity.setStart(getDate(start));
        validity.setEnd(getDate(end));
        return validity;
    }

    private Date getDate(String dateString) throws ParseException {
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm Z");
        return format.parse(dateString);
    }

    private Cluster publishCluster() throws FalconException {
        Cluster cluster = new Cluster();
        cluster.setName("feedCluster");
        cluster.setColo("colo");
        store.publish(EntityType.CLUSTER, cluster);
        return cluster;

    }

    private Feed publishFeed(Cluster cluster, String frequency, String start, String end)
        throws FalconException, ParseException {
        return publishFeed(cluster, frequency, start, end, null);
    }

    private Feed publishFeed(Cluster cluster, String frequency, String start, String end, Import imp)
        throws FalconException, ParseException {

        Feed feed = new Feed();
        feed.setName("feed");
        Frequency f = new Frequency(frequency);
        feed.setFrequency(f);
        feed.setTimezone(UTC);
        Clusters fClusters = new Clusters();
        org.apache.falcon.entity.v0.feed.Cluster fCluster = new org.apache.falcon.entity.v0.feed.Cluster();
        fCluster.setType(ClusterType.SOURCE);
        fCluster.setImport(imp);
        fCluster.setName(cluster.getName());
        fCluster.setValidity(getFeedValidity(start, end));
        fClusters.getClusters().add(fCluster);
        feed.setClusters(fClusters);
        store.publish(EntityType.FEED, feed);

        return feed;
    }

    private Process prepareProcess(Cluster cluster, String frequency, String start, String end) throws ParseException {
        Process process = new Process();
        process.setName("process");
        process.setTimezone(UTC);
        org.apache.falcon.entity.v0.process.Clusters pClusters = new org.apache.falcon.entity.v0.process.Clusters();
        org.apache.falcon.entity.v0.process.Cluster pCluster = new org.apache.falcon.entity.v0.process.Cluster();
        pCluster.setName(cluster.getName());
        org.apache.falcon.entity.v0.process.Validity validity = getProcessValidity(start, end);
        pCluster.setValidity(validity);
        pClusters.getClusters().add(pCluster);
        process.setClusters(pClusters);
        Frequency f = new Frequency(frequency);
        process.setFrequency(f);
        return process;
    }

    private Feed importFeedSnapshot(Cluster cluster, String frequency, String start, String end)
        throws FalconException, ParseException {

        Import imp = getAnImport(MergeType.SNAPSHOT);
        Feed feed = publishFeed(cluster, frequency, start, end, imp);
        return feed;
    }

    private Feed importFeedAppend(Cluster cluster, String frequency, String start, String end)
        throws FalconException, ParseException {

        Import imp = getAnImport(MergeType.APPEND);
        Feed feed = publishFeed(cluster, frequency, start, end);
        return feed;
    }

    private Import getAnImport(MergeType mergeType) {
        Extract extract = new Extract();
        extract.setType(ExtractMethod.FULL);
        extract.setMergepolicy(mergeType);

        FieldIncludeExclude fieldInclude = new FieldIncludeExclude();
        fieldInclude.getFields().add("id");
        fieldInclude.getFields().add("name");
        FieldsType fields = new FieldsType();
        fields.setIncludes(fieldInclude);

        Source source = new Source();
        source.setName("test-db");
        source.setTableName("test-table");
        source.setExtract(extract);
        source.setFields(fields);

        Argument a1 = new Argument();
        a1.setName("--split_by");
        a1.setValue("id");
        Argument a2 = new Argument();
        a2.setName("--num-mappers");
        a2.setValue("2");
        Arguments args = new Arguments();
        List<Argument> argList = args.getArguments();
        argList.add(a1);
        argList.add(a2);

        Import imp = new Import();
        imp.setSource(source);
        imp.setArguments(args);
        return imp;
    }
}
