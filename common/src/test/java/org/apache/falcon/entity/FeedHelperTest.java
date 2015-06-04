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
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Properties;
import org.apache.falcon.entity.v0.cluster.Property;
import org.apache.falcon.entity.v0.feed.Clusters;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.entity.v0.feed.Validity;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Inputs;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Outputs;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.resource.SchedulableEntityInstance;
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
        return new Object[][] {
            {"/data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}", "/data/2015/01/01/00/30", "2015-01-01T00:30Z"},
            {"/data/${YEAR}-${MONTH}-${DAY}-${HOUR}-${MINUTE}", "/data/2015-01-01-01-00", "2015-01-01T01:00Z"},
            {"/data/${YEAR}/${MONTH}/${DAY}", "/data/2015/01/01", "2015-01-01T00:00Z"},
            {"/data/${YEAR}/${MONTH}/${DAY}/data", "/data/2015/01/01/data", "2015-01-01T00:00Z"},
            {"/data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}", "/data/2015-01-01/00/30", null},
            {"/data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/data", "/data/2015-01-01/00/30", null},
            {"/data/${YEAR}/${MONTH}/${DAY}/${HOUR}/data", "/data/2015/05/25/00/data/{p1}/p2", "2015-05-25T00:00Z"},
            {"/data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/data", "/data/2015/05/25/00/00/{p1}/p2", null},
        };
    }

    @Test(dataProvider = "fsPathsforDate")
    public void testGetDateFromPath(String template, String path, String expectedDate) throws Exception {
        Date date = FeedHelper.getDate(template, new Path(path), UTC);
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
        //create a feed, submit it, test that ProducerProcess is null

        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "days(1)", "2011-02-28 10:00 UTC", "2016-02-28 10:00 UTC");
        Assert.assertNull(FeedHelper.getProducerProcess(feed));

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
        SchedulableEntityInstance result = FeedHelper.getProducerInstance(feed, getDate("2013-02-27 10:00 UTC"),
                cluster);
        SchedulableEntityInstance expected = new SchedulableEntityInstance(process.getName(), cluster.getName(),
                getDate("2013-02-28 10:00 UTC"), EntityType.PROCESS);
        expected.setTag(SchedulableEntityInstance.OUTPUT);
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
        expected.setTag(SchedulableEntityInstance.OUTPUT);
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
        expected.setTag(SchedulableEntityInstance.OUTPUT);
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
        expected.setTag(SchedulableEntityInstance.OUTPUT);
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
        expected.setTag(SchedulableEntityInstance.OUTPUT);
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
        ins.setTag(SchedulableEntityInstance.INPUT);
        expected.add(ins);
        Assert.assertEquals(result, expected);

    }

    @Test
    public void testGetMultipleConsumerInstances() throws Exception {
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "hours(1)", "2012-02-28 00:00 UTC", "2016-02-28 00:00 UTC");

        //create a consumer Process and submit it, assert that this is returned in ConsumerProcesses
        Process process = prepareProcess(cluster, "hours(1)", "2012-02-28 10:00 UTC", "2016-02-28 10:00 UTC");
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
        Assert.assertEquals(result.size(), 8);

        Set<SchedulableEntityInstance> expected = new HashSet<>();
        String[] consumers = { "2012-02-28 05:00 UTC", "2012-02-28 06:00 UTC", "2012-02-28 07:00 UTC",
            "2012-02-28 08:00 UTC", "2012-02-28 09:00 UTC", "2012-02-28 10:00 UTC", "2012-02-28 11:00 UTC",
            "2012-02-28 12:00 UTC", };
        for (String d : consumers) {
            SchedulableEntityInstance i = new SchedulableEntityInstance(process.getName(), cluster.getName(),
                    getDate(d), EntityType.PROCESS);
            i.setTag(SchedulableEntityInstance.INPUT);
            expected.add(i);
        }
        Assert.assertEquals(result, expected);
    }

    @Test
    public void testGetConsumerWithNow() throws Exception {
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "hours(1)", "2012-02-28 00:00 UTC", "2016-02-28 00:00 UTC");

        //create a consumer Process and submit it, assert that this is returned in ConsumerProcesses
        Process process = prepareProcess(cluster, "hours(1)", "2012-02-28 10:00 UTC", "2016-02-28 10:00 UTC");
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
        Assert.assertEquals(result.size(), 23);
    }

    @Test
    public void testGetConsumerWithLatest() throws Exception {
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "hours(1)", "2012-02-28 00:00 UTC", "2016-02-28 00:00 UTC");

        //create a consumer Process and submit it, assert that this is returned in ConsumerProcesses
        Process process = prepareProcess(cluster, "hours(1)", "2012-02-28 10:00 UTC", "2016-02-28 10:00 UTC");
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
        System.out.println("result.size() = " + result.size());
        Assert.assertEquals(result.size(), 23);
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

        Feed feed = new Feed();
        feed.setName("feed");
        Frequency f = new Frequency(frequency);
        feed.setFrequency(f);
        feed.setTimezone(UTC);
        Clusters fClusters = new Clusters();
        org.apache.falcon.entity.v0.feed.Cluster fCluster = new org.apache.falcon.entity.v0.feed.Cluster();
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
}
