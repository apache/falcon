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
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Clusters;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Inputs;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Outputs;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.resource.SchedulableEntityInstance;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;


/**
 * Tests for ProcessHelper methods.
 */
public class ProcessHelperTest extends AbstractTestBase {
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    private ConfigurationStore store;

    @BeforeClass
    public void init() throws Exception {
        initConfigStore();
    }

    @BeforeMethod
    public void setUp() throws Exception {
        cleanupStore();
        store = ConfigurationStore.get();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testBeforeStartInstance() throws FalconException, ParseException {
        // create a process with input feeds
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "minutes(5)", "2012-02-27 10:00 UTC", "2016-02-28 10:00 UTC");

        // find the input Feed instances time
        Process process = prepareProcess(cluster, "minutes(10)", "2012-02-28 10:37 UTC", "2012-02-28 10:47 UTC");
        Inputs inputs = new Inputs();
        Input input = getInput("inputFeed", feed.getName(), "now(0,-20)", "now(0,0)", false);
        inputs.getInputs().add(input);
        process.setInputs(inputs);
        store.publish(EntityType.PROCESS, process);

        Date processInstanceDate = getDate("2012-02-28 10:27 UTC");
        ProcessHelper.getInputFeedInstances(process, processInstanceDate, cluster, false);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testEqualsToEndInstance() throws FalconException, ParseException {
        // create a process with input feeds
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "minutes(5)", "2012-02-27 10:00 UTC", "2016-02-28 10:00 UTC");

        // find the input Feed instances time
        Process process = prepareProcess(cluster, "minutes(10)", "2012-02-28 10:37 UTC", "2012-02-28 10:47 UTC");
        Inputs inputs = new Inputs();
        Input input = getInput("inputFeed", feed.getName(), "now(0,-20)", "now(0,0)", false);
        inputs.getInputs().add(input);
        process.setInputs(inputs);
        store.publish(EntityType.PROCESS, process);

        Date processInstanceDate = getDate("2012-02-28 10:47 UTC");
        ProcessHelper.getInputFeedInstances(process, processInstanceDate, cluster, false);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testOutOfSyncInstance() throws FalconException, ParseException {
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "minutes(5)", "2012-02-27 10:00 UTC", "2016-02-28 10:00 UTC");
        Process process = prepareProcess(cluster, "minutes(10)", "2012-02-28 10:37 UTC", "2012-02-28 10:47 UTC");
        Inputs inputs = new Inputs();
        Input input = getInput("inputFeed", feed.getName(), "now(0,-20)", "now(0,0)", false);
        inputs.getInputs().add(input);
        process.setInputs(inputs);
        store.publish(EntityType.PROCESS, process);
        Date processInstanceDate = getDate("2012-02-28 10:40 UTC");
        ProcessHelper.getInputFeedInstances(process, processInstanceDate, cluster, false);
    }

    @Test
    public void testProcessWithNoDependencies() throws Exception {
        Cluster cluster = publishCluster();
        Process process = prepareProcess(cluster, "minutes(10)", "2012-02-28 10:37 UTC", "2012-02-28 10:47 UTC");
        store.publish(EntityType.PROCESS, process);
        Date processInstanceDate = getDate("2012-02-28 10:37 UTC");
        Set<SchedulableEntityInstance> inputFeedInstances = ProcessHelper.getInputFeedInstances(process,
            processInstanceDate, cluster, false);
        Assert.assertTrue(inputFeedInstances.isEmpty());
        Set<SchedulableEntityInstance> res = ProcessHelper.getOutputFeedInstances(process, processInstanceDate,
            cluster);
        Assert.assertTrue(res.isEmpty());
    }

    @Test
    public void testGetInputFeedInstances() throws FalconException, ParseException {
        // create a process with input feeds
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "minutes(5)", "2012-02-27 10:00 UTC", "2016-02-28 10:00 UTC");

        // find the input Feed instances time
        Process process = prepareProcess(cluster, "minutes(10)", "2012-02-28 10:37 UTC", "2016-02-28 10:37 UTC");
        Inputs inputs = new Inputs();
        Input input = getInput("inputFeed", feed.getName(), "now(0,-20)", "now(0,0)", false);
        inputs.getInputs().add(input);
        process.setInputs(inputs);
        store.publish(EntityType.PROCESS, process);

        Date processInstanceDate = getDate("2012-02-28 10:37 UTC");
        Set<SchedulableEntityInstance> inputFeedInstances = ProcessHelper.getInputFeedInstances(process,
                processInstanceDate, cluster, false);
        Assert.assertEquals(inputFeedInstances.size(), 5);

        Set<SchedulableEntityInstance> expectedInputFeedInstances = new HashSet<>();
        String[] inputInstances = { "2012-02-28 10:15 UTC", "2012-02-28 10:20 UTC", "2012-02-28 10:25 UTC",
            "2012-02-28 10:30 UTC", "2012-02-28 10:35 UTC", };
        for (String d : inputInstances) {
            SchedulableEntityInstance i = new SchedulableEntityInstance(feed.getName(), cluster.getName(),
                    getDate(d), EntityType.FEED);
            i.setTags(SchedulableEntityInstance.INPUT);
            expectedInputFeedInstances.add(i);
        }
        Assert.assertTrue(inputFeedInstances.equals(expectedInputFeedInstances));
    }

    @Test
    public void testGetOutputFeedInstances() throws FalconException, ParseException {
        // create a process with input feeds
        Cluster cluster = publishCluster();
        Feed feed = publishFeed(cluster, "days(1)", "2012-02-27 11:00 UTC", "2016-02-28 11:00 UTC");
        Process process = prepareProcess(cluster, "days(1)", "2012-02-28 10:00 UTC", "2016-02-28 10:00 UTC");
        Outputs outputs = new Outputs();
        outputs.getOutputs().add(getOutput("outputFeed", feed.getName(), "now(0,0)"));
        process.setOutputs(outputs);
        store.publish(EntityType.PROCESS, process);

        Set<SchedulableEntityInstance> result = ProcessHelper.getOutputFeedInstances(process,
                getDate("2012-02-28 10:00 UTC"), cluster);

        Set<SchedulableEntityInstance> expected = new HashSet<>();
        SchedulableEntityInstance ins = new SchedulableEntityInstance(feed.getName(), cluster.getName(),
                getDate("2012-02-27 11:00 UTC"), EntityType.FEED);
        ins.setTags(SchedulableEntityInstance.OUTPUT);
        expected.add(ins);

        Assert.assertEquals(result, expected);

    }

    private org.apache.falcon.entity.v0.process.Validity getProcessValidity(String start, String end) throws
            ParseException {

        org.apache.falcon.entity.v0.process.Validity validity = new org.apache.falcon.entity.v0.process.Validity();
        validity.setStart(getDate(start));
        validity.setEnd(getDate(end));
        return validity;
    }

    private Date getDate(String dateString) throws ParseException {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm Z").parse(dateString);
    }

    private org.apache.falcon.entity.v0.feed.Validity getFeedValidity(String start, String end) throws ParseException {
        org.apache.falcon.entity.v0.feed.Validity validity = new org.apache.falcon.entity.v0.feed.Validity();
        validity.setStart(getDate(start));
        validity.setEnd(getDate(end));
        return validity;
    }

    private Input getInput(String name, String feedName, String start, String end, boolean isOptional) {
        Input inFeed = new Input();
        inFeed.setName(name);
        inFeed.setFeed(feedName);
        inFeed.setStart(start);
        inFeed.setEnd(end);
        inFeed.setOptional(isOptional);
        return inFeed;
    }

    private Output getOutput(String name, String feedName, String instance) {
        Output output = new Output();
        output.setInstance(instance);
        output.setFeed(feedName);
        output.setName(name);
        return output;
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
