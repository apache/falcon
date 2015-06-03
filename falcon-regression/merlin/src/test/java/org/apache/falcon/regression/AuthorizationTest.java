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

package org.apache.falcon.regression;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.InstancesResult;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.apache.log4j.Logger;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

/**
 * test for Authorization in falcon .
 */
@Test(groups = "embedded")
public class AuthorizationTest extends BaseTestClass {
    private static final Logger LOGGER = Logger.getLogger(AuthorizationTest.class);

    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private String baseTestDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private String feedInputPath = baseTestDir + "/input" + MINUTE_DATE_PATTERN;

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        Bundle bundle = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundle, cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
    }

    /**
     * U2Delete test cases.
     */
    //disabled since, falcon does not have authorization https://issues.apache
    // .org/jira/browse/FALCON-388
    @Test(enabled = false)
    public void u1SubmitU2DeleteCluster() throws Exception {
        bundles[0].submitClusters(prism);
        final ServiceResponse serviceResponse = prism.getClusterHelper().delete(
            bundles[0].getClusters().get(0), MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
            "Entity submitted by first user should not be deletable by second user");
    }

    //disabled since, falcon does not have authorization https://issues.apache
    // .org/jira/browse/FALCON-388
    @Test(enabled = false)
    public void u1SubmitU2DeleteProcess() throws Exception {
        bundles[0].submitClusters(prism);
        bundles[0].submitProcess(true);
        final ServiceResponse serviceResponse = prism.getProcessHelper().delete(
            bundles[0].getProcessData(), MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
            "Entity submitted by first user should not be deletable by second user");
    }

    //disabled since, falcon does not have authorization https://issues.apache
    // .org/jira/browse/FALCON-388
    @Test(enabled = false)
    public void u1SubmitU2DeleteFeed() throws Exception {
        bundles[0].submitClusters(prism);
        bundles[0].submitFeed();
        final ServiceResponse serviceResponse = prism.getFeedHelper().delete(
            bundles[0].getDataSets().get(0), MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
            "Entity submitted by first user should not be deletable by second user");
    }

    //disabled since, falcon does not have authorization https://issues.apache
    // .org/jira/browse/FALCON-388
    @Test(enabled = false)
    public void u1ScheduleU2DeleteProcess()
        throws Exception {
        //submit, schedule process by U1
        bundles[0].submitFeedsScheduleProcess(prism);
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, bundles[0].getProcessData(),
            Job.Status.RUNNING);
        //try to delete process by U2
        final ServiceResponse serviceResponse = prism.getProcessHelper()
            .delete(bundles[0].getProcessData(), MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
            "Process scheduled by first user should not be deleted by second user");
    }

    //disabled since, falcon does not have authorization https://issues.apache
    // .org/jira/browse/FALCON-388
    @Test(enabled = false)
    public void u1ScheduleU2DeleteFeed() throws Exception {
        String feed = bundles[0].getInputFeedFromBundle();
        //submit, schedule feed by U1
        bundles[0].submitClusters(prism);
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed));
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, feed, Job.Status.RUNNING);
        //delete feed by U2
        final ServiceResponse serviceResponse = prism.getFeedHelper().delete(feed, MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
            "Feed scheduled by first user should not be deleted by second user");
    }

    //disabled since, falcon does not have authorization https://issues.apache
    // .org/jira/browse/FALCON-388
    @Test(enabled = false)
    public void u1SuspendU2DeleteProcess() throws Exception {
        //submit, schedule, suspend process by U1
        bundles[0].submitFeedsScheduleProcess(prism);
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, bundles[0].getProcessData(),
            Job.Status.RUNNING);
        AssertUtil.assertSucceeded(prism.getProcessHelper().suspend(bundles[0].getProcessData()));
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, bundles[0].getProcessData(),
            Job.Status.SUSPENDED);
        //try to delete process by U2
        final ServiceResponse serviceResponse = prism.getProcessHelper()
            .delete(bundles[0].getProcessData(), MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
            "Process suspended by first user should not be deleted by second user");
    }

    //disabled since, falcon does not have authorization https://issues.apache
    // .org/jira/browse/FALCON-388
    @Test(enabled = false)
    public void u1SuspendU2DeleteFeed() throws Exception {
        String feed = bundles[0].getInputFeedFromBundle();
        //submit, schedule, suspend feed by U1
        bundles[0].submitClusters(prism);
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed));
        AssertUtil.assertSucceeded(prism.getFeedHelper().suspend(feed));
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, feed, Job.Status.SUSPENDED);
        //delete feed by U2
        final ServiceResponse serviceResponse = prism.getFeedHelper()
            .delete(feed, MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
            "Feed scheduled by first user should not be deleted by second user");
    }

    /**
     * U2Suspend test cases.
     */
    //disabled since, falcon does not have authorization https://issues.apache
    // .org/jira/browse/FALCON-388
    @Test(enabled = false)
    public void u1ScheduleU2SuspendFeed() throws Exception {
        String feed = bundles[0].getInputFeedFromBundle();
        //submit, schedule by U1
        bundles[0].submitClusters(prism);
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed));
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, feed, Job.Status.RUNNING);
        //try to suspend by U2
        final ServiceResponse serviceResponse = prism.getFeedHelper()
            .suspend(feed, MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
            "Feed scheduled by first user should not be suspended by second user");
    }

    //disabled since, falcon does not have authorization https://issues.apache
    // .org/jira/browse/FALCON-388
    @Test(enabled = false)
    public void u1ScheduleU2SuspendProcess() throws Exception {
        bundles[0].submitFeedsScheduleProcess(prism);
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, bundles[0].getProcessData(),
            Job.Status.RUNNING);
        //try to suspend process by U2
        final ServiceResponse serviceResponse = prism.getProcessHelper()
            .suspend(bundles[0].getProcessData(), MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
            "Process scheduled by first user should not be suspended by second user");
    }

    /**
     * U2Resume test cases.
     */
    //disabled since, falcon does not have authorization https://issues.apache
    // .org/jira/browse/FALCON-388
    @Test(enabled = false)
    public void u1SuspendU2ResumeFeed() throws Exception {
        String feed = bundles[0].getInputFeedFromBundle();
        //submit, schedule and then suspend feed by User1
        bundles[0].submitClusters(prism);
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed));
        AssertUtil.assertSucceeded(prism.getFeedHelper().suspend(feed));
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, feed, Job.Status.SUSPENDED);
        //try to resume feed by User2
        final ServiceResponse serviceResponse = prism.getFeedHelper()
            .resume(feed, MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
            "Feed suspended by first user should not be resumed by second user");
    }

    //disabled since, falcon does not have authorization https://issues.apache
    // .org/jira/browse/FALCON-388
    @Test(enabled = false)
    public void u1SuspendU2ResumeProcess() throws Exception {
        //submit, schedule, suspend process by U1
        bundles[0].submitFeedsScheduleProcess(prism);
        AssertUtil.assertSucceeded(prism.getProcessHelper().suspend(bundles[0].getProcessData()));
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, bundles[0].getProcessData(),
            Job.Status.SUSPENDED);
        //try to resume process by U2
        final ServiceResponse serviceResponse = prism.getProcessHelper()
            .resume(bundles[0].getProcessData(), MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
            "Process suspended by first user should not be resumed by second user");
    }

    //disabled since, falcon does not have authorization https://issues.apache
    // .org/jira/browse/FALCON-388
    @Test(enabled = false)
    public void u1SuspendU2ResumeProcessInstances() throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(0);
        String endTime = TimeUtil.addMinsToTime(startTime, 5);
        String midTime = TimeUtil.addMinsToTime(startTime, 2);
        LOGGER.info("Start time: " + startTime + "\tEnd time: " + endTime);

        //prepare process definition
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessPeriodicity(1, Frequency.TimeUnit.minutes);
        bundles[0].setProcessConcurrency(5);
        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.minutes);
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setProcessInput("now(0,0)", "now(0,4)");

        //provide necessary data for first 3 instances to run
        LOGGER.info("Creating necessary data...");
        String prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);
        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(
            TimeUtil.addMinsToTime(startTime, -2), endTime, 0);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, prefix, dataDates);

        //submit, schedule process by U1
        LOGGER.info("Process data: " + Util.prettyPrintXml(bundles[0].getProcessData()));
        bundles[0].submitFeedsScheduleProcess(prism);

        //check that there are 3 running instances
        InstanceUtil.waitTillInstanceReachState(clusterOC, bundles[0].getProcessName(), 3,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS);

        //check that there are 2 waiting instances
        InstanceUtil.waitTillInstanceReachState(clusterOC, bundles[0].getProcessName(), 2,
                CoordinatorAction.Status.WAITING, EntityType.PROCESS);

        //3 instances should be running , other 2 should be waiting
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(bundles[0].getProcessName(),
            "?start=" + startTime + "&end=" + endTime);
        InstanceUtil.validateResponse(r, 5, 3, 0, 2, 0);

        //suspend 3 running instances
        r = prism.getProcessHelper().getProcessInstanceSuspend(bundles[0].getProcessName(),
            "?start=" + startTime + "&end=" + midTime);
        InstanceUtil.validateResponse(r, 3, 0, 3, 0, 0);

        //try to resume suspended instances by U2
        r = prism.getProcessHelper().getProcessInstanceResume(bundles[0].getProcessName(), "?start=" + startTime
                + "&end=" + midTime, MerlinConstants.USER2_NAME);

        //the state of above 3 instances should still be suspended
        InstanceUtil.validateResponse(r, 3, 0, 3, 0, 0);

        //check the status of all instances
        r = prism.getProcessHelper().getProcessInstanceStatus(bundles[0].getProcessName(),
            "?start=" + startTime + "&end=" + endTime);
        InstanceUtil.validateResponse(r, 5, 0, 3, 2, 0);
    }

    /**
     * U2Kill test cases.
     */
    //disabled since, falcon does not have authorization https://issues.apache
    // .org/jira/browse/FALCON-388
    @Test(enabled = false)
    public void u1ScheduleU2KillProcessInstances() throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(0);
        String endTime = TimeUtil.addMinsToTime(startTime, 5);
        LOGGER.info("Start time: " + startTime + "\tEnd time: " + endTime);

        //prepare process definition
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessPeriodicity(1, Frequency.TimeUnit.minutes);
        bundles[0].setProcessConcurrency(5);
        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.minutes);
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setProcessInput("now(0,0)", "now(0,4)");

        //provide necessary data for first 3 instances to run
        LOGGER.info("Creating necessary data...");
        String prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);
        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(
            TimeUtil.addMinsToTime(startTime, -2), endTime, 0);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, prefix, dataDates);

        //submit, schedule process by U1
        LOGGER.info("Process data: " + Util.prettyPrintXml(bundles[0].getProcessData()));
        bundles[0].submitFeedsScheduleProcess(prism);

        //check that there are 3 running instances
        InstanceUtil.waitTillInstanceReachState(clusterOC, bundles[0].getProcessName(), 3,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS);

        //3 instances should be running , other 2 should be waiting
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(bundles[0].getProcessName(),
            "?start=" + startTime + "&end=" + endTime);
        InstanceUtil.validateResponse(r, 5, 3, 0, 2, 0);

        //try to kill all instances by U2
        r = prism.getProcessHelper().getProcessInstanceKill(bundles[0].getProcessName(),
            "?start=" + startTime + "&end=" + endTime, MerlinConstants.USER2_NAME);

        //number of instances should be the same as before
        InstanceUtil.validateResponse(r, 5, 3, 0, 2, 0);
    }

    //disabled since, falcon does not have authorization https://issues.apache
    // .org/jira/browse/FALCON-388
    @Test(enabled = false)
    public void u1SuspendU2KillProcessInstances() throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(0);
        String endTime = TimeUtil.addMinsToTime(startTime, 5);
        String midTime = TimeUtil.addMinsToTime(startTime, 2);
        LOGGER.info("Start time: " + startTime + "\tEnd time: " + endTime);

        //prepare process definition
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessPeriodicity(1, Frequency.TimeUnit.minutes);
        bundles[0].setProcessConcurrency(5);
        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.minutes);
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setProcessInput("now(0,0)", "now(0,4)");

        //provide necessary data for first 3 instances to run
        LOGGER.info("Creating necessary data...");
        String prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);
        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(
            TimeUtil.addMinsToTime(startTime, -2), endTime, 0);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, prefix, dataDates);

        //submit, schedule process by U1
        LOGGER.info("Process data: " + Util.prettyPrintXml(bundles[0].getProcessData()));
        bundles[0].submitFeedsScheduleProcess(prism);

        //check that there are 3 running instances
        InstanceUtil.waitTillInstanceReachState(clusterOC, bundles[0].getProcessName(), 3,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS);

        //check that there are 2 waiting instances
        InstanceUtil.waitTillInstanceReachState(clusterOC, bundles[0].getProcessName(), 2,
                CoordinatorAction.Status.WAITING, EntityType.PROCESS);

        //3 instances should be running , other 2 should be waiting
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(bundles[0].getProcessName(),
            "?start=" + startTime + "&end=" + endTime);
        InstanceUtil.validateResponse(r, 5, 3, 0, 2, 0);

        //suspend 3 running instances
        r = prism.getProcessHelper().getProcessInstanceSuspend(bundles[0].getProcessName(),
            "?start=" + startTime + "&end=" + midTime);
        InstanceUtil.validateResponse(r, 3, 0, 3, 0, 0);

        //try to kill all instances by U2
        r = prism.getProcessHelper().getProcessInstanceKill(bundles[0].getProcessName(),
            "?start=" + startTime + "&end=" + endTime, MerlinConstants.USER2_NAME);

        //3 should still be suspended, 2 should be waiting
        InstanceUtil.validateResponse(r, 5, 0, 3, 2, 0);
    }

    /**
     * U2Rerun test cases.
     */
    //disabled since, falcon does not have authorization https://issues.apache
    // .org/jira/browse/FALCON-388
    @Test(enabled = false)
    public void u1KillSomeU2RerunAllProcessInstances()
        throws IOException, JAXBException, AuthenticationException, URISyntaxException,
        OozieClientException, InterruptedException {
        String startTime = TimeUtil
            .getTimeWrtSystemTime(0);
        String endTime = TimeUtil.addMinsToTime(startTime, 5);
        String midTime = TimeUtil.addMinsToTime(startTime, 2);
        LOGGER.info("Start time: " + startTime + "\tEnd time: " + endTime);

        //prepare process definition
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessPeriodicity(1, Frequency.TimeUnit.minutes);
        bundles[0].setProcessConcurrency(5);
        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.minutes);
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setProcessInput("now(0,0)", "now(0,3)");

        //provide necessary data for first 4 instances to run
        LOGGER.info("Creating necessary data...");
        String prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);
        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(
            TimeUtil.addMinsToTime(startTime, -2), endTime, 0);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, prefix, dataDates);

        //submit, schedule process by U1
        LOGGER.info("Process data: " + Util.prettyPrintXml(bundles[0].getProcessData()));
        bundles[0].submitFeedsScheduleProcess(prism);

        //check that there are 4 running instances
        InstanceUtil.waitTillInstanceReachState(clusterOC, bundles[0].getProcessName(), 4,
                CoordinatorAction.Status.RUNNING, EntityType.PROCESS);

        //4 instances should be running , 1 should be waiting
        InstancesResult r = prism.getProcessHelper().getProcessInstanceStatus(bundles[0].getProcessName(),
            "?start=" + startTime + "&end=" + endTime);
        InstanceUtil.validateResponse(r, 5, 4, 0, 1, 0);

        //kill 3 running instances
        r = prism.getProcessHelper().getProcessInstanceKill(bundles[0].getProcessName(),
                "?start=" + startTime + "&end=" + midTime);
        InstanceUtil.validateResponse(r, 3, 0, 0, 0, 3);

        //generally 3 instances should be killed, 1 is running and 1 is waiting

        //try to rerun instances by U2
        r = prism.getProcessHelper().getProcessInstanceRerun(bundles[0].getProcessName(),
                "?start=" + startTime + "&end=" + midTime, MerlinConstants.USER2_NAME);

        //instances should still be killed
        InstanceUtil.validateResponse(r, 3, 0, 0, 0, 3);
    }

    /**
     * U2Update test cases.
     */
    //disabled since, falcon does not have authorization https://issues.apache
    // .org/jira/browse/FALCON-388
    @Test(enabled = false)
    public void u1SubmitU2UpdateFeed()
        throws URISyntaxException, IOException, AuthenticationException, JAXBException,
        InterruptedException {
        FeedMerlin feed = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        //submit feed
        bundles[0].submitClusters(prism);
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(feed.toString()));
        String definition = prism.getFeedHelper().getEntityDefinition(feed.toString()).getMessage();
        Assert.assertTrue(definition.contains(feed.getName()) && !definition.contains("(feed) not found"),
            "Feed should be already submitted");
        //update feed definition
        FeedMerlin newFeed = new FeedMerlin(feed);
        newFeed.setFeedPathValue(baseTestDir + "/randomPath" + MINUTE_DATE_PATTERN);
        //try to update feed by U2
        final ServiceResponse serviceResponse = prism.getFeedHelper().update(feed.toString(), newFeed.toString(),
            MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
            "Feed submitted by first user should not be updated by second user");
    }

    //disabled since, falcon does not have authorization https://issues.apache
    // .org/jira/browse/FALCON-388
    @Test(enabled = false)
    public void u1ScheduleU2UpdateFeed() throws Exception {
        FeedMerlin feed = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        //submit and schedule feed
        bundles[0].submitClusters(prism);
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitAndSchedule(feed.toString()));
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, feed.toString(), Job.Status.RUNNING);
        //update feed definition
        FeedMerlin newFeed = new FeedMerlin(feed);
        newFeed.setFeedPathValue(baseTestDir + "/randomPath" + MINUTE_DATE_PATTERN);
        //try to update feed by U2
        final ServiceResponse serviceResponse = prism.getFeedHelper().update(feed.toString(), newFeed.toString(),
            MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
            "Feed scheduled by first user should not be updated by second user");
    }

    //disabled since, falcon does not have authorization https://issues.apache
    // .org/jira/browse/FALCON-388
    @Test(enabled = false)
    public void u1SubmitU2UpdateProcess() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        String processName = bundles[0].getProcessName();
        //submit process
        bundles[0].submitBundle(prism);
        String definition = prism.getProcessHelper()
            .getEntityDefinition(bundles[0].getProcessData()).getMessage();
        Assert.assertTrue(definition.contains(processName)
                &&
            !definition.contains("(process) not found"), "Process should be already submitted");
        //update process definition
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2020-01-02T01:04Z");
        //try to update process by U2
        final ServiceResponse serviceResponse = prism.getProcessHelper().update(bundles[0]
                .getProcessData(), bundles[0].getProcessData(),
            MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
            "Process submitted by first user should not be updated by second user");
    }

    //disabled since, falcon does not have authorization https://issues.apache
    // .org/jira/browse/FALCON-388
    @Test(enabled = false)
    public void u1ScheduleU2UpdateProcess() throws Exception {
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        //submit, schedule process by U1
        bundles[0].submitFeedsScheduleProcess(prism);
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, bundles[0].getProcessData(),
            Job.Status.RUNNING);
        //update process definition
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2020-01-02T01:04Z");
        //try to update process by U2
        final ServiceResponse serviceResponse = prism.getProcessHelper().update(bundles[0]
                .getProcessData(), bundles[0].getProcessData(),
            MerlinConstants.USER2_NAME);
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
            "Process scheduled by first user should not be updated by second user");
    }

    //disabled since, falcon does not have authorization https://issues.apache
    // .org/jira/browse/FALCON-388
    @Test(enabled = false)
    public void u1ScheduleFeedU2ScheduleDependantProcessU1UpdateFeed() throws Exception {
        FeedMerlin feed = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2099-01-02T01:00Z");
        //submit both feeds
        bundles[0].submitClusters(prism);
        bundles[0].submitFeeds(prism);
        //schedule input feed by U1
        AssertUtil.assertSucceeded(prism.getFeedHelper().schedule(feed.toString()));
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, feed.toString(), Job.Status.RUNNING);

        //by U2 schedule process dependant on scheduled feed by U1
        ServiceResponse serviceResponse = prism.getProcessHelper()
            .submitAndSchedule(bundles[0].getProcessData(), MerlinConstants.USER2_NAME);
        AssertUtil.assertSucceeded(serviceResponse);
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, bundles[0].getProcessData(), Job.Status.RUNNING);

        //get old process details
        String oldProcessBundleId = OozieUtil
            .getLatestBundleID(clusterOC, bundles[0].getProcessName(), EntityType.PROCESS);
        String oldProcessUser = getBundleUser(clusterOC, bundles[0].getProcessName(), EntityType.PROCESS);

        //get old feed details
        String oldFeedBundleId = OozieUtil.getLatestBundleID(clusterOC, feed.getName(), EntityType.FEED);
        String oldFeedUser = getBundleUser(clusterOC, feed.getName(), EntityType.FEED);

        //update feed definition
        FeedMerlin newFeed = new FeedMerlin(feed);
        newFeed.setFeedPathValue(baseTestDir + "/randomPath" + MINUTE_DATE_PATTERN);

        //update feed by U1
        serviceResponse = prism.getFeedHelper().update(feed.toString(), newFeed.toString(),
            MerlinConstants.CURRENT_USER_NAME);
        AssertUtil.assertSucceeded(serviceResponse);

        //new feed bundle should be created by U1
        OozieUtil.verifyNewBundleCreation(clusterOC, oldFeedBundleId, null, newFeed.toString(), true, false);
        String newFeedUser = getBundleUser(clusterOC, newFeed.getName(), EntityType.FEED);
        Assert.assertEquals(oldFeedUser, newFeedUser, "User should be the same");

        //new process bundle should be created by U2
        OozieUtil.verifyNewBundleCreation(
            clusterOC, oldProcessBundleId, null, bundles[0].getProcessData(), true, false);
        String newProcessUser =
            getBundleUser(clusterOC, bundles[0].getProcessName(), EntityType.PROCESS);
        Assert.assertEquals(oldProcessUser, newProcessUser, "User should be the same");
    }

    //disabled since, falcon does not have authorization https://issues.apache
    // .org/jira/browse/FALCON-388
    @Test(enabled = false)
    public void u1ScheduleFeedU2ScheduleDependantProcessU2UpdateFeed() throws Exception {
        FeedMerlin feed = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2099-01-02T01:00Z");
        //submit both feeds
        bundles[0].submitClusters(prism);
        bundles[0].submitFeeds(prism);
        //schedule input feed by U1
        AssertUtil.assertSucceeded(prism.getFeedHelper().schedule(feed.toString()));
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, feed.toString(), Job.Status.RUNNING);

        //by U2 schedule process dependent on scheduled feed by U1
        ServiceResponse serviceResponse = prism.getProcessHelper().submitAndSchedule(bundles[0].getProcessData(),
                MerlinConstants.USER2_NAME);
        AssertUtil.assertSucceeded(serviceResponse);
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, bundles[0].getProcessData(), Job.Status.RUNNING);

        //update feed definition
        FeedMerlin newFeed = new FeedMerlin(feed);
        newFeed.setFeedPathValue(baseTestDir + "/randomPath" + MINUTE_DATE_PATTERN);

        //get old process details
        String oldProcessBundleId = OozieUtil
                .getLatestBundleID(clusterOC, bundles[0].getProcessName(), EntityType.PROCESS);
        String oldProcessUser = getBundleUser(clusterOC, bundles[0].getProcessName(), EntityType.PROCESS);

        //get old feed details
        String oldFeedBundleId = OozieUtil.getLatestBundleID(clusterOC, feed.getName(), EntityType.FEED);
        String oldFeedUser = getBundleUser(clusterOC, feed.getName(), EntityType.FEED);

        //update feed by U2
        serviceResponse = prism.getFeedHelper().update(feed.toString(), newFeed.toString(),
            MerlinConstants.USER2_NAME);
        AssertUtil.assertSucceeded(serviceResponse);

        //new feed bundle should be created by U2
        OozieUtil.verifyNewBundleCreation(clusterOC, oldFeedBundleId, null, newFeed.toString(), true, false);
        String newFeedUser = getBundleUser(clusterOC, newFeed.getName(), EntityType.FEED);
        Assert.assertNotEquals(oldFeedUser, newFeedUser, "User should not be the same");
        Assert.assertEquals(MerlinConstants.USER2_NAME, newFeedUser);

        //new process bundle should be created by U2
        OozieUtil.verifyNewBundleCreation(
            clusterOC, oldProcessBundleId, null, bundles[0].getProcessData(), true, false);
        String newProcessUser = getBundleUser(clusterOC, bundles[0].getProcessName(), EntityType.PROCESS);
        Assert.assertEquals(oldProcessUser, newProcessUser, "User should be the same");
    }

    //disabled since, falcon does not have authorization https://issues.apache
    // .org/jira/browse/FALCON-388
    @Test(enabled = false)
    public void u1ScheduleFeedU1ScheduleDependantProcessU1UpdateProcess() throws Exception {
        String feed = bundles[0].getInputFeedFromBundle();
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2099-01-02T01:00Z");
        //submit both feeds
        bundles[0].submitClusters(prism);
        bundles[0].submitFeeds(prism);
        //schedule input feed by U1
        AssertUtil.assertSucceeded(prism.getFeedHelper().schedule(feed));
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, feed, Job.Status.RUNNING);

        //by U1 schedule process dependent on scheduled feed by U1
        ServiceResponse serviceResponse = prism.getProcessHelper().submitAndSchedule(bundles[0].getProcessData());
        AssertUtil.assertSucceeded(serviceResponse);
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, bundles[0].getProcessData(), Job.Status.RUNNING);

        //get old process details
        String oldProcessBundleId = OozieUtil
                .getLatestBundleID(clusterOC, bundles[0].getProcessName(), EntityType.PROCESS);
        String oldProcessUser = getBundleUser(clusterOC, bundles[0].getProcessName(), EntityType.PROCESS);

        //get old feed details
        String oldFeedBundleId = OozieUtil.getLatestBundleID(clusterOC, Util.readEntityName(feed), EntityType.FEED);

        //update process by U1
        ProcessMerlin processObj = bundles[0].getProcessObject();
        processObj.setProperty("randomProp", "randomVal");
        serviceResponse = prism.getProcessHelper().update(bundles[0].getProcessData(), processObj.toString());
        AssertUtil.assertSucceeded(serviceResponse);

        //new feed bundle should not be created
        OozieUtil.verifyNewBundleCreation(clusterOC, oldFeedBundleId, null, feed, false, false);

        //new process bundle should be created by U1
        OozieUtil.verifyNewBundleCreation(
            clusterOC, oldProcessBundleId, null, bundles[0].getProcessData(), true, false);
        String newProcessUser = getBundleUser(clusterOC, processObj.getName(), EntityType.PROCESS);
        Assert.assertEquals(oldProcessUser, newProcessUser, "User should be the same");
    }

    //disabled since, falcon does not have authorization https://issues.apache
    // .org/jira/browse/FALCON-388
    @Test(enabled = false)
    public void u1ScheduleFeedU1ScheduleDependantProcessU2UpdateProcess() throws Exception {
        String feed = bundles[0].getInputFeedFromBundle();
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2099-01-02T01:00Z");
        //submit both feeds
        bundles[0].submitClusters(prism);
        bundles[0].submitFeeds(prism);
        //schedule input feed by U1
        AssertUtil.assertSucceeded(prism.getFeedHelper().schedule(feed));
        AssertUtil.checkStatus(clusterOC, EntityType.FEED, feed, Job.Status.RUNNING);

        //by U1 schedule process dependent on scheduled feed by U1
        ServiceResponse serviceResponse = prism.getProcessHelper().submitAndSchedule(bundles[0].getProcessData());
        AssertUtil.assertSucceeded(serviceResponse);
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, bundles[0].getProcessData(), Job.Status.RUNNING);

        //get old process details
        String oldProcessBundleId = OozieUtil
                .getLatestBundleID(clusterOC, bundles[0].getProcessName(), EntityType.PROCESS);
        String oldProcessUser = getBundleUser(clusterOC, bundles[0].getProcessName(), EntityType.PROCESS);

        //get old feed details
        String oldFeedBundleId = OozieUtil.getLatestBundleID(clusterOC, Util.readEntityName(feed), EntityType.FEED);

        //update process by U2
        ProcessMerlin processObj = bundles[0].getProcessObject();
        processObj.setProperty("randomProp", "randomVal");
        serviceResponse = prism.getProcessHelper().update(bundles[0].getProcessData(), processObj.toString(),
            MerlinConstants.USER2_NAME);
        AssertUtil.assertSucceeded(serviceResponse);

        //new feed bundle should not be created
        OozieUtil.verifyNewBundleCreation(clusterOC, oldFeedBundleId, null, feed, false, false);

        //new process bundle should be created by U2
        OozieUtil.verifyNewBundleCreation(
            clusterOC, oldProcessBundleId, null, bundles[0].getProcessData(), true, false);
        String newProcessUser = getBundleUser(clusterOC, processObj.getName(), EntityType.PROCESS);
        Assert.assertNotEquals(oldProcessUser, newProcessUser, "User should not be the same");
        Assert.assertEquals(MerlinConstants.USER2_NAME, newProcessUser);
    }

    private String getBundleUser(OozieClient oozieClient, String entityName, EntityType entityType)
        throws OozieClientException {
        String newBundleId = OozieUtil.getLatestBundleID(oozieClient, entityName, entityType);
        BundleJob newBundleJob = oozieClient.getBundleJobInfo(newBundleId);
        CoordinatorJob coordinatorJob = null;
        for (CoordinatorJob coord : newBundleJob.getCoordinators()) {
            if ((entityType == EntityType.PROCESS && coord.getAppName().contains("DEFAULT"))
                || (entityType == EntityType.FEED && coord.getAppName().contains("RETENTION"))) {
                coordinatorJob = coord;
            }
        }
        Assert.assertNotNull(coordinatorJob);
        return coordinatorJob.getUser();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }
}
