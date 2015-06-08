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

package org.apache.falcon.regression.lineage;

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.InstancesResult;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

import java.io.IOException;
import java.util.Date;

/**
 * Test list instances api for process.
 */
@Test(groups = "embedded")
public class ListProcessInstancesTest extends BaseTestClass {
    private static final Logger LOGGER = Logger.getLogger(ListProcessInstancesTest.class);
    private ColoHelper cluster = servers.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private String sourcePath = baseTestHDFSDir + "/source";
    private String feedDataLocation = sourcePath + MINUTE_DATE_PATTERN;
    private String startTime, endTime;
    private String processName;

    @BeforeClass(alwaysRun = true)
    public void setUp() throws IOException {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        startTime = TimeUtil.getTimeWrtSystemTime(-55);
        endTime = TimeUtil.getTimeWrtSystemTime(5);
        LOGGER.info("Time range is between : " + startTime + " and " + endTime);
    }

    @BeforeMethod(alwaysRun = true)
    public void prepareData() throws Exception {
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], servers.get(0));
        bundles[0].generateUniqueBundle(this);
        //prepare process
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setInputFeedDataPath(feedDataLocation);
        bundles[0].setOutputFeedLocationData(baseTestHDFSDir + "/output" + MINUTE_DATE_PATTERN);
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setInputFeedPeriodicity(5, Frequency.TimeUnit.minutes);
        bundles[0].setProcessConcurrency(3);
        bundles[0].submitAndScheduleProcess();
        processName = bundles[0].getProcessName();
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        //create data for processes to run and wait some time for instances to make progress
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0, 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0, 1);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0, 2);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 3,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 3);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        removeTestClassEntities();
    }

    /**
     * List process instances using orderBy - status, -startTime, -endTime params, expecting list of
     * process instances in the right order.
     */
    @Test
    public void testProcessOrderBy() throws Exception {
        SoftAssert softAssert = new SoftAssert();
        //orderBy startTime descending order
        InstancesResult r = prism.getProcessHelper().listInstances(processName,
            "orderBy=startTime&sortOrder=desc", null);
        InstancesResult.Instance[] instances = r.getInstances();
        Date previousDate = new Date();
        for (InstancesResult.Instance instance : instances) {
            Date current = instance.getStartTime();
            if (current != null) { //e.g if instance is WAITING it doesn't have start time
                softAssert.assertTrue(current.before(previousDate) || current.equals(previousDate),
                    "Wrong order. Current startTime :" + current + " Previous: " + previousDate);
                previousDate = (Date) current.clone();
            }
        }
        //suspend one instance and kill another one to create variety of statuses
        r = prism.getProcessHelper().getProcessInstanceSuspend(processName,
            "?start=" + TimeUtil.addMinsToTime(startTime, 4)
                + "&end=" + TimeUtil.addMinsToTime(startTime, 8));
        InstanceUtil.validateResponse(r, 1, 0, 1, 0, 0);
        r = prism.getProcessHelper().getProcessInstanceKill(processName,
            "?start=" + startTime + "&end=" + TimeUtil.addMinsToTime(startTime, 3));
        InstanceUtil.validateResponse(r, 1, 0, 0, 0, 1);
        //wait till instances status be stable
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 3,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 3);

        //orderBy status ascending order
        r = prism.getProcessHelper().listInstances(processName,
            "start=" + startTime + "&numResults=12&orderBy=status&sortOrder=desc", null);
        InstanceUtil.validateResponse(r, 12, 3, 1, 7, 1);
        instances = r.getInstances();
        InstancesResult.WorkflowStatus previousStatus = InstancesResult.WorkflowStatus.WAITING;
        for (InstancesResult.Instance instance : instances) {
            InstancesResult.WorkflowStatus current = instance.getStatus();
            softAssert.assertTrue(current.toString().compareTo(previousStatus.toString()) <= 0,
                "Wrong order. Compared " + current + " and " + previousStatus + " statuses.");
            previousStatus = current;
        }
        //only instances which already have finished have endTime
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 2,
            CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS, 10);
        //sort by end time, descending order
        r = prism.getProcessHelper().listInstances(processName,
            "start=" + startTime + "&numResults=12&orderBy=endTime&sortOrder=desc", null);
        instances = r.getInstances();
        previousDate = new Date();
        for (InstancesResult.Instance instance : instances) {
            Date current = instance.getEndTime();
            if (current != null) { //e.g if instance is WAITING it doesn't have end time
                softAssert.assertTrue(current.before(previousDate) || current.equals(previousDate),
                    "Wrong order. Current startTime :" + current + " Previous: " + previousDate);
                previousDate = (Date) current.clone();
            }
        }
        softAssert.assertAll();
    }

    /**
     * List process instances using -offset and -numResults params expecting list of process
     * instances to start at the right offset and give expected number of instances.
     */
    @Test
    public void testProcessOffsetNumResults() throws Exception {
        //check default number. Should be 10.
        InstancesResult r = prism.getProcessHelper().listInstances(processName, null, null);
        InstanceUtil.validateResponse(r, 10, 1, 0, 9, 0);

        //change to 6 expected 6
        r = prism.getProcessHelper().listInstances(processName, "numResults=6", null);
        InstanceUtil.validateResponse(r, 6, 0, 0, 6, 0);

        //use start option without numResults. 10 instances expected
        r = prism.getProcessHelper().listInstances(processName, "start=" + startTime, null);
        InstanceUtil.validateResponse(r, 10, 1, 0, 9, 0);

        //use start option with numResults value which is smaller then default.
        r = prism.getProcessHelper().listInstances(processName,
            "start=" + startTime + "&numResults=8", null);
        InstanceUtil.validateResponse(r, 8, 0, 0, 8, 0);

        //use start option with numResults value greater then default. All 12 instances expected
        r = prism.getProcessHelper().listInstances(processName,
            "start=" + startTime + "&numResults=12", null);
        InstanceUtil.validateResponse(r, 12, 3, 0, 9, 0);

        //get all instances
        InstancesResult.Instance[] allInstances = r.getInstances();

        //adding offset param into request. Expected (total number - offset) instances.
        int offset = 3;
        r = prism.getProcessHelper().listInstances(processName,
            "start=" + startTime + "&offset=" + offset + "&numResults=12", null);
        InstanceUtil.validateResponse(r, 9, 3, 0, 6, 0);

        //check that expected instances were retrieved
        InstancesResult.Instance[] instances = r.getInstances();
        for (int i = 0; i < 9; i++) {
            LOGGER.info("Comparing instances: " + instances[i] + " and " + allInstances[i + offset]);
            Assert.assertTrue(instances[i].getInstance().equals(allInstances[i + offset].getInstance()));
        }
        //use different offset and numResults params in request
        offset = 6;
        r = prism.getProcessHelper().listInstances(processName,
            "start=" + startTime + "&offset=" + offset + "&numResults=6", null);
        InstanceUtil.validateResponse(r, 6, 3, 0, 3, 0);

        //check that expected instance are contained in response
        instances = r.getInstances();
        for (int i = 0; i < 6; i++) {
            LOGGER.info("Comparing instances: " + instances[i] + " and " + allInstances[i + offset]);
            Assert.assertTrue(instances[i].getInstance().equals(allInstances[i + offset].getInstance()));
        }
    }

    /**
     * List process instances using -filterBy param. Expecting list of process instances
     * which have the given status.
     */
    @Test
    public void testProcessFilterBy() throws Exception {
        //test with simple filters
        InstancesResult r = prism.getProcessHelper().listInstances(processName,
            "filterBy=STATUS:RUNNING", null);
        //it gets 10 most recent instances, in our case only the oldest will be RUNNING
        InstanceUtil.validateResponse(r, 1, 1, 0, 0, 0);
        r = prism.getProcessHelper().listInstances(processName, "filterBy=STATUS:WAITING", null);
        InstanceUtil.validateResponse(r, 9, 0, 0, 9, 0);

        //get all instances
        r = prism.getProcessHelper().listInstances(processName,
            "start=" + startTime + "&numResults=12", null);
        InstanceUtil.validateResponse(r, 12, 3, 0, 9, 0);

        //suspend one instance and kill another to create variety of statuses
        r = prism.getProcessHelper().getProcessInstanceSuspend(processName,
            "?start=" + TimeUtil.addMinsToTime(startTime, 4)
                + "&end=" + TimeUtil.addMinsToTime(startTime, 8));
        InstanceUtil.validateResponse(r, 1, 0, 1, 0, 0);
        r = prism.getProcessHelper().getProcessInstanceKill(processName,
            "?start=" + startTime + "&end=" + TimeUtil.addMinsToTime(startTime, 3));
        InstanceUtil.validateResponse(r, 1, 0, 0, 0, 1);

        //wait till new instances be RUNNING and total status count be stable
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 3,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 3);

        //get all instances
        r = prism.getProcessHelper().listInstances(processName,
            "start=" + startTime + "&numResults=12", null);
        InstanceUtil.validateResponse(r, 12, 3, 1, 7, 1);

        //use different statuses, filterBy among all instances
        r = prism.getProcessHelper().listInstances(processName,
            "start=" + startTime + "&filterBy=STATUS:KILLED", null);
        InstanceUtil.validateResponse(r, 1, 0, 0, 0, 1);
        r = prism.getProcessHelper().listInstances(processName,
            "start=" + startTime + "&filterBy=STATUS:SUSPENDED", null);
        InstanceUtil.validateResponse(r, 1, 0, 1, 0, 0);
        r = prism.getProcessHelper().listInstances(processName,
            "start=" + startTime + "&filterBy=STATUS:RUNNING", null);
        InstanceUtil.validateResponse(r, 3, 3, 0, 0, 0);
        r = prism.getProcessHelper().listInstances(processName,
            "start=" + startTime + "&filterBy=STATUS:WAITING", null);
        InstanceUtil.validateResponse(r, 7, 0, 0, 7, 0);
    }

    /**
     * List process instances using start/end filter. Expecting list of process instances which
     * satisfy start and end filters.
     */
    @Test
    public void testProcessStartEnd() throws Exception {
        InstancesResult r = prism.getProcessHelper().listInstances(processName,
            "start=" + startTime + "&end=" + endTime, null);
        InstanceUtil.validateResponse(r, 10, 1, 0, 9, 0);

        //without params, default start/end should be used
        r = prism.getProcessHelper().listInstances(processName, null, null);
        InstanceUtil.validateResponse(r, 10, 1, 0, 9, 0);

        //increasing -start, -end stays the same
        r = prism.getProcessHelper().listInstances(processName,
            "start=" + startTime + "&end=" + TimeUtil.addMinsToTime(startTime, 29), null);
        InstanceUtil.validateResponse(r, 6, 3, 0, 3, 0);
        r = prism.getProcessHelper().listInstances(processName,
            "start=" + TimeUtil.addMinsToTime(startTime, 4)
                + "&end=" + TimeUtil.addMinsToTime(startTime, 29), null);
        InstanceUtil.validateResponse(r, 5, 2, 0, 3, 0);
        r = prism.getProcessHelper().listInstances(processName,
            "start=" + TimeUtil.addMinsToTime(startTime, 9)
                + "&end=" + TimeUtil.addMinsToTime(startTime, 29), null);
        InstanceUtil.validateResponse(r, 4, 1, 0, 3, 0);

        //one instance between start/end
        r = prism.getProcessHelper().listInstances(processName,
            "start=" + TimeUtil.addMinsToTime(startTime, 12)
                + "&end=" + TimeUtil.addMinsToTime(startTime, 16), null);
        InstanceUtil.validateResponse(r, 1, 0, 0, 1, 0);

        //only start, actual startTime, should get 10 most recent instances
        r = prism.getProcessHelper().listInstances(processName, "start=" + startTime, null);
        InstanceUtil.validateResponse(r, 10, 1, 0, 9, 0);

        //only start, greater then actual startTime
        r = prism.getProcessHelper().listInstances(processName,
            "start=" + TimeUtil.addMinsToTime(startTime, 19), null);
        InstanceUtil.validateResponse(r, 8, 0, 0, 8, 0);

        //only end, 1 instance
        r = prism.getProcessHelper().listInstances(processName,
            "end=" + TimeUtil.addMinsToTime(startTime, 4), null);
        InstanceUtil.validateResponse(r, 1, 1, 0, 0, 0);

        //only end, 10 the most recent instances
        r = prism.getProcessHelper().listInstances(processName,
            "end=" + endTime, null);
        InstanceUtil.validateResponse(r, 10, 1, 0, 9, 0);

        //only end, middle value
        r = prism.getProcessHelper().listInstances(processName,
            "end=" + TimeUtil.addMinsToTime(endTime, -31), null);
        InstanceUtil.validateResponse(r, 6, 3, 0, 3, 0);
    }

    /**
     * Test list process instances using custom filter. Expecting list of process instances which
     * satisfy custom filters.
     */
    @Test
    public void testProcessCustomFilter() throws Exception {
        String params = "start=" + startTime + "&end=" + TimeUtil.addMinsToTime(startTime, 26)
            + "&filterBy=status:RUNNING";
        InstancesResult r = prism.getProcessHelper().listInstances(processName, params, null);
        InstanceUtil.validateResponse(r, 3, 3, 0, 0, 0);

        params = "start=" + startTime + "&end=" + TimeUtil.addMinsToTime(startTime, 21)
            + "&filterBy=status:WAITING";
        r = prism.getProcessHelper().listInstances(processName, params, null);
        InstanceUtil.validateResponse(r, 2, 0, 0, 2, 0);

        params = "start=" + startTime + "&filterBy=status:WAITING&offset=2&numResult=12";
        r = prism.getProcessHelper().listInstances(processName, params, null);
        InstanceUtil.validateResponse(r, 7, 0, 0, 7, 0);

        params = "start=" + TimeUtil.addMinsToTime(startTime, 16) + "&filterBy=STATUS:WAITING,CLUSTER:"
            + bundles[0].getClusterNames().get(0) + "&offset=4&numResult=7&sortOrder=desc";
        r = prism.getProcessHelper().listInstances(processName, params, null);
        InstanceUtil.validateResponse(r, 4, 0, 0, 4, 0);

        params = "start=" + TimeUtil.addMinsToTime(startTime, 7) + "&filterBy=CLUSTER:"
            + bundles[0].getClusterNames().get(0) + "&offset=4&numResult=7&sortOrder=asc";
        r = prism.getProcessHelper().listInstances(processName, params, null);
        InstanceUtil.validateResponse(r, 6, 1, 0, 5, 0);
    }
}
