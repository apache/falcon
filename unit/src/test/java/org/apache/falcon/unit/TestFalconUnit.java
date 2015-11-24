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
package org.apache.falcon.unit;

import org.apache.falcon.FalconException;
import org.apache.falcon.FalconWebException;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Property;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.EntityList;
import org.apache.falcon.resource.EntitySummaryResult;
import org.apache.falcon.resource.FeedInstanceResult;
import org.apache.falcon.resource.InstanceDependencyResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesSummaryResult;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;

import static org.apache.falcon.entity.EntityUtil.getEntity;


/**
 * Test cases of falcon jobs using Local Oozie and LocalJobRunner.
 */
public class TestFalconUnit extends FalconUnitTestBase {

    private static final String INPUT_FEED = "infeed.xml";
    private static final String OUTPUT_FEED = "outfeed.xml";
    private static final String PROCESS = "process.xml";
    private static final String PROCESS_APP_PATH = "/app/oozie-mr";
    private static final String CLUSTER_NAME = "local";
    private static final String INPUT_FEED_NAME = "in";
    private static final String PROCESS_NAME = "process";
    private static final String OUTPUT_FEED_NAME = "out";
    private static final String INPUT_FILE_NAME = "input.txt";
    private static final String SCHEDULE_TIME = "2013-11-18T00:05Z";
    private static final String END_TIME = "2013-11-18T00:07Z";
    private static final String WORKFLOW = "workflow.xml";
    private static final String SLEEP_WORKFLOW = "sleepWorkflow.xml";

    @Test
    public void testProcessInstanceExecution() throws Exception {
        submitClusterAndFeeds();
        // submitting and scheduling process
        createData(INPUT_FEED_NAME, CLUSTER_NAME, SCHEDULE_TIME, INPUT_FILE_NAME);
        APIResult result = submitProcess(getAbsolutePath(PROCESS), PROCESS_APP_PATH);
        assertStatus(result);
        result = scheduleProcess(PROCESS_NAME, SCHEDULE_TIME, 1, CLUSTER_NAME, getAbsolutePath(WORKFLOW), true, "");
        assertStatus(result);
        waitForStatus(EntityType.PROCESS.name(), PROCESS_NAME, SCHEDULE_TIME, InstancesResult.WorkflowStatus.SUCCEEDED);
        InstancesResult.WorkflowStatus status = getClient().getInstanceStatus(EntityType.PROCESS.name(),
                PROCESS_NAME, SCHEDULE_TIME);
        Assert.assertEquals(status, InstancesResult.WorkflowStatus.SUCCEEDED);
        String outPath = getFeedPathForTS(CLUSTER_NAME, OUTPUT_FEED_NAME, SCHEDULE_TIME);
        Assert.assertTrue(getFileSystem().exists(new Path(outPath)));
        FileStatus[] files = getFileSystem().listStatus(new Path(outPath));
        Assert.assertTrue(files.length > 0);
    }

    @Test
    public void testRetention() throws IOException, FalconCLIException, FalconException,
            ParseException, InterruptedException {
        // submit with default props
        submitCluster();
        // submitting feeds
        APIResult result = submit(EntityType.FEED, getAbsolutePath(INPUT_FEED));
        assertStatus(result);
        createData(INPUT_FEED_NAME, CLUSTER_NAME, SCHEDULE_TIME, INPUT_FILE_NAME);
        String inPath = getFeedPathForTS(CLUSTER_NAME, INPUT_FEED_NAME, SCHEDULE_TIME);
        Assert.assertTrue(fs.exists(new Path(inPath)));
        result = schedule(EntityType.FEED, INPUT_FEED_NAME, CLUSTER_NAME);
        Assert.assertEquals(APIResult.Status.SUCCEEDED, result.getStatus());
        waitFor(WAIT_TIME, new Predicate() {
            public boolean evaluate() throws Exception {
                InstancesResult.WorkflowStatus status = getRetentionStatus(INPUT_FEED_NAME, CLUSTER_NAME);
                return InstancesResult.WorkflowStatus.SUCCEEDED.equals(status);
            }
        });
        InstancesResult.WorkflowStatus status = getRetentionStatus(INPUT_FEED_NAME, CLUSTER_NAME);
        Assert.assertEquals(InstancesResult.WorkflowStatus.SUCCEEDED, status);
        Assert.assertFalse(fs.exists(new Path(inPath)));
    }

    @Test
    public void testSuspendAndResume() throws Exception {
        submitClusterAndFeeds();
        // submitting and scheduling process;
        createData(INPUT_FEED_NAME, CLUSTER_NAME, SCHEDULE_TIME, INPUT_FILE_NAME);
        APIResult result = submitProcess(getAbsolutePath(PROCESS), PROCESS_APP_PATH);
        assertStatus(result);
        result = scheduleProcess(PROCESS_NAME, SCHEDULE_TIME, 2, CLUSTER_NAME, getAbsolutePath(WORKFLOW), true, "");
        assertStatus(result);
        waitForStatus(EntityType.PROCESS.name(), PROCESS_NAME, SCHEDULE_TIME, InstancesResult.WorkflowStatus.SUCCEEDED);
        result = getClient().suspend(EntityType.PROCESS, PROCESS_NAME, CLUSTER_NAME, null);
        assertStatus(result);
        result = getClient().getStatus(EntityType.PROCESS, PROCESS_NAME, CLUSTER_NAME, null);
        assertStatus(result);
        Assert.assertEquals(result.getMessage(), "SUSPENDED");
        result = getClient().resume(EntityType.PROCESS, PROCESS_NAME, CLUSTER_NAME, null);
        assertStatus(result);
        result = getClient().getStatus(EntityType.PROCESS, PROCESS_NAME, CLUSTER_NAME, null);
        assertStatus(result);
        Assert.assertEquals(result.getMessage(), "RUNNING");
    }

    @Test
    public void testDelete() throws IOException, FalconCLIException, FalconException,
            ParseException, InterruptedException {
        // submit cluster and feeds
        submitClusterAndFeeds();
        APIResult result = submitProcess(getAbsolutePath(PROCESS), PROCESS_APP_PATH);
        assertStatus(result);
        createData(INPUT_FEED_NAME, CLUSTER_NAME, SCHEDULE_TIME, INPUT_FILE_NAME);
        result = submitProcess(getAbsolutePath(PROCESS), PROCESS_APP_PATH);
        assertStatus(result);
        result = scheduleProcess(PROCESS_NAME, SCHEDULE_TIME, 2, CLUSTER_NAME, getAbsolutePath(WORKFLOW), true, "");
        assertStatus(result);
        waitForStatus(EntityType.PROCESS.name(), PROCESS_NAME, SCHEDULE_TIME, InstancesResult.WorkflowStatus.SUCCEEDED);
        result = getClient().delete(EntityType.PROCESS, PROCESS_NAME, null);
        assertStatus(result);
        try {
            getEntity(EntityType.PROCESS, PROCESS_NAME);
            Assert.fail("Exception should be thrown");
        } catch (FalconException e) {
            // nothing to do
        }

        result = getClient().delete(EntityType.FEED, INPUT_FEED_NAME, null);
        assertStatus(result);
        try {
            getEntity(EntityType.FEED, INPUT_FEED_NAME);
            Assert.fail("Exception should be thrown");
        } catch (FalconException e) {
            // nothing to do
        }
    }

    @Test
    public void testValidate() throws IOException, FalconCLIException, FalconException {
        submitClusterAndFeeds();
        APIResult result = getClient().validate(EntityType.PROCESS.name(),
                getAbsolutePath(PROCESS), true, null);
        assertStatus(result);
        try {
            getClient().validate(EntityType.PROCESS.name(),
                    getAbsolutePath(INPUT_FEED), true, null);
            Assert.fail("Exception should be thrown");
        } catch (FalconWebException e) {
            // nothing to do
        }
    }

    @Test
    public void testUpdateAndTouch() throws IOException, FalconCLIException, FalconException, ParseException,
            InterruptedException {
        submitClusterAndFeeds();
        APIResult result = submitProcess(getAbsolutePath(PROCESS), PROCESS_APP_PATH);
        assertStatus(result);
        result = getClient().update(EntityType.PROCESS.name(), PROCESS_NAME,
                getAbsolutePath(PROCESS), true, null);
        assertStatus(result);
        createData(INPUT_FEED_NAME, CLUSTER_NAME, SCHEDULE_TIME, INPUT_FILE_NAME);
        result = submitProcess(getAbsolutePath(PROCESS), PROCESS_APP_PATH);
        assertStatus(result);
        result = scheduleProcess(PROCESS_NAME, SCHEDULE_TIME, 1, CLUSTER_NAME, getAbsolutePath(WORKFLOW), true, "");
        assertStatus(result);
        waitForStatus(EntityType.PROCESS.name(), PROCESS_NAME, SCHEDULE_TIME, InstancesResult.WorkflowStatus.SUCCEEDED);

        Process process = getEntity(EntityType.PROCESS, PROCESS_NAME);
        setDummyProperty(process);
        String processXml = process.toString();

        File file = new File("target/newprocess.xml");
        file.createNewFile();
        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
        bw.write(processXml);
        bw.close();

        result = falconUnitClient.update(EntityType.PROCESS.name(), PROCESS_NAME, file.getAbsolutePath(), true, null);
        assertStatus(result);
        result = falconUnitClient.touch(EntityType.PROCESS.name(), PROCESS_NAME, null, true, null);
        assertStatus(result);

        process = getEntity(EntityType.PROCESS,
                PROCESS_NAME);
        Assert.assertEquals(process.toString(), processXml);
        file.delete();
    }

    private void submitClusterAndFeeds() throws IOException, FalconCLIException {
        // submit with default props
        submitCluster();
        // submitting feeds
        APIResult result = submit(EntityType.FEED, getAbsolutePath(INPUT_FEED));
        assertStatus(result);
        result = submit(EntityType.FEED, getAbsolutePath(OUTPUT_FEED));
        assertStatus(result);
    }

    public void setDummyProperty(Process process) {
        Property property = new Property();
        property.setName("dummy");
        property.setValue("dummy");
        process.getProperties().getProperties().add(property);
    }

    @Test
    public void testProcessInstanceManagementAPI1() throws Exception {
        submitClusterAndFeeds();
        // submitting and scheduling process
        createData(INPUT_FEED_NAME, CLUSTER_NAME, SCHEDULE_TIME, INPUT_FILE_NAME);
        APIResult result = submitProcess(getAbsolutePath(PROCESS), PROCESS_APP_PATH);
        assertStatus(result);
        result = scheduleProcess(PROCESS_NAME, SCHEDULE_TIME, 2, CLUSTER_NAME, getAbsolutePath(SLEEP_WORKFLOW), true,
                "");
        assertStatus(result);
        InstancesResult.WorkflowStatus currentStatus;
        waitForStatus(EntityType.PROCESS.name(), PROCESS_NAME, SCHEDULE_TIME, InstancesResult.WorkflowStatus.RUNNING);
        currentStatus = getClient().getInstanceStatus(EntityType.PROCESS.name(), PROCESS_NAME, SCHEDULE_TIME);
        Assert.assertEquals(currentStatus, InstancesResult.WorkflowStatus.RUNNING);

        getClient().suspendInstances(EntityType.PROCESS.name(), PROCESS_NAME, SCHEDULE_TIME, END_TIME, null,
                CLUSTER_NAME, null, null, null);
        waitForStatus(EntityType.PROCESS.name(), PROCESS_NAME, SCHEDULE_TIME, InstancesResult.WorkflowStatus.SUSPENDED);
        currentStatus = getClient().getInstanceStatus(EntityType.PROCESS.name(), PROCESS_NAME, SCHEDULE_TIME);
        Assert.assertEquals(currentStatus, InstancesResult.WorkflowStatus.SUSPENDED);

        getClient().resumeInstances(EntityType.PROCESS.name(), PROCESS_NAME, SCHEDULE_TIME, END_TIME, null,
                CLUSTER_NAME, null, null, null);
        waitForStatus(EntityType.PROCESS.name(), PROCESS_NAME, SCHEDULE_TIME, InstancesResult.WorkflowStatus.RUNNING);
        currentStatus = getClient().getInstanceStatus(EntityType.PROCESS.name(), PROCESS_NAME, SCHEDULE_TIME);
        Assert.assertEquals(currentStatus, InstancesResult.WorkflowStatus.RUNNING);

        getClient().killInstances(EntityType.PROCESS.name(), PROCESS_NAME, SCHEDULE_TIME, END_TIME, null, CLUSTER_NAME,
                null, null, null);
        waitForStatus(EntityType.PROCESS.name(), PROCESS_NAME, SCHEDULE_TIME, InstancesResult.WorkflowStatus.KILLED);
        currentStatus = getClient().getInstanceStatus(EntityType.PROCESS.name(), PROCESS_NAME, SCHEDULE_TIME);
        Assert.assertEquals(currentStatus, InstancesResult.WorkflowStatus.KILLED);

        getClient().rerunInstances(EntityType.PROCESS.name(), PROCESS_NAME, SCHEDULE_TIME, END_TIME, null, null,
                CLUSTER_NAME, null, null, true, null);
        waitForStatus(EntityType.PROCESS.name(), PROCESS_NAME, SCHEDULE_TIME, InstancesResult.WorkflowStatus.RUNNING);
        currentStatus = getClient().getInstanceStatus(EntityType.PROCESS.name(), PROCESS_NAME, SCHEDULE_TIME);
        Assert.assertEquals(currentStatus, InstancesResult.WorkflowStatus.RUNNING);
    }

    @Test
    public void testProcessInstanceManagementAPI2() throws Exception {
        submitClusterAndFeeds();
        // submitting and scheduling process
        createData(INPUT_FEED_NAME, CLUSTER_NAME, SCHEDULE_TIME, INPUT_FILE_NAME);
        APIResult result = submitProcess(getAbsolutePath(PROCESS), PROCESS_APP_PATH);
        assertStatus(result);
        result = scheduleProcess(PROCESS_NAME, SCHEDULE_TIME, 2, CLUSTER_NAME, getAbsolutePath(SLEEP_WORKFLOW), true,
                "");
        InstancesResult.WorkflowStatus currentStatus;
        waitForStatus(EntityType.PROCESS.name(), PROCESS_NAME, SCHEDULE_TIME, InstancesResult.WorkflowStatus.SUCCEEDED);
        currentStatus = getClient().getInstanceStatus(EntityType.PROCESS.name(), PROCESS_NAME, SCHEDULE_TIME);
        Assert.assertEquals(currentStatus, InstancesResult.WorkflowStatus.SUCCEEDED);

        InstancesSummaryResult summaryResult = getClient().getSummaryOfInstances(EntityType.PROCESS.name(),
                PROCESS_NAME, SCHEDULE_TIME, END_TIME, null, null, null, "asc", null, null);
        Assert.assertEquals(summaryResult.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(summaryResult.getInstancesSummary());
        Assert.assertEquals(summaryResult.getInstancesSummary().length, 1);


        InstancesResult instancesResult = getClient().getLogsOfInstances(EntityType.PROCESS.name(), PROCESS_NAME,
                SCHEDULE_TIME, END_TIME, null, "0", null, null, "asc", null, new Integer(0), new Integer(1), null);
        Assert.assertEquals(instancesResult.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(instancesResult.getInstances());
        Assert.assertEquals(instancesResult.getInstances().length, 1);

        instancesResult = getClient().getParamsOfInstance(EntityType.PROCESS.name(), PROCESS_NAME, SCHEDULE_TIME, null,
                null, null);
        Assert.assertEquals(instancesResult.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(instancesResult.getInstances());
        Assert.assertEquals(instancesResult.getInstances().length, 1);

        InstanceDependencyResult dependencyResult = getClient().getInstanceDependencies(EntityType.PROCESS.name(),
                PROCESS_NAME, SCHEDULE_TIME, null);
        Assert.assertEquals(dependencyResult.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(dependencyResult.getDependencies());
        Assert.assertEquals(dependencyResult.getDependencies().length, 2); //2 for input and output feed
    }

    @Test
    public void testFeedInstanceManagementAPI() throws Exception {
        // submit with default props
        submitCluster();
        // submitting feeds
        APIResult result = submit(EntityType.FEED, getAbsolutePath(INPUT_FEED));
        assertStatus(result);
        createData(INPUT_FEED_NAME, CLUSTER_NAME, SCHEDULE_TIME, INPUT_FILE_NAME);
        String inPath = getFeedPathForTS(CLUSTER_NAME, INPUT_FEED_NAME, SCHEDULE_TIME);
        Assert.assertTrue(fs.exists(new Path(inPath)));
        result = schedule(EntityType.FEED, INPUT_FEED_NAME, CLUSTER_NAME);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
        FeedInstanceResult feedInstanceResult = getClient().getFeedListing(EntityType.FEED.name(), INPUT_FEED_NAME,
                SCHEDULE_TIME, END_TIME, null, null);
        Assert.assertEquals(feedInstanceResult.getStatus(), APIResult.Status.SUCCEEDED);
    }

    @Test
    public void testEntityList() throws Exception {
        submitClusterAndFeeds();
        // submitting and scheduling process
        createData(INPUT_FEED_NAME, CLUSTER_NAME, SCHEDULE_TIME, INPUT_FILE_NAME);
        APIResult result = submitAndSchedule(EntityType.PROCESS.name(), getAbsolutePath(PROCESS),
                getAbsolutePath(SLEEP_WORKFLOW), true, null, "", PROCESS_APP_PATH);
        assertStatus(result);
        waitForStatus(EntityType.PROCESS.name(), PROCESS_NAME, SCHEDULE_TIME, InstancesResult.WorkflowStatus.RUNNING);
        InstancesResult.WorkflowStatus currentStatus = getClient().getInstanceStatus(EntityType.PROCESS.name(),
                PROCESS_NAME, SCHEDULE_TIME);
        Assert.assertEquals(currentStatus, InstancesResult.WorkflowStatus.RUNNING);

        EntityList entityList = getClient().getEntityList(EntityType.PROCESS.name(), "", "", null, null, null, null,
                null, new Integer(0), new Integer(1), null);
        Assert.assertNotNull(entityList.getElements());
        Assert.assertEquals(entityList.getElements().length, 1);
        Assert.assertEquals(entityList.getElements()[0].name, PROCESS_NAME);
    }

    @Test
    public void testEntitySummary() throws Exception {
        submitClusterAndFeeds();
        // submitting and scheduling process
        createData(INPUT_FEED_NAME, CLUSTER_NAME, SCHEDULE_TIME, INPUT_FILE_NAME);
        APIResult result = submitAndSchedule(EntityType.PROCESS.name(), getAbsolutePath(PROCESS),
                getAbsolutePath(SLEEP_WORKFLOW), true, null, "", PROCESS_APP_PATH);
        assertStatus(result);
        waitForStatus(EntityType.PROCESS.name(), PROCESS_NAME, SCHEDULE_TIME, InstancesResult.WorkflowStatus.RUNNING);
        InstancesResult.WorkflowStatus currentStatus = getClient().getInstanceStatus(EntityType.PROCESS.name(),
                PROCESS_NAME, SCHEDULE_TIME);
        Assert.assertEquals(currentStatus, InstancesResult.WorkflowStatus.RUNNING);
        EntitySummaryResult summaryResult = getClient().getEntitySummary(EntityType.PROCESS.name(), CLUSTER_NAME,
                SCHEDULE_TIME, END_TIME, "", "", null, null, null, new Integer(0), new Integer(1), new Integer(1),
                null);
        Assert.assertEquals(summaryResult.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertNotNull(summaryResult.getEntitySummaries());
        Assert.assertEquals(summaryResult.getEntitySummaries().length, 1);
    }
}
