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
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.text.ParseException;

/**
 * Test cases of falcon jobs using Local Oozie and LocalJobRunner.
 */
public class TestFalconUnit extends FalconUnitTestBase {

    @Test
    public void testProcessInstanceExecution() throws Exception {
        // submit with default props
        submitCluster();
        // submitting feeds
        APIResult result = submit(EntityType.FEED, getAbsolutePath("/infeed.xml"));
        assertStatus(result);
        result = submit(EntityType.FEED, getAbsolutePath("/outfeed.xml"));
        assertStatus(result);
        // submitting and scheduling process
        String scheduleTime = "2015-06-20T00:00Z";
        createData("in", "local", scheduleTime, "input.txt");
        result = submitProcess(getAbsolutePath("/process.xml"), "/app/oozie-mr");
        assertStatus(result);
        result = scheduleProcess("process", scheduleTime, 1, "local", getAbsolutePath("/workflow.xml"),
                true, "");
        assertStatus(result);
        waitForStatus(EntityType.PROCESS, "process", scheduleTime);
        InstancesResult.WorkflowStatus status = falconUnitClient.getInstanceStatus(EntityType.PROCESS,
                "process", scheduleTime);
        Assert.assertEquals(status, InstancesResult.WorkflowStatus.SUCCEEDED);
        String outPath = getFeedPathForTS("local", "out", scheduleTime);
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
        APIResult result = submit(EntityType.FEED, getAbsolutePath("/infeed.xml"));
        assertStatus(result);
        String scheduleTime = "2015-06-20T00:00Z";
        createData("in", "local", scheduleTime, "input.txt");
        String inPath = getFeedPathForTS("local", "in", scheduleTime);
        Assert.assertTrue(fs.exists(new Path(inPath)));
        result = schedule(EntityType.FEED, "in", "local");
        Assert.assertEquals(APIResult.Status.SUCCEEDED, result.getStatus());
        waitFor(WAIT_TIME, new Predicate() {
            public boolean evaluate() throws Exception {
                InstancesResult.WorkflowStatus status = getRetentionStatus("in", "local");
                return InstancesResult.WorkflowStatus.SUCCEEDED.equals(status);
            }
        });
        InstancesResult.WorkflowStatus status = getRetentionStatus("in", "local");
        Assert.assertEquals(InstancesResult.WorkflowStatus.SUCCEEDED, status);
        Assert.assertFalse(fs.exists(new Path(inPath)));
    }

    @Test
    public void testSuspendAndResume() throws Exception {
        // submit with default props
        submitCluster();
        // submitting feeds
        APIResult result = submit(EntityType.FEED, getAbsolutePath("/infeed.xml"));
        assertStatus(result);
        result = submit(EntityType.FEED, getAbsolutePath("/outfeed.xml"));
        assertStatus(result);
        // submitting and scheduling process
        String scheduleTime = "2015-06-20T00:00Z";
        createData("in", "local", scheduleTime, "input.txt");
        result = submitProcess(getAbsolutePath("/process1.xml"), "/app/oozie-mr");
        assertStatus(result);
        result = scheduleProcess("process1", scheduleTime, 2, "local", getAbsolutePath("/workflow.xml"),
                true, "");
        assertStatus(result);
        waitForStatus(EntityType.PROCESS, "process1", scheduleTime);
        result = getClient().suspend(EntityType.PROCESS, "process1", "local", null);
        assertStatus(result);
        result = getClient().getStatus(EntityType.PROCESS, "process1", "local", null);
        assertStatus(result);
        Assert.assertEquals(result.getMessage(), "SUSPENDED");
        result = getClient().resume(EntityType.PROCESS, "process1", "local", null);
        assertStatus(result);
        result = getClient().getStatus(EntityType.PROCESS, "process1", "local", null);
        assertStatus(result);
        Assert.assertEquals(result.getMessage(), "RUNNING");
    }
}
