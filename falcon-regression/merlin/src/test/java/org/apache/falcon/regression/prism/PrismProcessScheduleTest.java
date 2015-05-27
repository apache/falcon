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

package org.apache.falcon.regression.prism;

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.supportClasses.HadoopFileEditor;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.TestNGException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;

/**
 * Schedule process via prism tests.
 */
public class PrismProcessScheduleTest extends BaseTestClass {

    private ColoHelper cluster1 = servers.get(0);
    private ColoHelper cluster2 = servers.get(1);
    private OozieClient cluster1OC = serverOC.get(0);
    private OozieClient cluster2OC = serverOC.get(1);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private String workflowForNoIpOp = baseTestHDFSDir + "/noinop";
    private String process1;
    private String process2;

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        uploadDirToClusters(workflowForNoIpOp, OSUtil.RESOURCES+"workflows/aggregatorNoOutput/");
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        Bundle bundle = BundleUtil.readLateDataBundle();
        for (int i = 0; i < 2; i++) {
            bundles[i] = new Bundle(bundle, servers.get(i));
            bundles[i].generateUniqueBundle(this);
            bundles[i].setProcessWorkflow(aggregateWorkflowDir);
        }
        process1 = bundles[0].getProcessData();
        process2 = bundles[1].getProcessData();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    /**
     * Schedules first process on colo-1. Schedule second process on colo-2. Check that first
     * process hasn't been scheduled on colo-2.
     *
     * @throws Exception
     */
    @Test(groups = {"prism", "0.2", "embedded"})
    public void testProcessScheduleOnBothColos() throws Exception {
        //schedule both bundles
        bundles[0].submitAndScheduleProcess();
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster1OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);

        bundles[1].submitAndScheduleProcess();

        //now check if they have been scheduled correctly or not
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);

        //check if there is no criss cross
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
    }

    /**
     * Schedule first process on colo-1 and second one on colo-2. Then try to schedule them once
     * more on the same colos. Check that request succeed and process status hasn't been changed.
     *
     * @throws Exception
     */
    @Test(groups = {"prism", "0.2", "distributed"})
    public void testScheduleAlreadyScheduledProcessOnBothColos() throws Exception {
        //schedule both bundles
        bundles[0].submitAndScheduleProcess();
        bundles[1].submitAndScheduleProcess();

        //now check if they have been scheduled correctly or not
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);

        //check if there is no criss cross
        AssertUtil.checkNotStatus(cluster1OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
        AssertUtil.checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);

        AssertUtil.assertSucceeded(cluster1.getProcessHelper().schedule(process1));
        AssertUtil.assertSucceeded(cluster2.getProcessHelper().schedule(process2));

        //now check if they have been scheduled correctly or not
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);

    }

    /**
     * Schedule two processes on two different colos. Suspend process on first colo.
     * Try to schedule first process once more. Check that its status didn't change. Resume that
     * process. Suspend process on colo-2. Check that process on colo-1 is running and process on
     * colo-2 is suspended.
     *
     * @throws Exception
     */
    @Test(groups = {"prism", "0.2", "distributed"})
    public void testScheduleSuspendedProcessOnBothColos() throws Exception {
        //schedule both bundles
        bundles[0].submitAndScheduleProcess();
        bundles[1].submitAndScheduleProcess();

        //suspend process on colo-1
        AssertUtil.assertSucceeded(cluster1.getProcessHelper().suspend(process1));
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);

        //now check if it has been scheduled correctly or not
        AssertUtil.assertSucceeded(cluster1.getProcessHelper().schedule(process1));
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.SUSPENDED);
        AssertUtil.assertSucceeded(cluster1.getProcessHelper().resume(process1));
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);

        //suspend process on colo-2
        AssertUtil.assertSucceeded(cluster2.getProcessHelper().suspend(process2));
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.SUSPENDED);
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);

        //now check if it has been scheduled correctly or not
        AssertUtil.assertSucceeded(cluster2.getProcessHelper().schedule(process2));
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.SUSPENDED);
        AssertUtil.assertSucceeded(cluster2.getProcessHelper().resume(process2));
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
    }

    /**
     * Schedule two processes on different colos. Delete both of them. Try to schedule them once
     * more. Attempt should fail.
     *
     * @throws Exception
     */
    @Test(groups = {"prism", "0.2", "distributed"})
    public void testScheduleDeletedProcessOnBothColos() throws Exception {
        //schedule both bundles
        bundles[0].submitAndScheduleProcess();
        bundles[1].submitAndScheduleProcess();

        AssertUtil.assertSucceeded(prism.getProcessHelper().delete(process1));
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);

        AssertUtil.assertSucceeded(prism.getProcessHelper().delete(process2));
        AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.KILLED);
        AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.KILLED);

        AssertUtil.assertFailed(cluster2.getProcessHelper().schedule(process1));
        AssertUtil.assertFailed(cluster1.getProcessHelper().schedule(process2));
    }

    /**
     * Attempt to schedule non-submitted process should fail.
     *
     * @throws Exception
     */
    @Test(groups = {"prism", "0.2", "distributed"})
    public void testScheduleNonExistentProcessOnBothColos() throws Exception {
        AssertUtil.assertFailed(cluster2.getProcessHelper().schedule(process1));
        AssertUtil.assertFailed(cluster1.getProcessHelper().schedule(process2));
    }

    /**
     * Submit process which has colo-2 in it definition through prism. Shutdown falcon on colo-1.
     * Submit and schedule the same process through prism. Check that mentioned process is running
     * on colo-2.
     *
     * @throws Exception
     */
    @Test(groups = {"prism", "0.2", "distributed"})
    public void testProcessScheduleOn1ColoWhileOtherColoIsDown() throws Exception {
        try {
            bundles[1].submitProcess(true);
            Util.shutDownService(cluster1.getProcessHelper());
            AssertUtil.assertSucceeded(prism.getProcessHelper().submitAndSchedule(process2));

            //now check if they have been scheduled correctly or not
            AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);

            //check if there is no criss cross
            AssertUtil
                .checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {
            Util.restartService(cluster1.getProcessHelper());
        }
    }

    /**
     * Submit process through prism. Shutdown a colo. Try to schedule process though prism.
     * Process shouldn't be scheduled on that colo.
     *
     * @throws Exception
     */
    @Test(groups = {"prism", "0.2", "distributed"})
    public void testProcessScheduleOn1ColoWhileThatColoIsDown() throws Exception {
        try {
            bundles[0].submitProcess(true);
            Util.shutDownService(cluster1.getProcessHelper());
            AssertUtil.assertFailed(prism.getProcessHelper().schedule(process1));
            AssertUtil
                .checkNotStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        } finally {
            Util.restartService(cluster1.getProcessHelper());
        }
    }

    /**
     * Submit and schedule process. Suspend it. Submit and schedule another process on another
     * colo. Check that first process is suspended and the second is running both on matching
     * colos.
     *
     * @throws Exception
     */
    @Test(groups = {"prism", "0.2", "embedded"})
    public void testProcessScheduleOn1ColoWhileAnotherColoHasSuspendedProcess()
        throws Exception {
        try {
            bundles[0].submitAndScheduleProcess();
            AssertUtil.assertSucceeded(cluster1.getProcessHelper().suspend(process1));
            AssertUtil
                .checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.SUSPENDED);

            bundles[1].submitAndScheduleProcess();
            AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
            AssertUtil
                .checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
            AssertUtil
                .checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.SUSPENDED);
            AssertUtil
                .checkNotStatus(cluster1OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        }
    }

    /**
     * Schedule process on one colo. Kill it. Schedule process on another colo. Check that
     * processes were scheduled on appropriate colos and have expected statuses killed
     * and running respectively.
     *
     * @throws Exception
     */
    @Test(groups = {"prism", "0.2", "embedded"})
    public void testProcessScheduleOn1ColoWhileAnotherColoHasKilledProcess()
        throws Exception {
        try {
            bundles[0].submitAndScheduleProcess();
            AssertUtil.assertSucceeded(prism.getProcessHelper().delete(process1));
            AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.KILLED);

            bundles[1].submitAndScheduleProcess();
            AssertUtil.checkStatus(cluster2OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
            AssertUtil
                .checkNotStatus(cluster2OC, EntityType.PROCESS, bundles[0], Job.Status.RUNNING);
            AssertUtil.checkStatus(cluster1OC, EntityType.PROCESS, bundles[0], Job.Status.KILLED);
            AssertUtil
                .checkNotStatus(cluster1OC, EntityType.PROCESS, bundles[1], Job.Status.RUNNING);
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e.getMessage());
        }
    }

    /**
     * Schedule process. Wait till it become killed. Remove it. Submit and schedule it again.
     * Check that process was scheduled with new bundle associated to it.
     *
     * @throws Exception
     */
    @Test(groups = {"prism", "0.2", "embedded"}, enabled = true, timeOut = 1800000)
    public void testRescheduleKilledProcess() throws Exception {
        bundles[0].setProcessValidity(TimeUtil.getTimeWrtSystemTime(-1),
               TimeUtil.getTimeWrtSystemTime(1));
        HadoopFileEditor hadoopFileEditor = null;
        String process = bundles[0].getProcessData();
        try {
            hadoopFileEditor = new HadoopFileEditor(cluster1.getClusterHelper().getHadoopFS());
            hadoopFileEditor.edit(new ProcessMerlin(process).getWorkflow().getPath()
                    + "/workflow.xml", "<value>${outputData}</value>", "<property>\n"
                       + "                    <name>randomProp</name>\n"
                       + "                    <value>randomValue</value>\n"
                       + "                </property>");

            bundles[0].submitFeedsScheduleProcess(prism);

            InstanceUtil.waitTillInstancesAreCreated(cluster1OC, bundles[0].getProcessData(), 0);
            OozieUtil.createMissingDependencies(cluster1, EntityType.PROCESS,
                    bundles[0].getProcessName(), 0);
            InstanceUtil.waitTillInstanceReachState(cluster1OC,
                    bundles[0].getProcessName(), 2,
                    CoordinatorAction.Status.RUNNING, EntityType.PROCESS, 5);

            OozieUtil.waitForBundleToReachState(cluster1OC,
                Util.readEntityName(process), Job.Status.KILLED);
            String oldBundleID = OozieUtil.getLatestBundleID(cluster1OC,
                Util.readEntityName(process), EntityType.PROCESS);
            prism.getProcessHelper().delete(process);

            bundles[0].submitAndScheduleProcess();
            OozieUtil.verifyNewBundleCreation(cluster1OC, oldBundleID,
                new ArrayList<String>(), process, true, false);
        } finally {
            if (hadoopFileEditor != null) {
                hadoopFileEditor.restore();
            }
        }
    }

    /**
    * Schedule a process that contains no inputs. The process should be successfully scheduled.
    * Asserting that the instance is running after successfully scheduling.
    *
    * @throws Exception
    */
    @Test(groups = {"prism", "0.2", "embedded"}, enabled = true, timeOut = 1800000)
    public void testScheduleWhenZeroInputs()throws Exception{
        bundles[0].submitClusters(prism);
        bundles[0].setProcessWorkflow(workflowForNoIpOp);
        ProcessMerlin processObj = new ProcessMerlin(bundles[0].getProcessData());
        processObj.setInputs(null);
        processObj.setLateProcess(null);
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(bundles[0].getOutputFeedFromBundle()));
        AssertUtil.assertSucceeded(prism.getProcessHelper().submitAndSchedule(
            processObj.toString()));
        InstanceUtil.waitTillInstanceReachState(cluster1OC, Util.readEntityName(processObj.toString()), 2,
                CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS, 10);
    }

    /**
    * Schedule a process that contains no inputs or outputs. The process should be successfully scheduled.
    * Asserting that the instance is running after successfully scheduling.
    * @throws Exception
    */
    @Test(groups = {"prism", "0.2", "embedded"}, enabled = true, timeOut = 1800000)
    public void testScheduleWhenZeroInputsZeroOutputs()throws Exception{
        bundles[0].submitClusters(prism);
        bundles[0].setProcessWorkflow(workflowForNoIpOp);
        ProcessMerlin processObj = new ProcessMerlin(bundles[0].getProcessData());
        processObj.setInputs(null);
        processObj.setOutputs(null);
        processObj.setLateProcess(null);
        AssertUtil.assertSucceeded(prism.getProcessHelper().submitAndSchedule(
            processObj.toString()));
        InstanceUtil.waitTillInstanceReachState(cluster1OC, Util.readEntityName(processObj.toString()), 2,
                CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS, 10);
    }
}
