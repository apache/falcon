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
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.List;


@Test(groups = "embedded")
public class OptionalInputTest extends BaseTestClass {

    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    OozieClient oozieClient = serverOC.get(0);
    String baseTestDir = baseHDFSDir + "/OptionalInputTest";
    String inputPath = baseTestDir + "/input";
    String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private static final Logger logger = Logger.getLogger(OptionalInputTest.class);

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle();
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        HadoopUtil.deleteDirIfExists(inputPath + "/", clusterFS);
        removeBundles();
    }

    /**
     * Test case: set 1 optional and 1 compulsory input. Provide data only for second one. Check
     * that process runs without waiting for optional input and finally succeeds.
     *
     * @throws Exception
     */
    @Test(enabled = true, groups = {"singleCluster"})
    public void optionalTest_1optional_1compulsary() throws Exception {
        bundles[0].generateRequiredBundle(1, 2, 1, inputPath, 1, "2010-01-02T01:00Z",
            "2010-01-02T01:12Z");
        for (int i = 0; i < bundles[0].getClusters().size(); i++)
            logger.info(Util.prettyPrintXml(bundles[0].getClusters().get(i)));

        for (int i = 0; i < bundles[0].getDataSets().size(); i++)
            logger.info(Util.prettyPrintXml(bundles[0].getDataSets().get(i)));

        bundles[0].setProcessInputStartEnd("now(0,-10)", "now(0,0)");
        bundles[0].setProcessConcurrency(2);
        logger.info(Util.prettyPrintXml(bundles[0].getProcessData()));

        bundles[0].submitAndScheduleBundle(prism, false);

        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide("2010-01-02T00:50Z",
            "2010-01-02T01:10Z", 5);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.SINGLE_FILE,
            inputPath + "/input1/", dataDates);

        InstanceUtil
            .waitTillInstanceReachState(oozieClient,
                Util.getProcessName(bundles[0].getProcessData()),
                2, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);
    }

    /**
     * Test case: set 1 optional and 2 compulsory inputs. Check that if data hasn't been provided
     * process is pending. Then provide data for compulsory inputs and check that process runs
     * and finally succeeds without waiting for optional input data.
     *
     * @throws Exception
     */
    @Test(enabled = true, groups = {"singleCluster"})
    public void optionalTest_1optional_2compulsary() throws Exception {
        bundles[0].generateRequiredBundle(1, 3, 1, inputPath, 1, "2010-01-02T01:00Z",
            "2010-01-02T01:12Z");

        for (int i = 0; i < bundles[0].getClusters().size(); i++)
            logger.info(Util.prettyPrintXml(bundles[0].getClusters().get(i)));

        for (int i = 0; i < bundles[0].getDataSets().size(); i++)
            logger.info(Util.prettyPrintXml(bundles[0].getDataSets().get(i)));

        bundles[0].setProcessInputStartEnd("now(0,-10)", "now(0,0)");
        bundles[0].setProcessConcurrency(2);
        logger.info(Util.prettyPrintXml(bundles[0].getProcessData()));

        bundles[0].submitAndScheduleBundle(prism, false);

        logger.info("instanceShouldStillBeInWaitingState");
        InstanceUtil
            .waitTillInstanceReachState(oozieClient,
                Util.getProcessName(bundles[0].getProcessData()),
                2, CoordinatorAction.Status.WAITING, EntityType.PROCESS);

        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide("2010-01-02T00:50Z",
            "2010-01-02T01:10Z", 5);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.SINGLE_FILE,
            inputPath + "/input1/", dataDates);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.SINGLE_FILE,
            inputPath + "/input2/", dataDates);

        InstanceUtil
            .waitTillInstanceReachState(oozieClient,
                Util.getProcessName(bundles[0].getProcessData()),
                2, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);
    }

    /**
     * Test case: set 2 optional and 1 compulsory inputs. Run process. Check that process
     * is pending because of lack of data. Provide it with data only for compulsory input. Check
     * that process runs and finally succeeds without waiting for optional input.
     *
     * @throws Exception
     */
    @Test(enabled = true, groups = {"singleCluster"})
    public void optionalTest_2optional_1compulsary() throws Exception {
        bundles[0].generateRequiredBundle(1, 3, 2, inputPath, 1, "2010-01-02T01:00Z",
            "2010-01-02T01:12Z");

        for (int i = 0; i < bundles[0].getClusters().size(); i++)
            logger.info(Util.prettyPrintXml(bundles[0].getClusters().get(i)));

        for (int i = 0; i < bundles[0].getDataSets().size(); i++)
            logger.info(Util.prettyPrintXml(bundles[0].getDataSets().get(i)));

        bundles[0].setProcessInputStartEnd("now(0,-10)", "now(0,0)");
        bundles[0].setProcessConcurrency(2);
        logger.info(Util.prettyPrintXml(bundles[0].getProcessData()));

        bundles[0].submitAndScheduleBundle(prism, false);

        InstanceUtil
            .waitTillInstanceReachState(oozieClient,
                Util.getProcessName(bundles[0].getProcessData()),
                2, CoordinatorAction.Status.WAITING, EntityType.PROCESS);

        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide("2010-01-02T00:50Z",
            "2010-01-02T01:10Z", 5);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.SINGLE_FILE,
            inputPath + "/input2/", dataDates);

        InstanceUtil
            .waitTillInstanceReachState(oozieClient,
                Util.getProcessName(bundles[0].getProcessData()),
                2, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);
    }

    /**
     * Test case: set process to have 1 optional and 1 compulsory input. Provide empty
     * directories for optional input and normal data for compulsory one. Check that process
     * doesn't wait for optional input, runs and finally succeeds.
     *
     * @throws Exception
     */
    @Test(enabled = true, groups = {"singleCluster"})
    public void optionalTest_optionalInputWithEmptyDir() throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(-4);
        String endTime = TimeUtil.getTimeWrtSystemTime(10);
        bundles[0].generateRequiredBundle(1, 2, 1, inputPath, 1, startTime, endTime);

        for (int i = 0; i < bundles[0].getClusters().size(); i++)
            logger.info(Util.prettyPrintXml(bundles[0].getClusters().get(i)));

        for (int i = 0; i < bundles[0].getDataSets().size(); i++)
            logger.info(Util.prettyPrintXml(bundles[0].getDataSets().get(i)));

        bundles[0].setProcessInputStartEnd("now(0,-10)", "now(0,0)");
        bundles[0].setProcessConcurrency(2);
        logger.info(Util.prettyPrintXml(bundles[0].getProcessData()));

        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(
            TimeUtil.addMinsToTime(startTime, -10), endTime, 5);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.SINGLE_FILE,
            inputPath + "/input1/", dataDates);
        HadoopUtil.recreateDir(clusterFS, inputPath + "/input0/");
        for (String date : dataDates) {
            HadoopUtil.recreateDir(clusterFS, inputPath + "/input0/" + date);
        }

        bundles[0].submitFeedsScheduleProcess(prism);

        InstanceUtil
            .waitTillInstanceReachState(oozieClient,
                Util.getProcessName(bundles[0].getProcessData()),
                2, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);
    }

    /**
     * Test case: set process with both optional inputs. Run it. Check that process have got
     * killed.
     *
     * @throws Exception
     */
    @Test(enabled = true, groups = {"singleCluster"})
    public void optionalTest_allInputOptional() throws Exception {
        bundles[0].generateRequiredBundle(1, 2, 2, inputPath, 1, "2010-01-02T01:00Z",
            "2010-01-02T01:12Z");

        bundles[0].setProcessInputNames("inputData");

        for (int i = 0; i < bundles[0].getClusters().size(); i++)
            logger.info(Util.prettyPrintXml(bundles[0].getClusters().get(i)));

        for (int i = 0; i < bundles[0].getDataSets().size(); i++)
            logger.info(Util.prettyPrintXml(bundles[0].getDataSets().get(i)));

        logger.info(Util.prettyPrintXml(bundles[0].getProcessData()));

        bundles[0].submitAndScheduleBundle(prism, false);

        InstanceUtil
            .waitTillInstanceReachState(oozieClient,
                Util.getProcessName(bundles[0].getProcessData()),
                2, CoordinatorAction.Status.KILLED, EntityType.PROCESS);
    }

    /**
     * Test case: set process with 1 optional and 1 compulsory input. Run it providing necessary
     * data. Check that process succeeds. Then update optional input to be compulsory. Check that
     * after process was updated it waits for data of updated input. Provide process with
     * necessary data and check that it succeeds finally.
     *
     * @throws Exception
     */
    @Test(enabled = true, groups = {"singleCluster"})
    public void optionalTest_updateProcessMakeOptionalCompulsory() throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(-4);
        String endTime = TimeUtil.getTimeWrtSystemTime(30);
        bundles[0].generateRequiredBundle(1, 2, 1, inputPath, 1, startTime, endTime);

        for (int i = 0; i < bundles[0].getClusters().size(); i++)
            logger.info(Util.prettyPrintXml(bundles[0].getClusters().get(i)));

        for (int i = 0; i < bundles[0].getDataSets().size(); i++)
            logger.info(Util.prettyPrintXml(bundles[0].getDataSets().get(i)));

        bundles[0].setProcessInputStartEnd("now(0,-10)", "now(0,0)");
        bundles[0].setProcessConcurrency(2);
        logger.info(Util.prettyPrintXml(bundles[0].getProcessData()));

        bundles[0].submitAndScheduleBundle(prism, true);

        InstanceUtil
            .waitTillInstanceReachState(oozieClient,
                Util.getProcessName(bundles[0].getProcessData()),
                2, CoordinatorAction.Status.WAITING, EntityType.PROCESS);

        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(
            TimeUtil.addMinsToTime(startTime, -10), endTime, 5);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.SINGLE_FILE,
            inputPath + "/input1/", dataDates);

        InstanceUtil
            .waitTillInstanceReachState(oozieClient,
                Util.getProcessName(bundles[0].getProcessData()),
                1, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        final ProcessMerlin processMerlin = new ProcessMerlin(bundles[0].getProcessData());
        processMerlin.setProcessFeeds(bundles[0].getDataSets(), 2, 0, 1);
        bundles[0].setProcessData(processMerlin.toString());
        bundles[0].setProcessInputStartEnd("now(0,-10)", "now(0,0)");
        logger.info("modified process:" + Util.prettyPrintXml(bundles[0].getProcessData()));

        prism.getProcessHelper().update(bundles[0].getProcessData(), bundles[0].getProcessData());

        //from now on ... it should wait of input0 also

        InstanceUtil
            .waitTillInstanceReachState(oozieClient,
                Util.getProcessName(bundles[0].getProcessData()),
                2, CoordinatorAction.Status.WAITING, EntityType.PROCESS);

        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.SINGLE_FILE,
            inputPath + "/input0/", dataDates);

        InstanceUtil
            .waitTillInstanceReachState(oozieClient,
                Util.getProcessName(bundles[0].getProcessData()),
                2, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);
    }

    /**
     * Test case: set process to have 1 optional and 1 compulsory input. Run it providing with
     * necessary data. Check that process succeeds without waiting for optional input. Then
     * update process to have 2 optional inputs instead of both optional and compulsory. Check
     * that process have got killed.
     *
     * @throws Exception
     */
    @Test(enabled = true, groups = {"singleCluster"})
    public void optionalTest_updateProcessMakeCompulsoryOptional() throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(-4);
        String endTime = TimeUtil.getTimeWrtSystemTime(30);
        bundles[0].generateRequiredBundle(1, 2, 1, inputPath, 1, startTime, endTime);

        for (int i = 0; i < bundles[0].getClusters().size(); i++)
            logger.info(Util.prettyPrintXml(bundles[0].getClusters().get(i)));

        for (int i = 0; i < bundles[0].getDataSets().size(); i++)
            logger.info(Util.prettyPrintXml(bundles[0].getDataSets().get(i)));

        bundles[0].setProcessInputStartEnd("now(0,-10)", "now(0,0)");
        bundles[0].setProcessConcurrency(4);
        logger.info(Util.prettyPrintXml(bundles[0].getProcessData()));

        bundles[0].submitAndScheduleBundle(prism, true);

        InstanceUtil
            .waitTillInstanceReachState(oozieClient,
                Util.getProcessName(bundles[0].getProcessData()),
                2, CoordinatorAction.Status.WAITING, EntityType.PROCESS);

        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(
            TimeUtil.addMinsToTime(startTime, -10), TimeUtil.addMinsToTime(endTime, 10), 5);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.SINGLE_FILE,
            inputPath + "/input1/", dataDates);
        InstanceUtil
            .waitTillInstanceReachState(oozieClient,
                Util.getProcessName(bundles[0].getProcessData()),
                1, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        final ProcessMerlin processMerlin = new ProcessMerlin(bundles[0].getProcessData());
        processMerlin.setProcessFeeds(bundles[0].getDataSets(), 2, 2, 1);
        bundles[0].setProcessData(processMerlin.toString());

        //delete all input data
        HadoopUtil.deleteDirIfExists(inputPath + "/", clusterFS);

        bundles[0].setProcessInputNames("inputData0", "inputData");

        logger.info("modified process:" + Util.prettyPrintXml(bundles[0].getProcessData()));

        prism.getProcessHelper().update(bundles[0].getProcessData(), bundles[0].getProcessData());

        logger.info("modified process:" + Util.prettyPrintXml(bundles[0].getProcessData()));
        //from now on ... it should wait of input0 also

        InstanceUtil
            .waitTillInstanceReachState(oozieClient,
                Util.getProcessName(bundles[0].getProcessData()),
                2, CoordinatorAction.Status.KILLED, EntityType.PROCESS);
    }
}
