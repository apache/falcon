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

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
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
import org.apache.oozie.client.Job.Status;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Tests with process lib folder detached from workflow.xml
 */
@Test(groups = "embedded")
public class ProcessLibPathTest extends BaseTestClass {

    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    String testDir = baseHDFSDir + "/ProcessLibPath";
    String testLibDir = testDir + "/TestLib";
    private static final Logger logger = Logger.getLogger(ProcessLibPathTest.class);

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {

        logger.info("in @BeforeClass");
        //common lib for both test cases
        HadoopUtil.uploadDir(clusterFS, testLibDir, OSUtil.RESOURCES_OOZIE + "lib");

        Bundle b = BundleUtil.readELBundle();
        b.generateUniqueBundle();
        b = new Bundle(b, cluster);

        String startDate = "2010-01-01T22:00Z";
        String endDate = "2010-01-02T03:00Z";

        b.setInputFeedDataPath(testDir + "/input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        String prefix = b.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);

        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(startDate, endDate, 20);

        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, prefix, dataDates);
    }


    @BeforeMethod(alwaysRun = true)
    public void testName(Method method) throws Exception {
        logger.info("test name: " + method.getName());
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle();
        bundles[0].setInputFeedDataPath(baseHDFSDir + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(
            baseHDFSDir + "/output-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
        bundles[0].setProcessConcurrency(1);
        bundles[0].setProcessLibPath(testLibDir);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeBundles();
    }

    /**
     * Test which test a process with no lib folder in workflow location
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void setDifferentLibPathWithNoLibFolderInWorkflowfLocaltion() throws Exception {
        String workflowDir = testLibDir + "/aggregatorLib1/";
        HadoopUtil.uploadDir(clusterFS, workflowDir, OSUtil.RESOURCES_OOZIE);
        HadoopUtil.deleteDirIfExists(workflowDir + "/lib", clusterFS);
        bundles[0].setProcessWorkflow(workflowDir);
        logger.info("processData: " + Util.prettyPrintXml(bundles[0].getProcessData()));
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil
            .waitForBundleToReachState(cluster, bundles[0].getProcessName(), Status.SUCCEEDED);
    }

    /**
     * Test which test a process with wrong jar in lib folder in workflow location
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void setDifferentLibPathWithWrongJarInWorkflowLib() throws Exception {
        String workflowDir = testLibDir + "/aggregatorLib2/";
        HadoopUtil.uploadDir(clusterFS, workflowDir, OSUtil.RESOURCES_OOZIE);
        HadoopUtil.recreateDir(clusterFS, workflowDir + "/lib");
        HadoopUtil.copyDataToFolder(clusterFS, workflowDir + "/lib",
            OSUtil.RESOURCES + "ivory-oozie-lib-0.1.jar");
        bundles[0].setProcessWorkflow(workflowDir);
        logger.info("processData: " + Util.prettyPrintXml(bundles[0].getProcessData()));
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil
            .waitForBundleToReachState(cluster, bundles[0].getProcessName(), Status.SUCCEEDED);
    }
}
