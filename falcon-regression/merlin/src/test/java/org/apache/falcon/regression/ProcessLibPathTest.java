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

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.Job.Status;
import org.apache.oozie.client.OozieClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Tests with process lib folder detached from workflow.xml.
 */
@Test(groups = { "distributed", "embedded", "sanity" })
public class ProcessLibPathTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private String testDir = cleanAndGetTestDir();
    private String testLibDir = testDir + "/TestLib";
    private static final Logger LOGGER = Logger.getLogger(ProcessLibPathTest.class);
    private String processName;
    private String process;

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {
        LOGGER.info("in @BeforeClass");
        Bundle b = BundleUtil.readELBundle();
        b.generateUniqueBundle(this);
        b = new Bundle(b, cluster);
        String startDate = "2010-01-01T22:00Z";
        String endDate = "2010-01-02T03:00Z";
        b.setInputFeedDataPath(testDir + "/input" + MINUTE_DATE_PATTERN);
        String prefix = b.getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);
        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(startDate, endDate, 20);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, prefix, dataDates);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].setInputFeedDataPath(testDir + MINUTE_DATE_PATTERN);
        bundles[0].setProcessValidity("2010-01-02T01:00Z", "2010-01-02T01:04Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(testDir + "/output-data" + MINUTE_DATE_PATTERN);
        bundles[0].setProcessConcurrency(1);
        bundles[0].setProcessLibPath(testLibDir);
        process = bundles[0].getProcessData();
        processName = Util.readEntityName(process);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    /**
     * Tests a process with no lib folder in workflow location.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void setDifferentLibPathWithNoLibFolderInWorkflowfLocaltion() throws Exception {
        String workflowDir = testLibDir + "/aggregatorLib1/";
        HadoopUtil.uploadDir(clusterFS, workflowDir, OSUtil.RESOURCES_OOZIE);
        bundles[0].setProcessWorkflow(workflowDir);
        LOGGER.info("processData: " + Util.prettyPrintXml(process));
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, process, 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        OozieUtil.waitForBundleToReachState(clusterOC, processName, Status.SUCCEEDED);
    }

    /**
     * Test which test a process with wrong jar in lib folder in workflow location.
     *
     * @throws Exception
     */
    @Test(groups = {"singleCluster"})
    public void setDifferentLibPathWithWrongJarInWorkflowLib() throws Exception {
        String workflowDir = testLibDir + "/aggregatorLib2/";
        HadoopUtil.uploadDir(clusterFS, workflowDir, OSUtil.RESOURCES_OOZIE);
        HadoopUtil.recreateDir(clusterFS, workflowDir + "/lib");
        HadoopUtil.copyDataToFolder(clusterFS, workflowDir + "/lib/invalid.jar",
            OSUtil.concat(OSUtil.NORMAL_INPUT, "dataFile.xml"));
        bundles[0].setProcessWorkflow(workflowDir);
        LOGGER.info("processData: " + Util.prettyPrintXml(process));
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, process, 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        OozieUtil.waitForBundleToReachState(clusterOC, processName, Status.SUCCEEDED);
    }
}
