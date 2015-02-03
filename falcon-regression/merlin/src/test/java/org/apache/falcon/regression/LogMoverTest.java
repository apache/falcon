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

import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Properties;
import org.apache.falcon.entity.v0.process.Property;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.*;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

/**
 * LogMover Test.
 * Adds job launcher success/failure logs to falcon staging directory.
 * It is not working for map-reduce actions(FALCON-1038).
 * Using pig-action to test this feature.
 */
@Test(groups = "embedded")
public class LogMoverTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private String pigTestDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = cleanAndGetTestDir() + "/aggregator";
    private String inputPath = pigTestDir + "/input" + MINUTE_DATE_PATTERN;
    private String propPath = pigTestDir + "/LogMover";
    private static final Logger LOGGER = Logger.getLogger(LogMoverTest.class);
    private String processName;
    private String process;
    private String startDate;
    private String endDate;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        LOGGER.info("in @BeforeMethod");
        startDate = TimeUtil.getTimeWrtSystemTime(-3);
        endDate = TimeUtil.getTimeWrtSystemTime(3);

        LOGGER.info("startDate : " + startDate + " , endDate : " + endDate);
        //copy pig script and workflow
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES + "LogMover");
        Bundle bundle = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundle, cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].setInputFeedDataPath(inputPath);
        bundles[0].setInputFeedPeriodicity(1, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(pigTestDir + "/output-data" + MINUTE_DATE_PATTERN);
        bundles[0].setOutputFeedAvailabilityFlag("_SUCCESS");
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setProcessInputNames("INPUT");
        bundles[0].setProcessOutputNames("OUTPUT");
        bundles[0].setProcessValidity(startDate, endDate);
        bundles[0].setProcessPeriodicity(1, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(1, TimeUnit.minutes);

        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(startDate, endDate, 20);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT,
                bundles[0].getFeedDataPathPrefix(), dataDates);

        // Defining path to be used in pig script
        final Process processElement = bundles[0].getProcessObject();
        final Properties properties = new Properties();
        final Property property = new Property();
        property.setName("inputPath");
        property.setValue(propPath);
        properties.getProperties().add(property);
        processElement.setProperties(properties);
        bundles[0].setProcessData(processElement.toString());
        process = bundles[0].getProcessData();
        processName = Util.readEntityName(process);

    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    /**
     *Schedule a process. It should succeed and job launcher success information
     * should be present in falcon staging directory.
     */
    @Test(groups = {"singleCluster"})
    public void logMoverSucceedTest() throws Exception {
        bundles[0].submitFeedsScheduleProcess(prism);
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, process, Job.Status.RUNNING);

        //Copy data to let pig job succeed
        HadoopUtil.copyDataToFolder(clusterFS, propPath, OSUtil.RESOURCES + "pig");

        InstanceUtil.waitTillInstancesAreCreated(cluster, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, bundles[0].getProcessName(), 1,
                CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        Assert.assertTrue(validate(true), "Success logs are not present");
    }

    /**
     *Schedule a process. It should fail and job launcher failure information
     * should be present in falcon staging directory.
     */
    @Test(groups = {"singleCluster"})
    public void logMoverFailTest() throws Exception {
        bundles[0].submitFeedsScheduleProcess(prism);
        AssertUtil.checkStatus(clusterOC, EntityType.PROCESS, process, Job.Status.RUNNING);

        InstanceUtil.waitTillInstancesAreCreated(cluster, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, bundles[0].getProcessName(), 1,
                        CoordinatorAction.Status.KILLED, EntityType.PROCESS);

        Assert.assertTrue(validate(false), "Filed logs are not present");
    }

    private boolean validate(boolean logFlag) throws Exception {
        String stagingDir= MerlinConstants.STAGING_LOCATION;
        String path=stagingDir+"/falcon/workflows/process/"+processName+"/logs";
        List<Path> logmoverPaths = HadoopUtil
                .getAllFilesRecursivelyHDFS(clusterFS, new Path(HadoopUtil.cutProtocol(path)));
        if (logFlag) {
            for (Path logmoverPath : logmoverPaths) {
                if (logmoverPath.toString().contains("SUCCEEDED")) {
                    return true;
                }
            }
        } else {
            for (Path logmoverPath : logmoverPaths) {
                if (logmoverPath.toString().contains("FAILED")) {
                    return true;
                }
            }
        }
        return false;
    }

}
