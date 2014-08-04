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
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.InstancesResult;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;

/**
 * tests for instance option params.
 */
public class InstanceParamTest extends BaseTestClass {

    /**
     * test cases for https://issues.apache.org/jira/browse/FALCON-263.
     */

    private String baseTestHDFSDir = baseHDFSDir + "/InstanceParamTest";
    private String feedInputPath = baseTestHDFSDir
            +
        "/testInputData/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private String startTime;
    private String endTime;

    private ColoHelper cluster1 = servers.get(0);
    private OozieClient oC1 = serverOC.get(0);
    private Bundle processBundle;
    private static final Logger LOGGER = Logger.getLogger(InstanceParamTest.class);


    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        startTime = TimeUtil.get20roundedTime(TimeUtil
            .getTimeWrtSystemTime(-20));
        endTime = TimeUtil.getTimeWrtSystemTime(60);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws Exception {
        LOGGER.info("test name: " + method.getName());
        processBundle = BundleUtil.readELBundle();
        processBundle = new Bundle(processBundle, cluster1);
        processBundle.generateUniqueBundle();
        processBundle.setInputFeedDataPath(feedInputPath);
        processBundle.setProcessWorkflow(aggregateWorkflowDir);
        for (int i = 0; i < 3; i++) {
            bundles[i] = new Bundle(processBundle, servers.get(i));
            bundles[i].generateUniqueBundle();
            bundles[i].setProcessWorkflow(aggregateWorkflowDir);
        }
    }
    @Test(timeOut = 1200000, enabled = false)
    public void getParamsValidRequestInstanceWaiting()
        throws URISyntaxException, JAXBException, AuthenticationException, IOException,
        OozieClientException {
        processBundle.setProcessValidity(startTime, endTime);
        processBundle.addClusterToBundle(bundles[1].getClusters().get(0),
            ClusterType.SOURCE, null, null);
        processBundle.addClusterToBundle(bundles[2].getClusters().get(0),
            ClusterType.SOURCE, null, null);
        processBundle.submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(cluster1, processBundle.getProcessData(), 0);
        InstancesResult r = prism.getProcessHelper()
            .getInstanceParams(Util.readEntityName(processBundle.getProcessData()),
                "?start="+startTime);
        r.getMessage();
    }

    @Test(timeOut = 1200000, enabled = true)
    public void getParamsValidRequestInstanceSucceeded()
        throws URISyntaxException, JAXBException, AuthenticationException, IOException,
        OozieClientException {
        processBundle.setProcessValidity(startTime, endTime);
        processBundle.addClusterToBundle(bundles[1].getClusters().get(0),
            ClusterType.SOURCE, null, null);
        processBundle.addClusterToBundle(bundles[2].getClusters().get(0),
            ClusterType.SOURCE, null, null);
        processBundle.submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(cluster1, processBundle.getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster1, EntityType.PROCESS,
            processBundle.getProcessName(), 0);
        InstanceUtil.waitTillInstanceReachState(oC1, processBundle.getProcessName(), 1,
            CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS, 10);
        InstancesResult r = prism.getProcessHelper()
            .getInstanceParams(Util.readEntityName(processBundle.getProcessData()),
                "?start="+startTime);
        LOGGER.info(r.getMessage());
    }

    @Test(timeOut = 1200000, enabled = false)
    public void getParamsValidRequestInstanceKilled()
        throws URISyntaxException, JAXBException, AuthenticationException, IOException,
        OozieClientException {
        processBundle.setProcessValidity(startTime, endTime);
        processBundle.addClusterToBundle(bundles[1].getClusters().get(0),
            ClusterType.SOURCE, null, null);
        processBundle.addClusterToBundle(bundles[2].getClusters().get(0),
            ClusterType.SOURCE, null, null);
        processBundle.submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(cluster1, processBundle.getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster1, EntityType.PROCESS,
            processBundle.getProcessName(), 0);
        InstanceUtil.waitTillInstanceReachState(oC1, processBundle.getProcessName(), 0,
            CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);
        InstancesResult r = prism.getProcessHelper()
            .getInstanceParams(Util.readEntityName(processBundle.getProcessData()),
                "?start="+startTime);
        r.getMessage();

    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        processBundle.deleteBundle(prism);
        removeBundles();
        for (FileSystem fs : serverFS) {
            HadoopUtil.deleteDirIfExists(Util.getPathPrefix(feedInputPath), fs);
        }
    }
}
