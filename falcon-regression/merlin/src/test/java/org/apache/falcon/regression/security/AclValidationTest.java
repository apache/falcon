/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.falcon.regression.security;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.falcon.regression.Entities.ClusterMerlin;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Method;

/**
 * Tests if ACL info is consistent with user submitting the entity
 */
@Test(groups = "authorization")
public class AclValidationTest extends BaseTestClass {
    private static final Logger LOGGER = Logger.getLogger(AclValidationTest.class);

    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private String baseTestDir = baseHDFSDir + "/AuthorizationTest";
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private String feedInputPath = baseTestDir + "/input" + MINUTE_DATE_PATTERN;

    ClusterMerlin clusterMerlin;
    FeedMerlin feedMerlin;
    ProcessMerlin processMerlin;

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup(Method method) throws Exception {
        LOGGER.info("test name: " + method.getName());
        Bundle bundle = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundle, cluster);
        bundles[0].generateUniqueBundle();
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        clusterMerlin = bundles[0].getClusterElement();
        feedMerlin = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        processMerlin = bundles[0].getProcessObject();
    }

    /**
     * Test a cluster's acl validations for different aclOwner and aclGroup
     * @param aclOwner owner for the acl
     * @param aclGroup group for the acl
     * @throws Exception
     */
    @Test(dataProvider = "generateUserAndGroup")
    public void submitClusterBadAcl(String aclOwner, String aclGroup) throws Exception {
        clusterMerlin.setACL(aclOwner,aclGroup, "*");
        final ServiceResponse serviceResponse =
            prism.getClusterHelper().submitEntity(clusterMerlin.toString());
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
            "Cluster's ACL owner should be same as submitting user");
    }

    /**
     * Test a feed's acl validations for different aclOwner and aclGroup
     * @param aclOwner owner for the acl
     * @param aclGroup group for the acl
     * @throws Exception
     */
    @Test(dataProvider = "generateUserAndGroup")
    public void submitFeedBadAcl(String aclOwner, String aclGroup) throws Exception {
        bundles[0].submitClusters(prism);
        feedMerlin.setACL(aclOwner, aclGroup, "*");
        final ServiceResponse serviceResponse =
            prism.getFeedHelper().submitEntity(feedMerlin.toString());
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
            "Feed's ACL owner should be same as submitting user");
    }

    /**
     * Test a process's acl validations for different aclOwner and aclGroup
     * @param aclOwner owner for the acl
     * @param aclGroup group for the acl
     * @throws Exception
     */
    @Test(dataProvider = "generateUserAndGroup")
    public void submitProcessBadAcl(String aclOwner, String aclGroup) throws Exception {
        bundles[0].submitAndScheduleAllFeeds();
        processMerlin.setACL(aclOwner, aclGroup, "*");
        final ServiceResponse serviceResponse =
            prism.getProcessHelper().submitEntity(processMerlin.toString());
        AssertUtil.assertFailedWithStatus(serviceResponse, HttpStatus.SC_BAD_REQUEST,
            "Process's ACL owner should be same as submitting user");
    }

    @DataProvider(name = "generateUserAndGroup")
    public Object[][] generateUserAndGroup() {
        return new Object[][] {
            {MerlinConstants.CURRENT_USER_NAME, MerlinConstants.DIFFERENT_USER_GROUP},
            {MerlinConstants.CURRENT_USER_NAME, "nonexistinggroup"},
            {"nonexistinguser", MerlinConstants.CURRENT_USER_GROUP},
        };
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeBundles();
    }

    @AfterClass(alwaysRun = true)
    public void tearDownClass() throws IOException {
        cleanTestDirs();
    }
}
