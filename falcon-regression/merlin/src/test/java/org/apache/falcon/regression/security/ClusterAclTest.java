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

package org.apache.falcon.regression.security;

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.entity.AbstractEntityHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.MatrixUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests ACL of cluster with different operations.
 */
@Test(groups = "authorization")
public class ClusterAclTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private String baseTestDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private String feedInputPath = baseTestDir + "/input" + MINUTE_DATE_PATTERN;
    private final AbstractEntityHelper clusterHelper = prism.getClusterHelper();
    private String clusterString;

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        Bundle bundle = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundle, cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setCLusterACL(MerlinConstants.CURRENT_USER_NAME,
            MerlinConstants.CURRENT_USER_GROUP, "*");
        clusterString = bundles[0].getClusters().get(0);
    }

    /**
     * Test cluster read operations by different users.
     * @param user the user that would perform operation
     * @param op the operation that user would perform
     * @param isAllowed is the user allowed that operation
     * @throws Exception
     */
    @Test(dataProvider = "generateUserReadOpsPermissions")
    public void othersReadCluster(final String user, EntityOp op, final boolean isAllowed)
        throws Exception {
        bundles[0].submitClusters(prism);
        bundles[0].submitFeeds(prism);
        final boolean executeRes = op.executeAs(user, clusterHelper, clusterString);
        Assert.assertEquals(executeRes, isAllowed, "Unexpected result user " + user
            + " performing: " + op);
    }

    @DataProvider(name = "generateUserReadOpsPermissions")
    public Object[][] generateUserReadOpsPermissions() {
        final Object[][] allowedCombinations = MatrixUtil.crossProduct(
            new String[]{
                MerlinConstants.FALCON_SUPER_USER_NAME,
                MerlinConstants.FALCON_SUPER_USER2_NAME,
                MerlinConstants.USER2_NAME,
            },
            new EntityOp[]{
                EntityOp.dependency,
                EntityOp.listing,
                EntityOp.definition,
            },
            new Boolean[]{true}
        );

        final Object[][] notAllowedCombinations = MatrixUtil.crossProduct(
            new String[]{MerlinConstants.DIFFERENT_USER_NAME},
            new EntityOp[]{EntityOp.dependency, EntityOp.listing, EntityOp.definition},
            new Boolean[]{false}
        );

        return MatrixUtil.append(allowedCombinations, notAllowedCombinations);
    }

    /**
     * Test cluster deletion by different users.
     * @param deleteUser the user that would attempt to delete
     * @param deleteAllowed is delete expected to go through
     * @throws Exception
     */
    @Test(dataProvider = "generateUserAndDeletePermission")
    public void othersDeleteCluster(final String deleteUser, final boolean deleteAllowed)
        throws Exception {
        bundles[0].submitClusters(prism);
        final ServiceResponse response = clusterHelper.delete(clusterString, deleteUser);
        if (deleteAllowed) {
            AssertUtil.assertSucceeded(response);
        } else {
            AssertUtil.assertFailedWith403(response);
        }
    }

    @DataProvider(name = "generateUserAndDeletePermission")
    public Object[][] generateUserAndDeletePermission() {
        return new Object[][] {
            //first element is username, second element indicates if deletion is allowed
            {MerlinConstants.FALCON_SUPER_USER_NAME, true},
            {MerlinConstants.FALCON_SUPER_USER2_NAME, true},
            {MerlinConstants.USER2_NAME, true},
            {"root", false},
        };
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }
}
