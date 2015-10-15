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
import org.apache.falcon.regression.core.supportClasses.ExecResult;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.KerberosHelper;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests falcon client's working in presence of ACL errors.
 */
@Test(groups = "authorization")
public class FalconClientTest extends BaseTestClass {

    private final ColoHelper cluster = servers.get(0);
    private final FileSystem clusterFS = serverFS.get(0);
    private final String baseTestDir = cleanAndGetTestDir();
    private final String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private final String feedInputPath = baseTestDir + "/input" + MINUTE_DATE_PATTERN;

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
    }

    /**
     * Test error thrown by falcon client, when acl of the submitted cluster has bad values.
     * @throws Exception
     */
    @Test (enabled = true)
    public void badClusterSubmit() throws Exception {
        bundles[0].setCLusterACL(MerlinConstants.DIFFERENT_USER_NAME,
            MerlinConstants.CURRENT_USER_GROUP, "*");
        final String clusterXml = bundles[0].getClusters().get(0);
        final ExecResult execResult = prism.getClusterHelper().clientSubmit(clusterXml);
        AssertUtil.assertFailed(execResult, String.format(
            "Invalid acl owner %s, does not exist or does not belong to group: %s",
            MerlinConstants.DIFFERENT_USER_NAME, MerlinConstants.CURRENT_USER_GROUP));
    }

    /**
     * Test error thrown by falcon client, a user tries to delete a cluster that it should not be.
     * able to delete
     * @throws Exception
     */
    @Test(enabled = true)
    public void badClusterDelete() throws Exception {
        bundles[0].submitClusters(prism);
        //switch user
        if (MerlinConstants.IS_SECURE) {
            KerberosHelper.initUserWithKeytab(MerlinConstants.DIFFERENT_USER_NAME);
        }
        final String clusterXml = bundles[0].getClusters().get(0);
        final ExecResult execResult =
            prism.getClusterHelper().clientDelete(clusterXml, MerlinConstants.DIFFERENT_USER_NAME);
        AssertUtil.assertFailed(execResult, "ERROR: Forbidden;");
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }
}
