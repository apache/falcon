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

import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.KerberosHelper;
import org.apache.falcon.regression.core.util.MathUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Method;

@Test(groups = "embedded")
public class ProcessAclTest extends BaseTestClass {
    private static final Logger LOGGER = Logger.getLogger(ProcessAclTest.class);

    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private String baseTestDir = baseHDFSDir + "/ProcessAclTest";
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private String feedInputPath = baseTestDir + "/input" + MINUTE_DATE_PATTERN;
    private final IEntityManagerHelper processHelper = prism.getProcessHelper();
    String processString;

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
        bundles[0].setProcessACL(MerlinConstants.CURRENT_USER_NAME,
            MerlinConstants.CURRENT_USER_GROUP, "*");
        processString = bundles[0].getProcessData();

        KerberosHelper.loginFromKeytab(MerlinConstants.CURRENT_USER_NAME);
    }

    /**
     * Test process read operation by different types of user
     * @param user the user that would attempt operation
     * @param op the read operation that would be performed
     * @param isAllowed is operation expected to go through
     * @throws Exception
     */
    @Test(dataProvider = "generateUserReadOpsPermissions")
    public void othersReadProcess(final String user, final EntityOp op, final boolean isAllowed)
        throws Exception {
        bundles[0].submitProcess(true);
        final boolean executeRes = op.executeAs(user, processHelper, processString);
        Assert.assertEquals(executeRes, isAllowed, "Unexpected result user " + user +
            " performing: " + op);
    }

    @DataProvider(name = "generateUserReadOpsPermissions")
    public Object[][] generateUserReadOpsPermissions() {
        final EntityOp[] falconReadOps = {EntityOp.status, EntityOp.dependency,
            EntityOp.listing, EntityOp.definition};
        final Object[][] allowedCombinations = MathUtil.crossProduct(
            new String[]{MerlinConstants.ADMIN_USER_NAME, MerlinConstants.ADMIN2_USER_NAME,
                MerlinConstants.USER2_NAME},
            falconReadOps,
            new Boolean[]{true}
        );

        final Object[][] notAllowedCombinations = MathUtil.crossProduct(
            new String[]{MerlinConstants.DIFFERENT_USER},
            falconReadOps,
            new Boolean[]{false}
        );

        return MathUtil.append(allowedCombinations, notAllowedCombinations);
    }

    /**
     * Test edit operation on submitted process by different users
     * @param user the user that would attempt operation
     * @param op the edit operation that would be performed
     * @param isAllowed is operation expected to go through
     * @throws Exception
     */
    @Test(dataProvider = "generateUserSubmittedEditOpsPermission")
    public void othersEditSubmittedProcess(final String user, final EntityOp op, boolean isAllowed)
        throws Exception {
        bundles[0].submitProcess(true);
        if (op == EntityOp.update) {
            ProcessMerlin processMerlin = new ProcessMerlin(processString);
            processMerlin.setProperty("abc", "xyz");
            processString = processMerlin.toString();
        }
        final boolean executeRes = op.executeAs(user, processHelper, processString);
        Assert.assertEquals(executeRes, isAllowed, "Unexpected result user " + user +
            " performing: " + op);
    }

    @DataProvider(name = "generateUserSubmittedEditOpsPermission")
    public Object[][] generateUserSubmittedEditOpsPermission() {
        final EntityOp[] falconEditOps = {EntityOp.submit, EntityOp.delete, EntityOp.update,
            EntityOp.schedule, EntityOp.submitAndSchedule};

        final Object[][] allowedCombinations = MathUtil.crossProduct(
            new String[]{MerlinConstants.ADMIN_USER_NAME, MerlinConstants.ADMIN2_USER_NAME,
                MerlinConstants.USER2_NAME},
            falconEditOps,
            new Boolean[]{true}
        );

        final Object[][] notAllowedCombinations = MathUtil.crossProduct(
            new String[]{MerlinConstants.DIFFERENT_USER},
            falconEditOps,
            new Boolean[]{false}
        );

        return MathUtil.append(allowedCombinations, notAllowedCombinations);
    }

    /**
     * Test edit operation on scheduled process by different users
     * @param user the user that would attempt operation
     * @param op the edit operation that would be performed
     * @param isAllowed is operation expected to go through
     * @throws Exception
     */
    @Test(dataProvider = "generateUserScheduledEditOpsPermission")
    public void othersEditScheduledProcess(final String user, final EntityOp op, boolean isAllowed)
        throws Exception {
        bundles[0].submitFeedsScheduleProcess();
        if(op == EntityOp.resume) {
            processHelper.suspend(processString);
        } else if (op == EntityOp.update) {
            ProcessMerlin processMerlin = new ProcessMerlin(processString);
            processMerlin.setProperty("abc", "xyz");
            processString = processMerlin.toString();
        }
        final boolean executeRes = op.executeAs(user, processHelper, processString);
        Assert.assertEquals(executeRes, isAllowed, "Unexpected result user " + user +
            " performing: " + op);
    }

    @DataProvider(name = "generateUserScheduledEditOpsPermission")
    public Object[][] generateUserScheduledEditOpsPermission() {

        final Object[][] allowedCombinations = MathUtil.crossProduct(
            new String[]{MerlinConstants.ADMIN_USER_NAME, MerlinConstants.ADMIN2_USER_NAME,
                MerlinConstants.USER2_NAME},
            new EntityOp[]{EntityOp.delete, EntityOp.update, EntityOp.suspend, EntityOp.resume},
            new Boolean[]{true}
        );

        final Object[][] notAllowedCombinations = MathUtil.crossProduct(
            new String[]{MerlinConstants.DIFFERENT_USER},
            new EntityOp[]{EntityOp.delete, EntityOp.update, EntityOp.schedule,
                EntityOp.submitAndSchedule, EntityOp.suspend, EntityOp.resume},
            new Boolean[]{false}
        );

        return MathUtil.append(allowedCombinations, notAllowedCombinations);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        KerberosHelper.loginFromKeytab(MerlinConstants.CURRENT_USER_NAME);
        removeBundles();
    }

    @AfterClass(alwaysRun = true)
    public void tearDownClass() throws IOException {
        cleanTestDirs();
    }
}
