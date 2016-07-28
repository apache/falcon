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

package org.apache.falcon.resource;

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.extensions.ExtensionProperties;
import org.apache.falcon.extensions.mirroring.hdfs.HdfsMirroringExtensionProperties;
import org.apache.falcon.extensions.store.AbstractTestExtensionStore;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * IT tests for org.apache.falcon.extensions.ExtensionManager.
 */
@Test (enabled = false)
public class ExtensionManagerIT extends AbstractTestExtensionStore {
    private static final String HDFS_MIRRORING_PROPERTY_TEMPLATE = "/hdfs-mirroring-property-template.txt";
    private static final String JOB_NAME_1 = "hdfs-mirroring-job-1";
    private static final String JOB_NAME_2 = "hdfs-mirroring-job-2";
    private static final String CLUSTER_NAME = "primaryCluster";
    private static final String START_TIME_1 = "2016-05-03T00:00Z";
    private static final String START_TIME_2 = "2016-05-01T00:00Z";
    private static final String FREQUENCY = "days(1)";
    private static final String SOURCE_DIR = "/apps/falcon/demo/input-{year}-{month}-{day}";
    private static final String TARGET_DIR = "/apps/falcon/demo/output-{year}-{month}-{day}";

    private final TestContext context = new TestContext();

    @BeforeClass
    public void prepare() throws Exception {
        TestContext.prepare();
        initExtensionStore(TestContext.class);
    }

    @AfterClass
    public void tearDown() throws Exception {
        TestContext.deleteEntitiesFromStore();
    }

    @Test
    public void testTrustedExtensionJob() throws Exception {
        Map<String, String> overlay = context.getUniqueOverlay();
        String endTime = context.getProcessEndTime();

        // submit cluster
        overlay.put("cluster", CLUSTER_NAME);
        String filePath = TestContext.overlayParametersOverTemplate(TestContext.CLUSTER_TEMPLATE, overlay);
        System.out.println("entity -submit -type cluster -file " + filePath);
        Assert.assertEquals(TestContext.executeWithURL("entity -submit -type cluster -file " + filePath), 0);

        // submit
        System.out.println(getHDFSMirroringProperty(JOB_NAME_1, START_TIME_1, endTime).toString());
        filePath = TestContext.overlayParametersOverTemplate(
                TestContext.getTempFile("property", "txt"),
                HDFS_MIRRORING_PROPERTY_TEMPLATE,
                getHDFSMirroringProperty(JOB_NAME_1, START_TIME_1, endTime));
        System.out.println("extension -submit -extensionName hdfs-mirroring -file " + filePath);
        Assert.assertEquals(TestContext.executeWithURL(
                "extension -submit -extensionName hdfs-mirroring -file " + filePath), 0);

        // schedule
        System.out.println("extension -schedule -jobName " + JOB_NAME_1);
        Assert.assertEquals(TestContext.executeWithURL("extension -schedule -jobName " + JOB_NAME_1), 0);

        // submit and schedule
        filePath = TestContext.overlayParametersOverTemplate(
                TestContext.getTempFile("property", "txt"),
                HDFS_MIRRORING_PROPERTY_TEMPLATE,
                getHDFSMirroringProperty(JOB_NAME_2, START_TIME_1, endTime));
        System.out.println("extension -submitAndSchedule -extensionName hdfs-mirroring -file " + filePath);
        Assert.assertEquals(TestContext.executeWithURL(
                "extension -submitAndSchedule -extensionName hdfs-mirroring -file " + filePath), 0);

        // list extension jobs
        System.out.println("extension -list -extensionName hdfs-mirroring -fields status,clusters,tags");
        Assert.assertEquals(TestContext.executeWithURL(
                "extension -list -extensionName hdfs-mirroring -fields status,clusters,tags"), 0);

        // validate job list results
        ExtensionJobList jobs = context.getExtensionJobs("hdfs-mirroring", null, null, null, null, null);
        Assert.assertEquals(jobs.numJobs, 2);
        Assert.assertEquals(jobs.job.get(0).jobName, JOB_NAME_1);
        Assert.assertEquals(jobs.job.get(1).jobName, JOB_NAME_2);

        // list extension job instances
        System.out.println("extension -instances -jobName " + JOB_NAME_1 + " -fields status,clusters,tags");
        Assert.assertEquals(TestContext.executeWithURL(
                "extension -instances -jobName " + JOB_NAME_1 + " -fields status,clusters,tags"), 0);
        System.out.println("extension -instances -jobName " + JOB_NAME_2 + " -fields status,clusters,tags");
        Assert.assertEquals(TestContext.executeWithURL(
                "extension -instances -jobName " + JOB_NAME_2 + " -fields status,clusters,tags"), 0);

        // validate instance list results
        context.waitForInstancesToStart(EntityType.PROCESS.name(), JOB_NAME_1, 10000);
        ExtensionInstanceList instanceList = context.getExtensionInstances(JOB_NAME_1, START_TIME_1, endTime, "RUNNING",
                null, null, null, null, null, null);
        System.out.println("Validate running instances of extension job " + JOB_NAME_1 + ": \n"
                + instanceList.toString());
        Assert.assertEquals(instanceList.numEntities, 1);
        Assert.assertEquals(instanceList.entitySummary.get(0).instances.length, 1);

        context.waitForInstancesToStart(EntityType.PROCESS.name(), JOB_NAME_2, 10000);
        instanceList = context.getExtensionInstances(JOB_NAME_2, START_TIME_1, endTime, "RUNNING",
                null, null, null, null, null, null);
        System.out.println("Validate running instances of extension job " + JOB_NAME_2 + ": \n"
                + instanceList.toString());
        Assert.assertEquals(instanceList.numEntities, 1);
        Assert.assertEquals(instanceList.entitySummary.get(0).instances.length, 1);

        // suspend
        System.out.println("extension -suspend -jobName " + JOB_NAME_1);
        Assert.assertEquals(TestContext.executeWithURL("extension -suspend -jobName " + JOB_NAME_1), 0);

        // resume
        System.out.println("extension -resume -jobName " + JOB_NAME_1);
        Assert.assertEquals(TestContext.executeWithURL("extension -resume -jobName " + JOB_NAME_1), 0);

        // delete
        System.out.println("extension -delete -jobName " + JOB_NAME_1);
        Assert.assertEquals(TestContext.executeWithURL("extension -delete -jobName " + JOB_NAME_1), 0);

        // update
        filePath = TestContext.overlayParametersOverTemplate(
                TestContext.getTempFile("property", "txt"),
                HDFS_MIRRORING_PROPERTY_TEMPLATE,
                getHDFSMirroringProperty(JOB_NAME_2, START_TIME_2, endTime));
        System.out.println("extension -update -extensionName hdfs-mirroring -file " + filePath);
        Assert.assertEquals(TestContext.executeWithURL(
                "extension -update -extensionName hdfs-mirroring -file " + filePath), 0);

        // validate
        System.out.println("extension -validate -extensionName hdfs-mirroring -file " + filePath);
        Assert.assertEquals(TestContext.executeWithURL(
                "extension -validate -extensionName hdfs-mirroring -file " + filePath), 0);

        // failure case: no file input
        System.out.println("extension -submitAndSchedule -extensionName hdfs-mirroring");
        Assert.assertEquals(TestContext.executeWithURL(
                "extension -submitAndSchedule -extensionName hdfs-mirroring"), -1);
    }

    private Map<String, String> getHDFSMirroringProperty(String jobName, String start, String end) {
        Map<String, String> properties = new HashMap<>();
        properties.put(ExtensionProperties.JOB_NAME.getName(), jobName);
        properties.put(ExtensionProperties.CLUSTER_NAME.getName(), CLUSTER_NAME);
        properties.put(ExtensionProperties.VALIDITY_START.getName(), start);
        properties.put(ExtensionProperties.VALIDITY_END.getName(), end);
        properties.put(ExtensionProperties.FREQUENCY.getName(), FREQUENCY);
        properties.put(HdfsMirroringExtensionProperties.SOURCE_DIR.getName(), SOURCE_DIR);
        properties.put(HdfsMirroringExtensionProperties.SOURCE_CLUSTER.getName(), CLUSTER_NAME);
        properties.put(HdfsMirroringExtensionProperties.TARGET_DIR.getName(), TARGET_DIR);
        properties.put(HdfsMirroringExtensionProperties.TARGET_CLUSTER.getName(), CLUSTER_NAME);
        return properties;
    }
}
