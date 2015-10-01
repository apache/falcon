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

import org.apache.commons.io.FileUtils;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.Job.Status;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * Tests with process lib folder with workflow.xml.
 */
@Test(groups = "embedded")
public class ProcessLibPathLoadTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private String testDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = testDir + "/aggregator";
    private static final Logger LOGGER = Logger.getLogger(ProcessLibPathLoadTest.class);

    private String oozieLib = MerlinConstants.OOZIE_EXAMPLE_LIB;
    private String oozieLibName = oozieLib.substring(oozieLib.lastIndexOf('/') + 1);
    private String filename = OSUtil.concat(OSUtil.OOZIE_LIB_FOLDER, "lib", oozieLibName);
    private String processName;
    private String process;

    @BeforeClass(alwaysRun = true)
    public void createTestData() throws Exception {
        LOGGER.info("in @BeforeClass");
        FileUtils.forceMkdir(new File(OSUtil.concat(OSUtil.OOZIE_LIB_FOLDER, "lib")));
        saveUrlToFile(oozieLib);
    }

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.OOZIE_LIB_FOLDER);
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].setInputFeedDataPath(testDir + MINUTE_DATE_PATTERN);
        bundles[0].setProcessValidity("2015-01-02T01:00Z", "2015-01-02T01:04Z");
        bundles[0].setProcessPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedPeriodicity(5, TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(testDir + "/output-data" + MINUTE_DATE_PATTERN);
        bundles[0].setProcessConcurrency(1);
        bundles[0].setProcessLibPath(aggregateWorkflowDir + "/lib");
        process = bundles[0].getProcessData();
        processName = Util.readEntityName(process);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    @AfterClass(alwaysRun = true)
    public void deleteJar() throws IOException {
        File file = new File(filename);
        Assert.assertEquals(file.delete(), true, filename + " is not present.");
        FileUtils.deleteDirectory(new File(OSUtil.concat(OSUtil.OOZIE_LIB_FOLDER, "lib")));
    }

    /**
     * Test which test a process with jar in lib location.
     * Schedule a process, it should succeed.
     *
     * @throws Exception
     */
    @Test
    public void setRightJarInWorkflowLib() throws Exception {
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, process, 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        OozieUtil.waitForBundleToReachState(clusterOC, processName, Status.SUCCEEDED);
    }

    /**
     * Test which test a process with no jar in lib location.
     * Schedule a process, it should get killed.
     *
     * @throws Exception
     */
    @Test
    public void setNoJarInWorkflowLibLocation() throws Exception {
        HadoopUtil.deleteDirIfExists(aggregateWorkflowDir + "/lib/" + oozieLibName, clusterFS);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].submitFeedsScheduleProcess(prism);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, process, 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        OozieUtil.waitForBundleToReachState(clusterOC, processName, Status.KILLED);
    }

    /**
     * Function to download jar at remote public location.
     * @param urlString public location from where jar is to be downloaded
     * filename is the location where the jar is to be saved
     * @throws Exception
     */
    private void saveUrlToFile(String urlString)
        throws IOException {

        URL url = new URL(urlString);
        String link;
        HttpURLConnection http = (HttpURLConnection) url.openConnection();
        Map<String, List<String>> header = http.getHeaderFields();
        while (isRedirected(header)) {
            link = header.get("Location").get(0);
            url = new URL(link);
            http = (HttpURLConnection) url.openConnection();
            header = http.getHeaderFields();
        }

        InputStream input = http.getInputStream();
        byte[] buffer = new byte[4096];
        int n;
        OutputStream output = new FileOutputStream(new File(filename));
        while ((n = input.read(buffer)) != -1) {
            output.write(buffer, 0, n);
        }
        output.close();
    }

    private static boolean isRedirected(Map<String, List<String>> header) {
        for (String hv : header.get(null)) {
            if (hv.contains(" 301 ") || hv.contains(" 302 ")) {
                return true;
            }
        }
        return false;
    }

}
