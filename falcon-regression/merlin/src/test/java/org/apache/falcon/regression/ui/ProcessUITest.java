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

package org.apache.falcon.regression.ui;

import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Inputs;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.Generator;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.lineage.LineageApiTest;
import org.apache.falcon.regression.testHelper.BaseUITestClass;
import org.apache.falcon.regression.ui.pages.EntitiesPage;
import org.apache.falcon.regression.ui.pages.Page;
import org.apache.falcon.regression.ui.pages.ProcessPage;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * UI tests with submitted/scheduled process.
 */
@Test(groups = "lineage-ui")
public class ProcessUITest extends BaseUITestClass {

    private ColoHelper cluster = servers.get(0);
    private String baseTestDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private static final Logger LOGGER = Logger.getLogger(ProcessUITest.class);
    private final String feedInputPath = baseTestDir + "/input";
    private final String feedOutputPath = baseTestDir + "/output";
    private FileSystem clusterFS = serverFS.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private SoftAssert softAssert = new SoftAssert();

    @BeforeClass
    public void setUp()
        throws IOException, JAXBException, NoSuchMethodException, IllegalAccessException,
        InvocationTargetException, URISyntaxException, AuthenticationException,
        InterruptedException {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        openBrowser();
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        String startTime = TimeUtil.getTimeWrtSystemTime(0);
        String endTime = TimeUtil.addMinsToTime(startTime, 5);
        LOGGER.info("Start time: " + startTime + "\tEnd time: " + endTime);

        //prepare process definition
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessPeriodicity(1, Frequency.TimeUnit.minutes);
        bundles[0].setProcessConcurrency(5);
        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.minutes);
        bundles[0].setInputFeedDataPath(feedInputPath + MINUTE_DATE_PATTERN);
        Process process = bundles[0].getProcessObject();
        Inputs inputs = new Inputs();
        Input input = new Input();
        input.setFeed(Util.readEntityName(bundles[0].getInputFeedFromBundle()));
        input.setStart("now(0,0)");
        input.setEnd("now(0,4)");
        input.setName("inputData");
        inputs.getInputs().add(input);
        process.setInputs(inputs);

        bundles[0].setProcessData(process.toString());

        //provide necessary data for first 3 instances to run
        LOGGER.info("Creating necessary data...");
        String prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);
        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(
            TimeUtil.addMinsToTime(startTime, -2), endTime, 0);

        // use 5 <= x < 10 input feeds
        final int numInputFeeds = 5 + new Random().nextInt(5);
        // use 5 <= x < 10 output feeds
        final int numOutputFeeds = 5 + new Random().nextInt(5);

        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, prefix, dataDates);

        prefix = prefix.substring(0, prefix.length()-1);
        for (int k = 1; k <= numInputFeeds; k++) {
            HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT,
                prefix + "_00" + k + "/", dataDates);
        }

        LOGGER.info("Process data: " + Util.prettyPrintXml(bundles[0].getProcessData()));
        FeedMerlin[] inputFeeds;
        FeedMerlin[] outputFeeds;
        final FeedMerlin inputMerlin = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        final FeedMerlin outputMerlin = new FeedMerlin(bundles[0].getOutputFeedFromBundle());

        String namePrefix = Util.getEntityPrefix(this) + '-';
        inputFeeds = LineageApiTest.generateFeeds(numInputFeeds, inputMerlin,
                Generator.getNameGenerator(namePrefix + "infeed",
                    inputMerlin.getName().replace(namePrefix, "")),
                Generator.getHadoopPathGenerator(feedInputPath, MINUTE_DATE_PATTERN));
        int j = 0;
        for (FeedMerlin feed : inputFeeds) {
            bundles[0].addInputFeedToBundle("inputFeed" + j++, feed);
        }

        outputFeeds = LineageApiTest.generateFeeds(numOutputFeeds, outputMerlin,
                Generator.getNameGenerator(namePrefix + "outfeed",
                    inputMerlin.getName().replace(namePrefix, "")),
                Generator.getHadoopPathGenerator(feedOutputPath, MINUTE_DATE_PATTERN));
        j = 0;
        for (FeedMerlin feed : outputFeeds) {
            bundles[0].addOutputFeedToBundle("outputFeed" + j++, feed);
        }

        AssertUtil.assertSucceeded(bundles[0].submitBundle(prism));

    }


    @AfterClass(alwaysRun = true)
    public void tearDown() throws IOException {
        closeBrowser();
        removeTestClassEntities();
    }

    /**
     * Test checks that UI show expected statuses of submitted Process (SUBMITTED and RUNNING)
     * then checks instances icons to be relevant to statuses of oozie actions
     * and checks that Lineage links are available only for SUCCEEDED instances.
     */
    @Test
    public void testProcessUI()
        throws URISyntaxException, IOException, AuthenticationException, JAXBException,
        OozieClientException, InterruptedException {

        //check Process statuses via UI
        EntitiesPage page = new EntitiesPage(getDriver(), cluster, EntityType.PROCESS);
        page.navigateTo();
        String process = bundles[0].getProcessData();
        String processName = Util.readEntityName(process);
        softAssert.assertEquals(page.getEntityStatus(processName),
                Page.EntityStatus.SUBMITTED, "Process status should be SUBMITTED");
        prism.getProcessHelper().schedule(process);

        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS);

        softAssert.assertEquals(page.getEntityStatus(processName),
                Page.EntityStatus.RUNNING, "Process status should be RUNNING");

        ProcessPage processPage = new ProcessPage(getDriver(), cluster, processName);
        processPage.navigateTo();

        String bundleID = OozieUtil.getLatestBundleID(clusterOC, processName, EntityType.PROCESS);
        Map<Date, CoordinatorAction.Status> actions = OozieUtil.getActionsNominalTimeAndStatus(
            clusterOC, bundleID,   EntityType.PROCESS);
        checkActions(actions, processPage);

        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1,
            CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);

        processPage.refresh();
        actions = OozieUtil.getActionsNominalTimeAndStatus(clusterOC, bundleID, EntityType.PROCESS);
        checkActions(actions, processPage);

        softAssert.assertAll();
    }

    private void checkActions(Map<Date, CoordinatorAction.Status> actions, ProcessPage page) {
        for(Date date : actions.keySet()) {
            String oozieDate = TimeUtil.dateToOozieDate(date);
            String status = page.getInstanceStatus(oozieDate);
            //checks instances icons to be relevant to statuses of oozie actions
            softAssert.assertNotNull(status, oozieDate + " instance is not present on UI");
            softAssert.assertEquals(status, actions.get(date).toString(), "Status of instance '"
                    + oozieDate + "' is not the same via oozie and via UI");

            //check that Lineage links are available only for SUCCEEDED instances
            boolean isPresent = page.isLineageLinkPresent(oozieDate);
            if (actions.get(date) == CoordinatorAction.Status.SUCCEEDED) {
                softAssert.assertTrue(isPresent, "Lineage button should be present for instance: " + oozieDate);
            } else {
                softAssert.assertFalse(isPresent, "Lineage button should not be present for instance: " + oozieDate);
            }
        }
    }
}
