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

package org.apache.falcon.regression.lineage;

import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.ActionType;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.InstancesResult;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.CleanupUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.EntitySummaryResult;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.asserts.SoftAssert;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Test bulk API for dashboard needs.
 */
@Test(groups = "embedded")
public class EntitySummaryTest extends BaseTestClass {
    private static final Logger LOGGER = Logger.getLogger(EntitySummaryTest.class);
    private ColoHelper cluster1 = servers.get(0);
    private ColoHelper cluster2 = servers.get(1);
    private OozieClient cluster1OC = serverOC.get(0);
    private OozieClient cluster2OC = serverOC.get(1);
    private FileSystem cluster1FS = serverFS.get(0);
    private String testDir = "/EntitySummaryTest";
    private String baseTestHDFSDir = baseHDFSDir + testDir;
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private String sourcePath = baseTestHDFSDir + "/source";
    private String feedDataLocation = baseTestHDFSDir + "/source" + MINUTE_DATE_PATTERN;
    private String targetPath = baseTestHDFSDir + "/target";
    private String targetDataLocation = targetPath + MINUTE_DATE_PATTERN;
    private String startTime, endTime;
    private SoftAssert softAssert;

    @BeforeMethod(alwaysRun = true)
    public void prepareData() throws IOException {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        Bundle bundle = BundleUtil.readELBundle();
        for (int i = 0; i <= 1; i++) {
            bundles[i] = new Bundle(bundle, servers.get(i));
            bundles[i].generateUniqueBundle();
            bundles[i].setProcessWorkflow(aggregateWorkflowDir);
            bundles[i].setInputFeedDataPath(feedDataLocation);
        }
        startTime = TimeUtil.getTimeWrtSystemTime(-35);
        endTime = TimeUtil.getTimeWrtSystemTime(5);
        LOGGER.info("Time range is between : " + startTime + " and " + endTime);
        softAssert = new SoftAssert();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        cleanTestDirs();
        CleanupUtil.cleanAllEntities(prism);
    }

    /**
     * Get status of 7 processes and 7 instances of each process. The call should give correct
     * information, instance info must be recent.
     */
    @Test
    public void getProcessSummary() throws Exception {
        //prepare process
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].submitClusters(prism);
        bundles[0].submitFeeds(prism);
        String clusterName = Util.readEntityName(bundles[0].getClusters().get(0));
        List<String> processes = scheduleEntityValidateWaitingInstances(cluster1,
            bundles[0].getProcessData(), EntityType.PROCESS, clusterName);

        //create data for processes to run and wait some time for instances to make progress
        OozieUtil.createMissingDependencies(cluster1, EntityType.PROCESS, processes.get(0), 0);
        InstanceUtil.waitTillInstanceReachState(cluster1OC, processes.get(0), 1,
            CoordinatorAction.Status.RUNNING, EntityType.PROCESS);

        //compare summary and instance result for each of 7 processes
        validateProgressingInstances(processes, EntityType.PROCESS, clusterName);
        softAssert.assertAll();
    }

    /**
     * Get status of 7 feeds and 7 instances of each feed the call should give correct information,
     * instance info must be recent.
     */
    @Test
    public void getFeedSummary() throws Exception {
        //prepare feed template.
        bundles[0].setInputFeedPeriodicity(5, Frequency.TimeUnit.minutes);
        bundles[0].setInputFeedDataPath(feedDataLocation);
        String feed = bundles[0].getInputFeedFromBundle();
        String cluster1Def = bundles[0].getClusters().get(0);
        String cluster2Def = bundles[1].getClusters().get(0);
        //erase all clusters from feed definition
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity("2012-10-01T12:00Z", "2010-01-01T00:00Z"),
            XmlUtil.createRetention("days(1000000)", ActionType.DELETE), null,
            ClusterType.SOURCE, null);
        //set cluster1 as source
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(startTime, endTime),
            XmlUtil.createRetention("days(1000000)", ActionType.DELETE),
            Util.readEntityName(cluster1Def), ClusterType.SOURCE, null);
        //set cluster2 as target
        feed = InstanceUtil.setFeedCluster(feed,
            XmlUtil.createValidity(startTime, endTime),
            XmlUtil.createRetention("days(1000000)", ActionType.DELETE),
            Util.readEntityName(cluster2Def), ClusterType.TARGET, null, targetDataLocation);
        String clusterName = Util.readEntityName(cluster2Def);

        //submit clusters
        AssertUtil.assertSucceeded(prism.getClusterHelper().submitEntity(cluster1Def));
        AssertUtil.assertSucceeded(prism.getClusterHelper().submitEntity(cluster2Def));

        //submit and schedule 7 feeds, check that 7 waiting instances are present for each feed
        List<String> feeds = scheduleEntityValidateWaitingInstances(cluster2, feed,
            EntityType.FEED, clusterName);

        //create data for processes to run and wait some time for instances to make progress
        List<String> folders = TimeUtil.getMinuteDatesOnEitherSide(TimeUtil.oozieDateToDate(
            startTime), TimeUtil.oozieDateToDate(endTime), 1);
        HadoopUtil.flattenAndPutDataInFolder(cluster1FS, OSUtil.NORMAL_INPUT, sourcePath, folders);
        InstanceUtil.waitTillInstanceReachState(cluster2OC, feeds.get(0), 1,
            CoordinatorAction.Status.RUNNING, EntityType.FEED);

        //compare summary and instance result for each of 7 processes
        validateProgressingInstances(feeds, EntityType.FEED, clusterName);
        softAssert.assertAll();
    }

    /*
     * Schedules 7 entities and checks that summary reflects info about the most recent 7
     * instances of each of them.
     */
    private List<String> scheduleEntityValidateWaitingInstances(ColoHelper cluster, String entity,
                                                                EntityType entityType,
                                                                String clusterName)
            throws AuthenticationException, IOException, URISyntaxException, JAXBException,
            OozieClientException, InterruptedException {
        String entityName = Util.readEntityName(entity);
        IEntityManagerHelper helper;
        List<String> names = new ArrayList<String>();
        for (int i = 1; i <= 7; i++) {
            String uniqueName = entityName + i;
            names.add(uniqueName);
            Entity entityMerlin;
            if (entityType == EntityType.FEED) {
                helper = prism.getFeedHelper();
                entityMerlin = new FeedMerlin(entity);
                ((FeedMerlin) entityMerlin).setName(uniqueName);
            } else {
                helper = prism.getProcessHelper();
                entityMerlin = new ProcessMerlin(entity);
                ((ProcessMerlin) entityMerlin).setName(uniqueName);
            }
            entity = entityMerlin.toString();
            AssertUtil.assertSucceeded(helper.submitAndSchedule(entity));
            InstanceUtil.waitTillInstancesAreCreated(cluster, entity, 0);
            InstanceUtil.waitTillInstanceReachState(cluster.getClusterHelper().getOozieClient(),
                uniqueName, 7, CoordinatorAction.Status.WAITING, entityType);

            //check that summary shows recent (i) number of feeds and their instances
            EntitySummaryResult summary = helper.getEntitySummary(clusterName, null)
                .getEntitySummaryResult();

            EntitySummaryResult.EntitySummary[] entitiesSummary = summary.getEntitySummaries();
            softAssert.assertEquals(entitiesSummary.length, i, "Summary must contain info "
                + "about exact number of feeds.");
            for (EntitySummaryResult.EntitySummary entitySummary : entitiesSummary) {
                String status = entitySummary.getStatus();
                softAssert.assertTrue(status.equals("RUNNING") || status.equals("SUBMITTED"),
                    "Unexpected entity status : " + status);
                softAssert.assertTrue(names.contains(entitySummary.getName()),
                    "Unexpected entity name: " + entitySummary.getName());
                softAssert.assertEquals(entitySummary.getInstances().length, 7,
                    "Unexpected number of instances.");
                for (EntitySummaryResult.Instance instance : entitySummary.getInstances()) {
                    softAssert.assertEquals(instance.getStatus(),
                        EntitySummaryResult.WorkflowStatus.WAITING,
                        "Unexpected instance status.");
                    softAssert.assertEquals(instance.getCluster(), clusterName,
                        "Invalid cluster in summary.");
                }
            }
        }
        return names;
    }

    /*
     * Retrieves the most resent info from instanceStatus and entitySummary API and compares them.
     * Summary for each entity should contain 7 instances and that instances should be the same
     * as in response of instancesResult request.
     */
    private void validateProgressingInstances(List<String> names, EntityType entityType,
                                              String clusterName)
            throws AuthenticationException, IOException, URISyntaxException, InterruptedException {
        InstancesResult r;
        IEntityManagerHelper helper;
        if (entityType == EntityType.FEED) {
            helper = prism.getFeedHelper();
        } else {
            helper = prism.getProcessHelper();
        }
        for (String entityName : names) {
            LOGGER.info("Working with " + entityType + " : " + entityName);
            //get recent instances info by -getStatus API
            r = helper.getProcessInstanceStatus(entityName, null);
            InstancesResult.Instance[] instancesR = r.getInstances();
            LOGGER.info("Instances from InstancesResult: " + Arrays.toString(instancesR));
            //get recent instances info by -summary API
            EntitySummaryResult.EntitySummary[] summaries =
                helper.getEntitySummary(clusterName, null)
                    .getEntitySummaryResult().getEntitySummaries();
            EntitySummaryResult.EntitySummary summaryItem = null;
            //get instances for specific process
            for (EntitySummaryResult.EntitySummary item : summaries) {
                if (item.getName().equals(entityName)) {
                    summaryItem = item;
                    break;
                }
            }
            Assert.assertNotNull(summaryItem, "Appropriate summary not found for : " + entityName);
            EntitySummaryResult.Instance[] instancesS = summaryItem.getInstances();
            LOGGER.info("Instances from SummaryResult: " + Arrays.toString(instancesS));
            softAssert.assertEquals(instancesS.length, 7, "7 instances should be present in "
                + "summary.");
            for (EntitySummaryResult.Instance instance : instancesS) {
                softAssert.assertTrue(containsInstances(instancesR, instance), "Instance "
                    + instance.toString() + " is absent in list : " + Arrays.toString(instancesR));
            }
        }
    }

    /*
     * Checks if array of instances of InstancesResult contains particular instance
     * from SummaryResult by common properties.
     */
    private boolean containsInstances(InstancesResult.Instance[] instancesR,
                                      EntitySummaryResult.Instance instanceS) {
        for (InstancesResult.Instance instanceR : instancesR) {
            if (instanceR.getInstance().equals(instanceS.getInstance())
                && instanceR.getStatus().toString().equals(instanceS.getStatus().toString())
                && instanceR.getCluster().equals(instanceS.getCluster())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Test entitySummary optional params. Schedule few processes and get summary using
     * different optional params. Check that responses match to expected.
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws AuthenticationException
     * @throws JAXBException
     */
    @Test
    public void getSummaryFilterBy()
            throws URISyntaxException, IOException, AuthenticationException, JAXBException,
            InterruptedException {
        //prepare process template
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].submitClusters(prism);
        bundles[0].submitFeeds(prism);
        String clusterName = Util.readEntityName(bundles[0].getClusters().get(0));
        String originName = bundles[0].getProcessName();
        List<String> pNames = new ArrayList<String>();
        //schedule 3 processes with different pipelines. 1st and 3d processes have same tag value.
        for (int i = 1; i <= 3; i++) {
            String uniqueName = originName + "-" + i;
            pNames.add(uniqueName);
            String pipeline = "pipeline_" + i;
            bundles[0].setProcessName(uniqueName);
            bundles[0].setProcessPipeline(pipeline);
            if (i == 2) {
                bundles[0].setProcessTags("value=2");
            } else {
                bundles[0].setProcessTags("value=1");
            }
            String process = bundles[0].getProcessData();
            AssertUtil.assertSucceeded(prism.getProcessHelper().submitAndSchedule(process));
        }

        //test filterBy name option. Use first process name. Expecting 1 summary be returned.
        EntitySummaryResult.EntitySummary[] summaries = prism.getProcessHelper()
            .getEntitySummary(clusterName, "filterBy=NAME:" + pNames.get(0))
            .getEntitySummaryResult().getEntitySummaries();
        softAssert.assertEquals(summaries.length, 1, "There should be single process filtered by name.");
        softAssert.assertEquals(summaries[0].getName(), pNames.get(0), "Invalid process was returned.");

        //suspend one process and test filterBy status option for both running and suspended processes.
        bundles[0].setProcessName(pNames.get(0));
        AssertUtil.assertSucceeded(prism.getProcessHelper().suspend(bundles[0].getProcessData()));
        summaries = prism.getProcessHelper()
            .getEntitySummary(clusterName, "filterBy=STATUS:SUSPENDED")
            .getEntitySummaryResult().getEntitySummaries();
        softAssert.assertEquals(summaries.length, 1, "There should be single SUSPENDED process filtered.");
        softAssert.assertEquals(summaries[0].getName(), pNames.get(0),
            "Summary shows invalid suspended process.");

        //lets use the same request for RUNNING processes, adding orderBy option and
        //sortOrder=asc option, check result
        summaries = prism.getProcessHelper()
            .getEntitySummary(clusterName, "filterBy=STATUS:RUNNING&orderBy=name&sortOrder=asc")
            .getEntitySummaryResult().getEntitySummaries();
        softAssert.assertEquals(summaries.length, 2, "Invalid RUNNING processes number.");
        softAssert.assertEquals(summaries[0].getName(), pNames.get(1), "Another process expected.");
        softAssert.assertEquals(summaries[1].getName(), pNames.get(2), "Another process expected.");

        //lets use the same request adding orderBy option and sortOrder=desc option, check result
        summaries = prism.getProcessHelper()
            .getEntitySummary(clusterName, "filterBy=STATUS:RUNNING&orderBy=name&sortOrder=desc")
            .getEntitySummaryResult().getEntitySummaries();
        softAssert.assertEquals(summaries.length, 2, "Invalid RUNNING processes number.");
        softAssert.assertEquals(summaries[0].getName(), pNames.get(2), "Another process expected.");
        softAssert.assertEquals(summaries[1].getName(), pNames.get(1), "Another process expected.");

        //lets use original request adding numResults option. Single summary should be returned
        summaries = prism.getProcessHelper()
            .getEntitySummary(clusterName, "filterBy=STATUS:RUNNING&numResults=1")
            .getEntitySummaryResult().getEntitySummaries();
        softAssert.assertEquals(summaries.length, 1, "Single process should be shown.");
        softAssert.assertEquals(summaries[0].getStatus(), "RUNNING", "Wrong status was returned.");
        softAssert.assertEquals(summaries[0].getInstances().length, 7, "Invalid instances number.");

        //use the same request adding numInstances option, check instances number differs
        summaries = prism.getProcessHelper()
            .getEntitySummary(clusterName, "filterBy=STATUS:RUNNING&numResults=1&numInstances=2")
            .getEntitySummaryResult().getEntitySummaries();
        softAssert.assertEquals(summaries.length, 1, "Single process should be shown.");
        softAssert.assertEquals(summaries[0].getStatus(), "RUNNING", "Wrong status was returned.");
        softAssert.assertEquals(summaries[0].getInstances().length, 2, "Invalid instances number.");

        //filterBy RUNNING status AND specific pipeline name of one of the running processes.
        //Result should reflect 'AND' logic and return the only process with correct pipeline
        //name which matches to a given one in request. Also we should use 'fields' option to get
        //pipelines listed in entity summary.
        summaries = prism.getProcessHelper().getEntitySummary(clusterName,
            "filterBy=STATUS:RUNNING,PIPELINES:pipeline_2&fields=PIPELINES")
            .getEntitySummaryResult().getEntitySummaries();
        softAssert.assertEquals(summaries.length, 1, "Single process should be shown.");
        softAssert.assertEquals(summaries[0].getName(), pNames.get(1), "Invalid process name returned");
        softAssert.assertEquals(summaries[0].getPipelines()[0], "pipeline_2",
            "Summary returned process with a wrong pipeline name.");

        //test 'tags' option. 1st and 3d processes should be returned in ascending order.
        summaries = prism.getProcessHelper()
            .getEntitySummary(clusterName, "tags=value=1&orderBy=name&sortOrder=asc")
            .getEntitySummaryResult().getEntitySummaries();
        softAssert.assertEquals(summaries.length, 2, "Wrong number of processes.");
        softAssert.assertEquals(summaries[0].getName(), pNames.get(0), "Wrong order.");
        softAssert.assertEquals(summaries[0].getStatus(), "SUSPENDED", "Wrong status.");
        softAssert.assertEquals(summaries[1].getName(), pNames.get(2), "Wrong order.");
        softAssert.assertEquals(summaries[1].getStatus(), "RUNNING", "Wrong status.");

        //attempt to use invalid option or values should not return any summaries.
        EntitySummaryResult summary = prism.getProcessHelper()
            .getEntitySummary(clusterName, "filterBy=STATUS:STUCK")
            .getEntitySummaryResult();
        softAssert.assertEquals(summary.getStatus(), APIResult.Status.SUCCEEDED);
        softAssert.assertEquals(summary.getMessage(), "Entity Summary Result", "Invalid message.");
        softAssert.assertEquals(summary.getEntitySummaries(), null, "Entity summary should be null.");
        softAssert.assertAll();
    }
}
