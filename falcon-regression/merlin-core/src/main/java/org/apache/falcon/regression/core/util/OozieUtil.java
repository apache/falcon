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

package org.apache.falcon.regression.core.util;

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.entity.AbstractEntityHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.AuthOozieClient;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.WorkflowAction;
import org.joda.time.DateTime;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONException;
import org.testng.Assert;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * helper methods for oozie .
 */
public final class OozieUtil {

    public static final String FAIL_MSG = "NO_such_workflow_exists";
    private OozieUtil() {
        throw new AssertionError("Instantiating utility class...");
    }

    private static final Logger LOGGER = Logger.getLogger(OozieUtil.class);

    public static AuthOozieClient getClient(String url) {
        return new AuthOozieClient(url);
    }

    public static List<BundleJob> getBundles(OozieClient client, String filter, int start, int len)
        throws OozieClientException {
        LOGGER.info("Connecting to oozie: " + client.getOozieUrl());
        return client.getBundleJobsInfo(filter, start, len);
    }

    public static List<String> getBundleIds(List<BundleJob> bundles) {
        List<String> ids = new ArrayList<>();
        for (BundleJob bundle : bundles) {
            LOGGER.info("Bundle Id: " + bundle.getId());
            ids.add(bundle.getId());
        }
        return ids;
    }

    public static List<Job.Status> getBundleStatuses(List<BundleJob> bundles) {
        List<Job.Status> statuses = new ArrayList<>();
        for (BundleJob bundle : bundles) {
            LOGGER.info("bundle: " + bundle);
            statuses.add(bundle.getStatus());
        }
        return statuses;
    }

    public static String getMaxId(List<String> ids) {
        String oozieId = ids.get(0);
        int maxInt = Integer.valueOf(oozieId.split("-")[0]);
        for (int i = 1; i < ids.size(); i++) {
            String currentId = ids.get(i);
            int currInt = Integer.valueOf(currentId.split("-")[0]);
            if (currInt > maxInt) {
                oozieId = currentId;
            }
        }
        return oozieId;
    }

    public static String getMinId(List<String> ids) {
        String oozieId = ids.get(0);
        int minInt = Integer.valueOf(oozieId.split("-")[0]);
        for (int i = 1; i < ids.size(); i++) {
            String currentId = ids.get(i);
            int currInt = Integer.valueOf(currentId.split("-")[0]);
            if (currInt < minInt) {
                oozieId = currentId;
            }
        }
        return oozieId;
    }

    /**
     * @param bundleID bundle number
     * @param oozieClient oozie client
     * @return list of action ids of the succeeded retention workflow
     * @throws OozieClientException
     */
    public static List<String> waitForRetentionWorkflowToSucceed(String bundleID,
                                                                 OozieClient oozieClient)
        throws OozieClientException {
        LOGGER.info("Connecting to oozie: " + oozieClient.getOozieUrl());
        List<String> jobIds = new ArrayList<>();
        LOGGER.info("using bundleId:" + bundleID);
        waitForCoordinatorJobCreation(oozieClient, bundleID);
        final String coordinatorId =
            oozieClient.getBundleJobInfo(bundleID).getCoordinators().get(0).getId();
        LOGGER.info("using coordinatorId: " + coordinatorId);

        for (int i = 0;
             i < 120 && oozieClient.getCoordJobInfo(coordinatorId).getActions().isEmpty(); ++i) {
            TimeUtil.sleepSeconds(4);
        }
        Assert.assertFalse(oozieClient.getCoordJobInfo(coordinatorId).getActions().isEmpty(),
            "Coordinator actions should have got created by now.");

        CoordinatorAction action = oozieClient.getCoordJobInfo(coordinatorId).getActions().get(0);
        for (int i = 0; i < 180; ++i) {
            CoordinatorAction actionInfo = oozieClient.getCoordActionInfo(action.getId());
            LOGGER.info("actionInfo: " + actionInfo);
            if (EnumSet.of(CoordinatorAction.Status.SUCCEEDED, CoordinatorAction.Status.KILLED,
                CoordinatorAction.Status.FAILED).contains(actionInfo.getStatus())) {
                break;
            }
            TimeUtil.sleepSeconds(10);
        }
        Assert.assertEquals(
            oozieClient.getCoordActionInfo(action.getId()).getStatus(),
            CoordinatorAction.Status.SUCCEEDED,
            "Action did not succeed.");
        jobIds.add(action.getId());

        return jobIds;
    }

    public static void waitForCoordinatorJobCreation(OozieClient oozieClient, String bundleID)
        throws OozieClientException {
        LOGGER.info("Connecting to oozie: " + oozieClient.getOozieUrl());
        for (int i = 0;
             i < 60 && oozieClient.getBundleJobInfo(bundleID).getCoordinators().isEmpty(); ++i) {
            TimeUtil.sleepSeconds(2);
        }
        Assert.assertFalse(oozieClient.getBundleJobInfo(bundleID).getCoordinators().isEmpty(),
            "Coordinator job should have got created by now.");
    }

    public static Job.Status getOozieJobStatus(OozieClient client, String processName,
                                               EntityType entityType)
        throws OozieClientException {
        String filter = String.format("name=FALCON_%s_%s", entityType, processName);
        List<Job.Status> statuses = getBundleStatuses(getBundles(client, filter, 0, 10));
        if (statuses.isEmpty()) {
            return null;
        } else {
            return statuses.get(0);
        }
    }

    public static List<String> getBundles(OozieClient client, String entityName,
                                          EntityType entityType)
        throws OozieClientException {
        String filter = "name=FALCON_" + entityType + "_" + entityName;
        return getBundleIds(getBundles(client, filter, 0, 10));
    }

    public static List<DateTime> getStartTimeForRunningCoordinators(ColoHelper prismHelper,
                                                                    String bundleID)
        throws OozieClientException {
        List<DateTime> startTimes = new ArrayList<>();

        OozieClient oozieClient = prismHelper.getClusterHelper().getOozieClient();
        BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleID);
        CoordinatorJob jobInfo;


        for (CoordinatorJob job : bundleJob.getCoordinators()) {

            if (job.getAppName().contains("DEFAULT")) {
                jobInfo = oozieClient.getCoordJobInfo(job.getId());
                for (CoordinatorAction action : jobInfo.getActions()) {
                    DateTime temp = new DateTime(action.getCreatedTime(), DateTimeZone.UTC);
                    LOGGER.info(temp);
                    startTimes.add(temp);
                }
            }

            Collections.sort(startTimes);

            if (!(startTimes.isEmpty())) {
                return startTimes;
            }
        }

        return null;
    }

    public static boolean verifyOozieJobStatus(OozieClient client, String processName,
                                               EntityType entityType, Job.Status expectedStatus)
        throws OozieClientException {
        for (int seconds = 0; seconds < 100; seconds+=5) {
            Job.Status status = getOozieJobStatus(client, processName, entityType);
            LOGGER.info("Current status: " + status);
            if (status == expectedStatus) {
                return true;
            }
            TimeUtil.sleepSeconds(5);
        }
        return false;
    }


    public static List<String> getMissingDependencies(OozieClient oozieClient, String bundleID)
        throws OozieClientException {
        CoordinatorJob jobInfo;
        jobInfo = null;
        BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleID);
        List<CoordinatorJob> coordinatorJobList = bundleJob.getCoordinators();
        if (coordinatorJobList.size() > 1) {

            for (CoordinatorJob coord : bundleJob.getCoordinators()) {
                LOGGER.info("Appname is : " + coord.getAppName());
                if ((coord.getAppName().contains("DEFAULT")
                        && coord.getAppName().contains("PROCESS"))
                    ||
                    (coord.getAppName().contains("REPLICATION")
                        && coord.getAppName().contains("FEED"))) {
                    jobInfo = oozieClient.getCoordJobInfo(coord.getId());
                } else {
                    LOGGER.info("Desired coord does not exists on " + oozieClient.getOozieUrl());
                }
            }

        } else {
            jobInfo = oozieClient.getCoordJobInfo(bundleJob.getCoordinators().get(0).getId());
        }

        LOGGER.info("Coordinator id : " + jobInfo);
        List<CoordinatorAction> actions = null;
        if (jobInfo != null) {
            actions = jobInfo.getActions();
        }

        if (actions != null) {
            if (actions.size() < 1) {
                return null;
            }
        }
        if (actions != null) {
            LOGGER.info("conf from event: " + actions.get(0).getMissingDependencies());
        }

        String[] missingDependencies = new String[0];
        if (actions != null) {
            missingDependencies = actions.get(0).getMissingDependencies().split("#");
        }
        return new ArrayList<>(Arrays.asList(missingDependencies));
    }

    public static List<String> getWorkflowJobs(OozieClient oozieClient, String bundleID)
        throws OozieClientException {
        waitForCoordinatorJobCreation(oozieClient, bundleID);
        List<String> workflowIds = new ArrayList<>();
        List<CoordinatorJob> coordJobs = oozieClient.getBundleJobInfo(bundleID).getCoordinators();
        CoordinatorJob coordJobInfo = oozieClient.getCoordJobInfo(coordJobs.get(0).getId());

        for (CoordinatorAction action : coordJobInfo.getActions()) {
            workflowIds.add(action.getExternalId());
        }
        return workflowIds;
    }

    public static List<String> getWorkflow(OozieClient oozieClient, String bundleID)
        throws OozieClientException {
        waitForCoordinatorJobCreation(oozieClient, bundleID);
        List<String> workflowIds = new ArrayList<>();
        String coordId = getDefaultCoordIDFromBundle(oozieClient, bundleID);
        CoordinatorJob coordJobInfo = oozieClient.getCoordJobInfo(coordId);
        for (CoordinatorAction action : coordJobInfo.getActions()) {
            if (action.getStatus().name().equals("RUNNING") || action.getStatus().name().equals("SUCCEEDED")) {
                workflowIds.add(action.getExternalId());
            }
            if (action.getStatus().name().equals("KILLED") || action.getStatus().name().equals("WAITING")) {
                Assert.assertNull(action.getExternalId());
            }
        }
        return workflowIds;
    }

    public static Date getNominalTime(OozieClient oozieClient, String bundleID)
        throws OozieClientException {
        BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleID);
        CoordinatorJob jobInfo =
            oozieClient.getCoordJobInfo(bundleJob.getCoordinators().get(0).getId());
        List<CoordinatorAction> actions = jobInfo.getActions();
        return actions.get(0).getNominalTime();
    }

    public static CoordinatorJob getDefaultOozieCoord(OozieClient oozieClient, String bundleId,
                                                      EntityType type)
        throws OozieClientException {
        BundleJob bundlejob = oozieClient.getBundleJobInfo(bundleId);
        for (CoordinatorJob coord : bundlejob.getCoordinators()) {
            if ((coord.getAppName().contains("DEFAULT") && EntityType.PROCESS == type)
                    ||
                (coord.getAppName().contains("REPLICATION") && EntityType.FEED == type)) {
                return oozieClient.getCoordJobInfo(coord.getId());
            } else {
                LOGGER.info("Desired coord does not exists on " + oozieClient.getOozieUrl());
            }
        }
        return null;
    }

    public static int getNumberOfWorkflowInstances(OozieClient oozieClient, String bundleId)
        throws OozieClientException {
        return getDefaultOozieCoord(oozieClient, bundleId, EntityType.PROCESS).getActions().size();
    }

    public static List<String> getActionsNominalTime(OozieClient oozieClient,
                                                     String bundleId, EntityType type)
        throws OozieClientException {
        Map<Date, CoordinatorAction.Status> actions = getActionsNominalTimeAndStatus(oozieClient, bundleId, type);
        List<String> nominalTime = new ArrayList<>();
        for (Date date : actions.keySet()) {
            nominalTime.add(date.toString());
        }
        return nominalTime;
    }

    public static Map<Date, CoordinatorAction.Status> getActionsNominalTimeAndStatus(OozieClient oozieClient,
            String bundleId, EntityType type) throws OozieClientException {
        Map<Date, CoordinatorAction.Status> result = new TreeMap<>();
        List<CoordinatorAction> actions = getDefaultOozieCoord(oozieClient, bundleId, type).getActions();
        for (CoordinatorAction action : actions) {
            result.put(action.getNominalTime(), action.getStatus());
        }
        return result;
    }

    public static boolean isBundleOver(ColoHelper coloHelper, String bundleId)
        throws OozieClientException {
        OozieClient client = coloHelper.getClusterHelper().getOozieClient();

        BundleJob bundleJob = client.getBundleJobInfo(bundleId);

        if (EnumSet.of(BundleJob.Status.DONEWITHERROR, BundleJob.Status.FAILED,
            BundleJob.Status.SUCCEEDED, BundleJob.Status.KILLED).contains(bundleJob.getStatus())) {
            return true;
        }
        TimeUtil.sleepSeconds(20);
        return false;
    }

    public static void verifyNewBundleCreation(OozieClient oozieClient, String originalBundleId,
                                               List<String> initialNominalTimes, String entity,
                                               boolean shouldBeCreated, boolean matchInstances)
        throws OozieClientException {
        String entityName = Util.readEntityName(entity);
        EntityType entityType = Util.getEntityType(entity);
        String newBundleId = getLatestBundleID(oozieClient, entityName, entityType);
        if (shouldBeCreated) {
            Assert.assertTrue(!newBundleId.equalsIgnoreCase(originalBundleId),
                "eeks! new bundle is not getting created!!!!");
            LOGGER.info("old bundleId=" + originalBundleId + " on oozie: " + oozieClient);
            LOGGER.info("new bundleId=" + newBundleId + " on oozie: " + oozieClient);
            if (matchInstances) {
                validateNumberOfWorkflowInstances(oozieClient,
                        initialNominalTimes, originalBundleId, newBundleId, entityType);
            }
        } else {
            Assert.assertEquals(newBundleId, originalBundleId, "eeks! new bundle is getting created!!!!");
        }
    }

    private static void validateNumberOfWorkflowInstances(OozieClient oozieClient,
                                                          List<String> initialNominalTimes,
                                                          String originalBundleId,
                                                          String newBundleId, EntityType type)
        throws OozieClientException {
        List<String> nominalTimesOriginalAndNew = getActionsNominalTime(oozieClient, originalBundleId, type);
        nominalTimesOriginalAndNew.addAll(getActionsNominalTime(oozieClient, newBundleId, type));
        initialNominalTimes.removeAll(nominalTimesOriginalAndNew);
        if (initialNominalTimes.size() != 0) {
            LOGGER.info("Missing instance are : " + initialNominalTimes);
            LOGGER.debug("Original Bundle ID   : " + originalBundleId);
            LOGGER.debug("New Bundle ID        : " + newBundleId);
            Assert.fail("some instances have gone missing after update");
        }
    }

    public static String getCoordStartTime(OozieClient oozieClient, String entity, int bundleNo)
        throws OozieClientException {
        String bundleID = getSequenceBundleID(oozieClient,
            Util.readEntityName(entity), Util.getEntityType(entity), bundleNo);
        CoordinatorJob coord = getDefaultOozieCoord(oozieClient, bundleID,
            Util.getEntityType(entity));
        return TimeUtil.dateToOozieDate(coord.getStartTime());
    }

    public static DateTimeFormatter getOozieDateTimeFormatter() {
        return DateTimeFormat.forPattern("yyyy'-'MM'-'dd'T'HH':'mm'Z'");
    }

    public static int getNumberOfBundle(OozieClient oozieClient, EntityType type, String entityName)
        throws OozieClientException {
        return OozieUtil.getBundles(oozieClient, entityName, type).size();
    }

    public static void createMissingDependencies(ColoHelper helper, EntityType type,
                                                 String entityName, int bundleNumber,
                                                 int instanceNumber)
        throws OozieClientException, IOException {
        final OozieClient oozieClient = helper.getClusterHelper().getOozieClient();
        String bundleID = getSequenceBundleID(oozieClient, entityName, type, bundleNumber);
        List<CoordinatorJob> coords = oozieClient.getBundleJobInfo(bundleID).getCoordinators();
        final List<String> missingDependencies = getMissingDependenciesForInstance(oozieClient, coords, instanceNumber);
        HadoopUtil.createFolders(helper.getClusterHelper().getHadoopFS(), helper.getPrefix(), missingDependencies);
    }

    private static List<String> getMissingDependenciesForInstance(OozieClient oozieClient,
            List<CoordinatorJob> coords, int instanceNumber)
        throws OozieClientException {
        ArrayList<String> missingPaths = new ArrayList<>();
        for (CoordinatorJob coord : coords) {
            CoordinatorJob temp = oozieClient.getCoordJobInfo(coord.getId());
            CoordinatorAction instance = temp.getActions().get(instanceNumber);
            missingPaths.addAll(Arrays.asList(instance.getMissingDependencies().split("#")));
        }
        return missingPaths;
    }

    public static List<List<String>> createMissingDependencies(ColoHelper helper, EntityType type,
                                                 String entityName, int bundleNumber)
        throws OozieClientException, IOException {
        final OozieClient oozieClient = helper.getClusterHelper().getOozieClient();
        String bundleID = getSequenceBundleID(oozieClient, entityName, type, bundleNumber);
        return createMissingDependenciesForBundle(helper, bundleID);
    }

    public static List<List<String>> createMissingDependenciesForBundle(ColoHelper helper, String bundleId)
        throws OozieClientException, IOException {
        OozieClient oozieClient = helper.getClusterHelper().getOozieClient();
        List<CoordinatorJob> coords = oozieClient.getBundleJobInfo(bundleId).getCoordinators();
        List<List<String>> missingDependencies = getMissingDependenciesForBundle(oozieClient, coords);
        for (List<String> missingDependencyPerInstance : missingDependencies) {
            HadoopUtil.createFolders(helper.getClusterHelper().getHadoopFS(), helper.getPrefix(),
                missingDependencyPerInstance);
        }
        return missingDependencies;
    }

    private static List<List<String>> getMissingDependenciesForBundle(OozieClient oozieClient,
                                                                      List<CoordinatorJob> coords)
        throws OozieClientException, IOException {
        List<List<String>> missingDependencies = new ArrayList<>();
        for (CoordinatorJob coord : coords) {
            CoordinatorJob temp = oozieClient.getCoordJobInfo(coord.getId());
            for (int instanceNumber = 0; instanceNumber < temp.getActions().size();
                 instanceNumber++) {
                CoordinatorAction instance = temp.getActions().get(instanceNumber);
                missingDependencies.add(Arrays.asList(instance.getMissingDependencies().split("#")));
            }
        }
        return missingDependencies;
    }

    public static void validateRetryAttempts(OozieClient oozieClient, String bundleId, EntityType type,
                                             int attempts) throws OozieClientException {
        CoordinatorJob coord = getDefaultOozieCoord(oozieClient, bundleId, type);
        int actualRun = oozieClient.getJobInfo(coord.getActions().get(0).getExternalId()).getRun();
        LOGGER.info("Actual run count: " + actualRun); // wrt 0
        Assert.assertEquals(actualRun, attempts, "Rerun attempts did not match");
    }

    /**
     * Try to find feed coordinators of given type.
     */
    public static int checkIfFeedCoordExist(OozieClient oozieClient,
                                            String feedName, String coordType) throws OozieClientException {
        return checkIfFeedCoordExist(oozieClient, feedName, coordType, 5);
    }

    /**
     * Try to find feed coordinators of given type given number of times.
     */
    public static int checkIfFeedCoordExist(OozieClient oozieClient,
            String feedName, String coordType, int numberOfRetries) throws OozieClientException {
        LOGGER.info("feedName: " + feedName);
        for (int retryAttempt = 0; retryAttempt < numberOfRetries; retryAttempt++) {
            int numberOfCoord = 0;
            List<String> bundleIds = getBundles(oozieClient, feedName, EntityType.FEED);
            if (bundleIds.size() == 0) {
                TimeUtil.sleepSeconds(4);
                continue;
            }
            LOGGER.info("bundleIds: " + bundleIds);
            for (String aBundleId : bundleIds) {
                LOGGER.info("aBundleId: " + aBundleId);
                waitForCoordinatorJobCreation(oozieClient, aBundleId);
                List<CoordinatorJob> coords = getBundleCoordinators(oozieClient, aBundleId);
                LOGGER.info("coords: " + coords);
                for (CoordinatorJob coord : coords) {
                    if (coord.getAppName().contains(coordType)) {
                        numberOfCoord++;
                    }
                }
            }
            if (numberOfCoord > 0) {
                return numberOfCoord;
            }
            TimeUtil.sleepSeconds(4);
        }
        return 0;
    }

    /**
     * Retrieves replication coordinatorID from bundle of coordinators.
     */
    public static List<String> getReplicationCoordID(String bundleId, AbstractEntityHelper helper)
        throws OozieClientException {
        final OozieClient oozieClient = helper.getOozieClient();
        List<CoordinatorJob> coords = getBundleCoordinators(oozieClient, bundleId);
        List<String> replicationCoordID = new ArrayList<>();
        for (CoordinatorJob coord : coords) {
            if (coord.getAppName().contains("FEED_REPLICATION")) {
                replicationCoordID.add(coord.getId());
            }
        }
        return replicationCoordID;
    }

    /**
     * Retrieves ID of bundle related to some process/feed using its ordinal number.
     *
     * @param entityName   - name of entity bundle is related to
     * @param entityType   - feed or process
     * @param bundleNumber - ordinal number of bundle
     * @return bundle ID
     * @throws org.apache.oozie.client.OozieClientException
     */
    public static String getSequenceBundleID(OozieClient oozieClient, String entityName,
            EntityType entityType, int bundleNumber) throws OozieClientException {
        //sequence start from 0
        List<String> bundleIds = getBundles(oozieClient, entityName, entityType);
        Collections.sort(bundleIds);
        if (bundleNumber < bundleIds.size()) {
            return bundleIds.get(bundleNumber);
        }
        return null;
    }

    /**
     * Retrieves the latest bundle ID.
     *
     * @param oozieClient where job is running
     * @param entityName  name of entity job is related to
     * @param entityType  type of entity - feed or process expected
     * @return latest bundle ID
     * @throws org.apache.oozie.client.OozieClientException
     */
    public static String getLatestBundleID(OozieClient oozieClient,
            String entityName, EntityType entityType) throws OozieClientException {
        List<String> bundleIds = getBundles(oozieClient, entityName, entityType);
        String max = "0";
        int maxID = -1;
        for (String strID : bundleIds) {
            if (maxID < Integer.parseInt(strID.substring(0, strID.indexOf('-')))) {
                maxID = Integer.parseInt(strID.substring(0, strID.indexOf('-')));
                max = strID;
            }
        }
        return max;
    }

    /**
     * Retrieves all coordinators of bundle.
     *
     * @param oozieClient Oozie client to use for fetching info.
     * @param bundleID specific bundle ID
     * @return list of bundle coordinators
     * @throws org.apache.oozie.client.OozieClientException
     */
    public static List<CoordinatorJob> getBundleCoordinators(OozieClient oozieClient, String bundleID)
        throws OozieClientException {
        BundleJob bundleInfo = oozieClient.getBundleJobInfo(bundleID);
        return bundleInfo.getCoordinators();
    }

    public static Job.Status getDefaultCoordinatorStatus(OozieClient oozieClient, String processName,
            int bundleNumber) throws OozieClientException {
        String bundleID = getSequenceBundleID(oozieClient, processName, EntityType.PROCESS, bundleNumber);
        String coordId = getDefaultCoordIDFromBundle(oozieClient, bundleID);
        return oozieClient.getCoordJobInfo(coordId).getStatus();
    }

    public static String getDefaultCoordIDFromBundle(OozieClient oozieClient, String bundleId)
        throws OozieClientException {
        waitForCoordinatorJobCreation(oozieClient, bundleId);
        BundleJob bundleInfo = oozieClient.getBundleJobInfo(bundleId);
        List<CoordinatorJob> coords = bundleInfo.getCoordinators();
        int min = 100000;
        String minString = "";
        for (CoordinatorJob coord : coords) {
            String strID = coord.getId();
            if (min > Integer.parseInt(strID.substring(0, strID.indexOf('-')))) {
                min = Integer.parseInt(strID.substring(0, strID.indexOf('-')));
                minString = coord.getId();
            }
        }
        LOGGER.info("function getDefaultCoordIDFromBundle: minString: " + minString);
        return minString;
    }

    public static String getLatestCoordinatorID(OozieClient oozieClient, String processName,
            EntityType entityType) throws OozieClientException {
        final String latestBundleID = getLatestBundleID(oozieClient, processName, entityType);
        return getDefaultCoordIDFromBundle(oozieClient, latestBundleID);
    }

    /**
     * Waits till bundle job will reach expected status.
     * Generates time according to expected status.
     *
     * @param oozieClient    oozieClient of cluster job is running on
     * @param processName    name of process which job is being analyzed
     * @param expectedStatus job status we are waiting for
     * @throws org.apache.oozie.client.OozieClientException
     */
    public static void waitForBundleToReachState(OozieClient oozieClient,
            String processName, Job.Status expectedStatus) throws OozieClientException {
        int totalMinutesToWait = getMinutesToWait(expectedStatus);
        waitForBundleToReachState(oozieClient, processName, expectedStatus, totalMinutesToWait);
    }

    /**
     * Waits till bundle job will reach expected status during specific time.
     * Use it directly in test cases when timeouts are different from trivial, in other cases use
     * waitForBundleToReachState(OozieClient, String, Status)
     *
     * @param oozieClient        oozie client of cluster job is running on
     * @param processName        name of process which job is being analyzed
     * @param expectedStatus     job status we are waiting for
     * @param totalMinutesToWait specific time to wait expected state
     * @throws org.apache.oozie.client.OozieClientException
     */
    public static void waitForBundleToReachState(OozieClient oozieClient, String processName,
            Job.Status expectedStatus, int totalMinutesToWait) throws OozieClientException {
        int sleep = totalMinutesToWait * 60 / 20;
        for (int sleepCount = 0; sleepCount < sleep; sleepCount++) {
            String bundleID =
                    getLatestBundleID(oozieClient, processName, EntityType.PROCESS);
            BundleJob j = oozieClient.getBundleJobInfo(bundleID);
            LOGGER.info(sleepCount + ". Current status: " + j.getStatus()
                + "; expected: " + expectedStatus);
            if (j.getStatus() == expectedStatus) {
                return;
            }
            TimeUtil.sleepSeconds(20);
        }
        Assert.fail("State " + expectedStatus + " wasn't reached in " + totalMinutesToWait + " mins");
    }

    /**
     * Generates time which is presumably needed for bundle job to reach particular state.
     *
     * @param expectedStatus status which we are expect to get from bundle job
     * @return minutes to wait for expected status
     */
    private static int getMinutesToWait(Job.Status expectedStatus) {
        switch (expectedStatus) {
        case DONEWITHERROR:
        case SUCCEEDED:
            return OSUtil.IS_WINDOWS ? 40 : 20;
        case KILLED:
            return OSUtil.IS_WINDOWS ? 30 : 15;
        default:
            return OSUtil.IS_WINDOWS ? 60 : 30;
        }
    }

    public static String getActionStatus(OozieClient oozieClient, String workflowId, String actionName)
        throws OozieClientException {
        List<WorkflowAction> wfAction = oozieClient.getJobInfo(workflowId).getActions();
        for (WorkflowAction wf : wfAction) {
            if (wf.getName().contains(actionName)) {
                return wf.getExternalStatus();
            }
        }
        return "";
    }

    public static String getWorkflowActionStatus(OozieClient oozieClient, String bundleId, String actionName)
        throws OozieClientException {
        List<String> workflowIds = getWorkflowJobs(oozieClient, bundleId);
        if (workflowIds.get(0).isEmpty()) {
            return FAIL_MSG;
        }
        return getActionStatus(oozieClient, workflowIds.get(0), actionName);
    }

    public static String getSubWorkflowActionStatus(OozieClient oozieClient, String bundleId,
                                                    String actionName, String subAction)
        throws OozieClientException {
        List<String> workflowIds = getWorkflowJobs(oozieClient, bundleId);
        if (workflowIds.get(0).isEmpty()) {
            return FAIL_MSG;
        }

        String wid="";
        List<WorkflowAction> wfAction = oozieClient.getJobInfo(workflowIds.get(0)).getActions();
        for (WorkflowAction wf : wfAction) {
            if (wf.getName().contains(actionName)) {
                wid = wf.getExternalId();
            }
        }

        if (!wid.isEmpty()) {
            return getActionStatus(oozieClient, wid, subAction);
        }
        return FAIL_MSG;
    }

    /**
     * Returns configuration object of a given bundleID for a given instanceTime.
     *
     * @param oozieClient  oozie client of cluster job is running on
     * @param bundleID     name of process which job is being analyzed
     * @param time         job status we are waiting for
     * @throws org.apache.oozie.client.OozieClientException
     * @throws org.json.JSONException
     */
    public static Configuration getProcessConf(OozieClient oozieClient, String bundleID, String time)
        throws OozieClientException, JSONException {
        waitForCoordinatorJobCreation(oozieClient, bundleID);
        List<CoordinatorJob> coordJobs = oozieClient.getBundleJobInfo(bundleID).getCoordinators();
        CoordinatorJob coordJobInfo = oozieClient.getCoordJobInfo(coordJobs.get(0).getId());

        Configuration conf = new Configuration();
        for (CoordinatorAction action : coordJobInfo.getActions()) {
            String dateStr = (new DateTime(action.getNominalTime(), DateTimeZone.UTC)).toString();
            if (!dateStr.isEmpty() && dateStr.contains(time.replace("Z", ""))) {
                conf.addResource(new ByteArrayInputStream(oozieClient.getJobInfo(action.getExternalId()).
                        getConf().getBytes()));
            }
        }
        return conf;
    }

    /**
     * Method retrieves and parses replication coordinator action workflow definition and checks whether specific
     * properties are present in list of workflow args or not.
     * @param workflowDefinition workflow definition
     * @param actionName action within workflow, e.g pre-processing, replication etc.
     * @param propMap specific properties which are expected to be in arg list
     * @return true if all keys and values are present, false otherwise
     */
    public static boolean propsArePresentInWorkflow(String workflowDefinition, String actionName,
                                              HashMap<String, String> propMap) {
        //get action definition
        Document definition = Util.convertStringToDocument(workflowDefinition);
        Assert.assertNotNull(definition, "Workflow definition shouldn't be null.");
        NodeList actions = definition.getElementsByTagName("action");
        Element action = null;
        for (int i = 0; i < actions.getLength(); i++) {
            Node node = actions.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                action = (Element) node;
                if (action.getAttribute("name").equals(actionName)) {
                    break;
                }
                action = null;
            }
        }
        Assert.assertNotNull(action, actionName + " action not found.");

        //retrieve and checks whether properties are present in workflow args
        Element javaElement = (Element) action.getElementsByTagName("java").item(0);
        NodeList args = javaElement.getElementsByTagName("arg");
        int counter = 0;
        String key = null;
        for (int i = 0; i < args.getLength(); i++) {
            Node node = args.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                String argKey = node.getTextContent().replace("-", "");
                if (key != null && propMap.get(key).equals(argKey)) {
                    counter++;
                    key = null;
                } else if (key == null && propMap.containsKey(argKey)) {
                    key = argKey;
                }
            }
        }
        return counter == propMap.size();
    }
}
