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
import org.apache.oozie.client.AuthOozieClient;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.XOozieClient;
import org.joda.time.DateTime;
import org.apache.log4j.Logger;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * helper methods for oozie .
 */
public final class OozieUtil {
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

    public static List<String> getBundleIds(OozieClient client, String filter, int start, int len)
        throws OozieClientException {
        return getBundleIds(getBundles(client, filter, start, len));
    }

    public static List<String> getBundleIds(List<BundleJob> bundles) {
        List<String> ids = new ArrayList<String>();
        for (BundleJob bundle : bundles) {
            LOGGER.info("Bundle Id: " + bundle.getId());
            ids.add(bundle.getId());
        }
        return ids;
    }

    public static List<Job.Status> getBundleStatuses(OozieClient client, String filter, int start,
                                                     int len) throws OozieClientException {
        return getBundleStatuses(getBundles(client, filter, start, len));
    }

    public static List<Job.Status> getBundleStatuses(List<BundleJob> bundles) {
        List<Job.Status> statuses = new ArrayList<Job.Status>();
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
        List<String> jobIds = new ArrayList<String>();
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

        final List<CoordinatorAction> actions =
            oozieClient.getCoordJobInfo(coordinatorId).getActions();
        LOGGER.info("actions: " + actions);

        for (CoordinatorAction action : actions) {
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

        }

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
        List<Job.Status> statuses = getBundleStatuses(client, filter, 0, 10);
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
        return getBundleIds(client, filter, 0, 10);
    }

    public static List<DateTime> getStartTimeForRunningCoordinators(ColoHelper prismHelper,
                                                                    String bundleID)
        throws OozieClientException {
        List<DateTime> startTimes = new ArrayList<DateTime>();

        XOozieClient oozieClient = prismHelper.getClusterHelper().getOozieClient();
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


    public static List<String> getMissingDependencies(ColoHelper helper, String bundleID)
        throws OozieClientException {
        CoordinatorJob jobInfo;
        jobInfo = null;
        OozieClient oozieClient = helper.getClusterHelper().getOozieClient();
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
        return new ArrayList<String>(Arrays.asList(missingDependencies));
    }

    public static List<String> getWorkflowJobs(ColoHelper prismHelper, String bundleID)
        throws OozieClientException {
        XOozieClient oozieClient = prismHelper.getClusterHelper().getOozieClient();
        waitForCoordinatorJobCreation(oozieClient, bundleID);
        List<String> workflowIds = new ArrayList<String>();
        List<CoordinatorJob> coordJobs = oozieClient.getBundleJobInfo(bundleID).getCoordinators();
        CoordinatorJob coordJobInfo = oozieClient.getCoordJobInfo(coordJobs.get(0).getId());

        for (CoordinatorAction action : coordJobInfo.getActions()) {
            workflowIds.add(action.getExternalId());
        }
        return workflowIds;
    }

    public static Date getNominalTime(ColoHelper prismHelper, String bundleID)
        throws OozieClientException {
        XOozieClient oozieClient = prismHelper.getClusterHelper().getOozieClient();
        BundleJob bundleJob = oozieClient.getBundleJobInfo(bundleID);
        CoordinatorJob jobInfo =
            oozieClient.getCoordJobInfo(bundleJob.getCoordinators().get(0).getId());
        List<CoordinatorAction> actions = jobInfo.getActions();

        return actions.get(0).getNominalTime();

    }

    public static CoordinatorJob getDefaultOozieCoord(ColoHelper prismHelper, String bundleId,
                                                      EntityType type)
        throws OozieClientException {
        XOozieClient client = prismHelper.getClusterHelper().getOozieClient();
        BundleJob bundlejob = client.getBundleJobInfo(bundleId);

        for (CoordinatorJob coord : bundlejob.getCoordinators()) {
            if ((coord.getAppName().contains("DEFAULT") && EntityType.PROCESS == type)
                    ||
                (coord.getAppName().contains("REPLICATION") && EntityType.FEED == type)) {
                return client.getCoordJobInfo(coord.getId());
            } else {
                LOGGER.info("Desired coord does not exists on " + client.getOozieUrl());
            }
        }

        return null;
    }

    public static int getNumberOfWorkflowInstances(ColoHelper prismHelper, String bundleId)
        throws OozieClientException {
        return getDefaultOozieCoord(prismHelper, bundleId,
            EntityType.PROCESS).getActions().size();
    }

    public static List<String> getActionsNominalTime(ColoHelper prismHelper,
                                                     String bundleId,
                                                     EntityType type)
        throws OozieClientException {
        Map<Date, CoordinatorAction.Status> actions = getActionsNominalTimeAndStatus(prismHelper, bundleId, type);
        List<String> nominalTime = new ArrayList<String>();
        for (Date date : actions.keySet()) {
            nominalTime.add(date.toString());
        }
        return nominalTime;
    }
    public static Map<Date, CoordinatorAction.Status> getActionsNominalTimeAndStatus(ColoHelper prismHelper,
            String bundleId, EntityType type) throws OozieClientException {
        Map<Date, CoordinatorAction.Status> result = new TreeMap<Date, CoordinatorAction.Status>();
        List<CoordinatorAction> actions = getDefaultOozieCoord(prismHelper,
                bundleId, type).getActions();
        for (CoordinatorAction action : actions) {
            result.put(action.getNominalTime(), action.getStatus());
        }
        return result;
    }

    public static boolean isBundleOver(ColoHelper coloHelper, String bundleId)
        throws OozieClientException {
        XOozieClient client = coloHelper.getClusterHelper().getOozieClient();

        BundleJob bundleJob = client.getBundleJobInfo(bundleId);

        if (EnumSet.of(BundleJob.Status.DONEWITHERROR, BundleJob.Status.FAILED,
            BundleJob.Status.SUCCEEDED, BundleJob.Status.KILLED).contains(bundleJob.getStatus())) {
            return true;
        }

        TimeUtil.sleepSeconds(20);
        return false;
    }

    public static void verifyNewBundleCreation(ColoHelper cluster,
                                               String originalBundleId,
                                               List<String>
                                                   initialNominalTimes,
                                               String entity,
                                               boolean shouldBeCreated,

                                               boolean matchInstances) throws OozieClientException {
        String entityName = Util.readEntityName(entity);
        EntityType entityType = Util.getEntityType(entity);
        String newBundleId = InstanceUtil.getLatestBundleID(cluster, entityName,
            entityType);
        if (shouldBeCreated) {
            Assert.assertTrue(!newBundleId.equalsIgnoreCase(originalBundleId),
                "eeks! new bundle is not getting created!!!!");
            LOGGER.info("old bundleId=" + originalBundleId + " on oozie: "
                    +
                "" + cluster.getProcessHelper().getOozieClient().getOozieUrl());
            LOGGER.info("new bundleId=" + newBundleId + " on oozie: "
                    +
                "" + cluster.getProcessHelper().getOozieClient().getOozieUrl());
            if (matchInstances) {
                validateNumberOfWorkflowInstances(cluster,
                        initialNominalTimes, originalBundleId, newBundleId, entityType);
            }
        } else {
            Assert.assertEquals(newBundleId,
                originalBundleId, "eeks! new bundle is getting created!!!!");
        }
    }

    private static void validateNumberOfWorkflowInstances(ColoHelper cluster,
                                                          List<String> initialNominalTimes,
                                                          String originalBundleId,
                                                          String newBundleId, EntityType type)
        throws OozieClientException {

        List<String> nominalTimesOriginalAndNew = getActionsNominalTime(cluster,
                originalBundleId, type);

        nominalTimesOriginalAndNew.addAll(getActionsNominalTime(cluster,
            newBundleId, type));

        initialNominalTimes.removeAll(nominalTimesOriginalAndNew);

        if (initialNominalTimes.size() != 0) {
            LOGGER.info("Missing instance are : " + initialNominalTimes);
            LOGGER.debug("Original Bundle ID   : " + originalBundleId);
            LOGGER.debug("New Bundle ID        : " + newBundleId);

            Assert.assertFalse(true, "some instances have gone missing after "
                    +
                "update");
        }
    }

    public static String getCoordStartTime(ColoHelper colo, String entity,
                                           int bundleNo)
        throws OozieClientException {
        String bundleID = InstanceUtil.getSequenceBundleID(colo,
            Util.readEntityName(entity), Util.getEntityType(entity), bundleNo);

        CoordinatorJob coord = getDefaultOozieCoord(colo, bundleID,
            Util.getEntityType(entity));

        return TimeUtil.dateToOozieDate(coord.getStartTime()
        );
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
        String bundleID = InstanceUtil.getSequenceBundleID(helper, entityName, type, bundleNumber);
        OozieClient oozieClient = helper.getClusterHelper().getOozieClient();
        List<CoordinatorJob> coords = oozieClient.getBundleJobInfo(bundleID).getCoordinators();
        HadoopUtil.createHDFSFolders(helper, getMissingDependenciesForInstance(oozieClient, coords,
            instanceNumber));
    }

    private static List<String> getMissingDependenciesForInstance(OozieClient oozieClient,
            List<CoordinatorJob> coords, int instanceNumber)
        throws OozieClientException {
        ArrayList<String> missingPaths = new ArrayList<String>();
        for (CoordinatorJob coord : coords) {

            CoordinatorJob temp = oozieClient.getCoordJobInfo(coord.getId());
            CoordinatorAction instance = temp.getActions().get(instanceNumber);
            missingPaths.addAll(Arrays.asList(instance.getMissingDependencies().split("#")));
        }
        return missingPaths;
    }

    public static void createMissingDependencies(ColoHelper helper, EntityType type,
                                                 String entityName, int bundleNumber)
        throws OozieClientException, IOException {
        String bundleID = InstanceUtil.getSequenceBundleID(helper, entityName, type, bundleNumber);
        createMissingDependenciesForBundle(helper, bundleID);

    }

    public static void createMissingDependenciesForBundle(ColoHelper helper, String bundleId)
        throws OozieClientException, IOException {
        OozieClient oozieClient = helper.getClusterHelper().getOozieClient();
        List<CoordinatorJob> coords = oozieClient.getBundleJobInfo(bundleId).getCoordinators();
        for (CoordinatorJob coord : coords) {

            CoordinatorJob temp = oozieClient.getCoordJobInfo(coord.getId());
            for (int instanceNumber = 0; instanceNumber < temp.getActions().size();
                 instanceNumber++) {
                CoordinatorAction instance = temp.getActions().get(instanceNumber);
                HadoopUtil.createHDFSFolders(helper,
                    Arrays.asList(instance.getMissingDependencies().split("#")));
            }
        }
    }

    public static void validateRetryAttempts(ColoHelper helper, String bundleId, EntityType type,
                                             int attempts) throws OozieClientException {
        OozieClient oozieClient = helper.getClusterHelper().getOozieClient();
        CoordinatorJob coord = getDefaultOozieCoord(helper, bundleId, type);
        int actualRun = oozieClient.getJobInfo(coord.getActions().get(0).getExternalId()).getRun();
        LOGGER.info("Actual run count: " + actualRun); // wrt 0
        Assert.assertEquals(actualRun, attempts, "Rerun attempts did not match");
    }
}
