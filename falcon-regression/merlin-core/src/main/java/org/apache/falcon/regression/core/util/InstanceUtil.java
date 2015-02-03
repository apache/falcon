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

import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonSyntaxException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.ResponseErrors;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.entity.AbstractEntityHelper;
import org.apache.falcon.request.BaseRequest;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.FeedInstanceResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesSummaryResult;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.http.HttpResponse;
import org.apache.log4j.Logger;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.Job.Status;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.joda.time.DateTime;
import org.testng.Assert;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * util functions related to instanceTest.
 */
public final class InstanceUtil {

    private InstanceUtil() {
        throw new AssertionError("Instantiating utility class...");
    }

    private static final Logger LOGGER = Logger.getLogger(InstanceUtil.class);
    private static final EnumSet<Status> RUNNING_PREP_SUCCEEDED = EnumSet.of(Status.RUNNING,
        Status.PREP, Status.SUCCEEDED);

    public static APIResult sendRequestProcessInstance(String url, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return hitUrl(url, Util.getMethodType(url), user);
    }

    public static final int INSTANCES_CREATED_TIMEOUT = OSUtil.IS_WINDOWS ? 20 : 10;

    public static APIResult hitUrl(String url,
            String method, String user) throws URISyntaxException,
            IOException, AuthenticationException, InterruptedException {
        BaseRequest request = new BaseRequest(url, method, user);
        HttpResponse response = request.run();
        String responseString = IOUtils.toString(response.getEntity().getContent(), "UTF-8");
        LOGGER.info("The web service response is:\n" + Util.prettyPrintXmlOrJson(responseString));
        APIResult result;
        if (url.contains("/summary/")) {
            result = new InstancesSummaryResult(APIResult.Status.FAILED, responseString);
        }else if (url.contains("/listing/")) {
            result = new FeedInstanceResult(APIResult.Status.FAILED, responseString);
        }else {
            result = new InstancesResult(APIResult.Status.FAILED, responseString);
        }
        Assert.assertNotNull(result, "APIResult is null");
        for (ResponseErrors error : ResponseErrors.values()) {
            if (responseString.contains(error.getError())) {
                return result;
            }
        }
        final String[] errorStrings = {
            "(FEED) not found",
            "is beforePROCESS  start",
            "is after end date",
            "is after PROCESS's end",
            "is before PROCESS's  start",
            "is before the entity was scheduled",
        };
        for (String error : errorStrings) {
            if (responseString.contains(error)) {
                return result;
            }
        }
        try {
            result = new GsonBuilder().registerTypeAdapter(Date.class, new JsonDeserializer<Date>() {
                @Override
                public Date deserialize(JsonElement json, Type t, JsonDeserializationContext c) {
                    return new DateTime(json.getAsString()).toDate();
                }
            }).create().fromJson(responseString,
                    url.contains("/listing/") ? FeedInstanceResult.class : url.contains("/summary/")
                        ? InstancesSummaryResult.class : InstancesResult.class);
        } catch (JsonSyntaxException e) {
            Assert.fail("Not a valid json:\n" + responseString);
        }
        LOGGER.info("statusCode: " + response.getStatusLine().getStatusCode());
        LOGGER.info("message: " + result.getMessage());
        LOGGER.info("APIResult.Status: " + result.getStatus());
        return result;
    }

    /**
     * Checks if API response reflects success and if it's instances match to expected status.
     *
     * @param instancesResult  - kind of response from API which should contain information about
     *                           instances
     * @param bundle           - bundle from which process instances are being analyzed
     * @param wfStatus - - expected status of instances
     */
    public static void validateSuccess(InstancesResult instancesResult, Bundle bundle,
            InstancesResult.WorkflowStatus wfStatus) {
        Assert.assertEquals(instancesResult.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertEquals(instancesInResultWithStatus(instancesResult, wfStatus),
            bundle.getProcessConcurrency());
    }

    /**
     * Check the number of instances in response which have the same status as expected.
     *
     * @param instancesResult  kind of response from API which should contain information about
     *                         instances
     * @param workflowStatus   expected status of instances
     * @return number of instances which have expected status
     */
    public static int instancesInResultWithStatus(InstancesResult instancesResult,
            InstancesResult.WorkflowStatus workflowStatus) {
        InstancesResult.Instance[] instances = instancesResult.getInstances();
        LOGGER.info("instances: " + Arrays.toString(instances));
        List<InstancesResult.WorkflowStatus> statuses =
            new ArrayList<InstancesResult.WorkflowStatus>();
        for (InstancesResult.Instance instance : instances) {
            LOGGER.info("instance: " + instance + " status = " + instance.getStatus());
            statuses.add(instance.getStatus());
        }
        return Collections.frequency(statuses, workflowStatus);
    }

    public static void validateSuccessWOInstances(InstancesResult r) {
        AssertUtil.assertSucceeded(r);
        Assert.assertNull(r.getInstances(), "Unexpected :" + Arrays.toString(r.getInstances()));
    }

    public static void validateError(InstancesResult instancesResult, ResponseErrors error) {
        Assert.assertTrue(instancesResult.getMessage().contains(error.getError()),
            "Error should contains '" + error.getError() + "'");
    }

    /**
     * Checks that actual number of instances with different statuses are equal to expected number
     * of instances with matching statuses.
     *
     * @param instancesResult kind of response from API which should contain information about
     *                        instances <p/>
     *                        All parameters below reflect number of expected instances with some
     *                        kind of status.
     * @param totalCount      total number of instances.
     * @param runningCount    number of running instances.
     * @param suspendedCount  number of suspended instance.
     * @param waitingCount    number of waiting instance.
     * @param killedCount     number of killed instance.
     */
    public static void validateResponse(InstancesResult instancesResult, int totalCount,
            int runningCount, int suspendedCount, int waitingCount, int killedCount) {
        InstancesResult.Instance[] instances = instancesResult.getInstances();
        LOGGER.info("instances: " + Arrays.toString(instances));
        Assert.assertNotNull(instances, "instances should be not null");
        Assert.assertEquals(instances.length, totalCount, "Total Instances");
        List<InstancesResult.WorkflowStatus> statuses = new ArrayList<InstancesResult.WorkflowStatus>();
        for (InstancesResult.Instance instance : instances) {
            final InstancesResult.WorkflowStatus status = instance.getStatus();
            LOGGER.info("status: " + status + ", instance: " + instance.getInstance());
            statuses.add(status);
        }

        Assert.assertEquals(Collections.frequency(statuses, InstancesResult.WorkflowStatus.RUNNING),
            runningCount, "Running Instances");
        Assert.assertEquals(Collections.frequency(statuses, InstancesResult.WorkflowStatus.SUSPENDED),
            suspendedCount, "Suspended Instances");
        Assert.assertEquals(Collections.frequency(statuses, InstancesResult.WorkflowStatus.WAITING),
            waitingCount, "Waiting Instances");
        Assert.assertEquals(Collections.frequency(statuses, InstancesResult.WorkflowStatus.KILLED),
            killedCount, "Killed Instances");
    }

    public static List<String> getWorkflowJobIds(InstancesResult instancesResult) {
        InstancesResult.Instance[] instances = instancesResult.getInstances();
        LOGGER.info("instances: " + Arrays.toString(instances));
        Assert.assertNotNull(instances, "instances should be not null");
        List<String> wfids = new ArrayList<String>();
        for (InstancesResult.Instance instance : instances) {
            LOGGER.warn("instance: " + instance + " , status: "
                    + instance.getStatus() +  ", logs : " + instance.getLogFile());
            if (instance.getStatus().name().equals("RUNNING") || instance.getStatus().name().equals("SUCCEEDED")) {
                wfids.add(instance.getLogFile());
            }
            if (instance.getStatus().name().equals("KILLED") || instance.getStatus().name().equals("WAITING")) {
                Assert.assertNull(instance.getLogFile());
            }
        }
        return wfids;
    }

    /**
     * Checks that expected number of failed instances matches actual number of failed ones.
     *
     * @param instancesResult kind of response from API which should contain information about
     *                        instances.
     * @param failCount number of instances which should be failed.
     */
    public static void validateFailedInstances(InstancesResult instancesResult, int failCount) {
        AssertUtil.assertSucceeded(instancesResult);
        int counter = 0;
        for (InstancesResult.Instance oneInstance : instancesResult.getInstances()) {
            if (oneInstance.getStatus() == InstancesResult.WorkflowStatus.FAILED) {
                counter++;
            }
        }
        Assert.assertEquals(counter, failCount, "Actual number of failed instances does not "
                + "match expected number of failed instances.");
    }

    public static List<String> getWorkflows(ColoHelper prismHelper, String processName,
            WorkflowJob.Status... statuses) throws OozieClientException {
        OozieClient oozieClient = prismHelper.getClusterHelper().getOozieClient();
        String bundleID = OozieUtil.getBundles(oozieClient, processName, EntityType.PROCESS).get(0);
        List<String> workflowJobIds = OozieUtil.getWorkflowJobs(prismHelper, bundleID);

        List<String> toBeReturned = new ArrayList<String>();
        for (String jobId : workflowJobIds) {
            WorkflowJob wfJob = oozieClient.getJobInfo(jobId);
            LOGGER.info("wfJob.getId(): " + wfJob.getId() + " wfJob.getStartTime(): "
                + wfJob.getStartTime()
                + "jobId: " + jobId + "  wfJob.getStatus(): " + wfJob.getStatus());
            if (statuses.length == 0 || Arrays.asList(statuses).contains(wfJob.getStatus())) {
                toBeReturned.add(jobId);
            }
        }
        return toBeReturned;
    }

    public static boolean isWorkflowRunning(OozieClient oozieClient, String workflowID) throws
            OozieClientException {
        WorkflowJob.Status status = oozieClient.getJobInfo(workflowID).getStatus();
        return status == WorkflowJob.Status.RUNNING;
    }

    public static void areWorkflowsRunning(OozieClient oozieClient, List<String> workflowIds,
            int totalWorkflows, int runningWorkflows, int killedWorkflows,
            int succeededWorkflows) throws OozieClientException {
        if (totalWorkflows != -1) {
            Assert.assertEquals(workflowIds.size(), totalWorkflows);
        }
        final List<WorkflowJob.Status> statuses = new ArrayList<WorkflowJob.Status>();
        for (String wfId : workflowIds) {
            final WorkflowJob.Status status = oozieClient.getJobInfo(wfId).getStatus();
            LOGGER.info("wfId: " + wfId + " status: " + status);
            statuses.add(status);
        }
        if (runningWorkflows != -1) {
            Assert.assertEquals(Collections.frequency(statuses, WorkflowJob.Status.RUNNING),
                runningWorkflows, "Number of running jobs doesn't match.");
        }
        if (killedWorkflows != -1) {
            Assert.assertEquals(Collections.frequency(statuses, WorkflowJob.Status.KILLED),
                killedWorkflows, "Number of killed jobs doesn't match.");
        }
        if (succeededWorkflows != -1) {
            Assert.assertEquals(Collections.frequency(statuses, WorkflowJob.Status.SUCCEEDED),
                succeededWorkflows, "Number of succeeded jobs doesn't match.");
        }
    }

    public static List<CoordinatorAction> getProcessInstanceList(ColoHelper coloHelper,
            String processName, EntityType entityType) throws OozieClientException {
        OozieClient oozieClient = coloHelper.getProcessHelper().getOozieClient();
        String coordId = getLatestCoordinatorID(oozieClient, processName, entityType);
        //String coordId = getDefaultCoordinatorFromProcessName(processName);
        LOGGER.info("default coordID: " + coordId);
        return oozieClient.getCoordJobInfo(coordId).getActions();
    }

    public static String getLatestCoordinatorID(OozieClient oozieClient, String processName,
            EntityType entityType) throws OozieClientException {
        final String latestBundleID = getLatestBundleID(oozieClient, processName, entityType);
        return getDefaultCoordIDFromBundle(oozieClient, latestBundleID);
    }

    public static String getDefaultCoordIDFromBundle(OozieClient oozieClient, String bundleId)
        throws OozieClientException {
        OozieUtil.waitForCoordinatorJobCreation(oozieClient, bundleId);
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

    public static int getInstanceCountWithStatus(ColoHelper coloHelper, String processName,
            org.apache.oozie.client.CoordinatorAction.Status status, EntityType entityType)
        throws OozieClientException {
        List<CoordinatorAction> coordActions = getProcessInstanceList(coloHelper, processName,
            entityType);
        List<CoordinatorAction.Status> statuses = new ArrayList<CoordinatorAction.Status>();
        for (CoordinatorAction action : coordActions) {
            statuses.add(action.getStatus());
        }
        return Collections.frequency(statuses, status);
    }

    public static Status getDefaultCoordinatorStatus(ColoHelper colohelper, String processName,
            int bundleNumber) throws OozieClientException {
        OozieClient oozieClient = colohelper.getProcessHelper().getOozieClient();
        String bundleID =
                getSequenceBundleID(oozieClient, processName, EntityType.PROCESS, bundleNumber);
        String coordId = getDefaultCoordIDFromBundle(oozieClient, bundleID);
        return oozieClient.getCoordJobInfo(coordId).getStatus();
    }

    /**
     * Retrieves all coordinators of bundle.
     *
     * @param oozieClient Oozie client to use for fetching info.
     * @param bundleID specific bundle ID
     * @return list of bundle coordinators
     * @throws OozieClientException
     */
    public static List<CoordinatorJob> getBundleCoordinators(OozieClient oozieClient,
            String bundleID) throws OozieClientException {
        BundleJob bundleInfo = oozieClient.getBundleJobInfo(bundleID);
        return bundleInfo.getCoordinators();
    }

    /**
     * Retrieves the latest bundle ID.
     *
     * @param coloHelper colo helper of cluster job is running on
     * @param entityName name of entity job is related to
     * @param entityType type of entity - feed or process expected
     * @return latest bundle ID
     * @throws OozieClientException
     */
    public static String getLatestBundleID(ColoHelper coloHelper,
            String entityName, EntityType entityType)
        throws OozieClientException {
        final OozieClient oozieClient = coloHelper.getFeedHelper().getOozieClient();
        return getLatestBundleID(oozieClient, entityName, entityType);
    }

    /**
     * Retrieves the latest bundle ID.
     *
     * @param oozieClient where job is running
     * @param entityName  name of entity job is related to
     * @param entityType  type of entity - feed or process expected
     * @return latest bundle ID
     * @throws OozieClientException
     */
    public static String getLatestBundleID(OozieClient oozieClient,
            String entityName, EntityType entityType) throws OozieClientException {
        List<String> bundleIds = OozieUtil.getBundles(oozieClient, entityName, entityType);
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
     * Retrieves ID of bundle related to some process/feed using its ordinal number.
     *
     * @param entityName   - name of entity bundle is related to
     * @param entityType   - feed or process
     * @param bundleNumber - ordinal number of bundle
     * @return bundle ID
     * @throws OozieClientException
     */
    public static String getSequenceBundleID(OozieClient oozieClient, String entityName,
            EntityType entityType, int bundleNumber)
        throws OozieClientException {

        //sequence start from 0
        List<String> bundleIds = OozieUtil.getBundles(oozieClient,
                entityName, entityType);
        Map<Integer, String> bundleMap = new TreeMap<Integer, String>();
        String bundleID;
        for (String strID : bundleIds) {
            LOGGER.info("getSequenceBundleID: " + strID);
            int key = Integer.parseInt(strID.substring(0, strID.indexOf('-')));
            bundleMap.put(key, strID);
        }
        for (Map.Entry<Integer, String> entry : bundleMap.entrySet()) {
            LOGGER.info("Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }
        int i = 0;
        for (Map.Entry<Integer, String> entry : bundleMap.entrySet()) {
            bundleID = entry.getValue();
            if (i == bundleNumber) {
                return bundleID;
            }
            i++;
        }
        return null;
    }

    /**
     * Retrieves status of one instance.
     *
     * @param coloHelper     - server from which instance status will be retrieved.
     * @param processName    - name of process which mentioned instance belongs to.
     * @param bundleNumber   - ordinal number of one of the bundle which are related to that
     *                         process.
     * @param instanceNumber - ordinal number of instance which state will be returned.
     * @return - state of mentioned instance.
     * @throws OozieClientException
     */
    public static CoordinatorAction.Status getInstanceStatus(ColoHelper coloHelper,
            String processName,
            int bundleNumber, int
            instanceNumber) throws OozieClientException {
        final OozieClient oozieClient = coloHelper.getClusterHelper().getOozieClient();
        String bundleID =
            getSequenceBundleID(oozieClient, processName, EntityType.PROCESS, bundleNumber);
        if (StringUtils.isEmpty(bundleID)) {
            return null;
        }
        String coordID = InstanceUtil.getDefaultCoordIDFromBundle(oozieClient, bundleID);
        if (StringUtils.isEmpty(coordID)) {
            return null;
        }
        CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);
        if (coordInfo == null) {
            return null;
        }
        LOGGER.info("coordInfo = " + coordInfo);
        List<CoordinatorAction> actions = coordInfo.getActions();
        if (actions.size() == 0) {
            return null;
        }
        LOGGER.info("actions = " + actions);
        return actions.get(instanceNumber).getStatus();
    }

    /**
     * Retrieves replication coordinatorID from bundle of coordinators.
     */
    public static List<String> getReplicationCoordID(String bundleId, AbstractEntityHelper helper)
        throws OozieClientException {
        final OozieClient oozieClient = helper.getOozieClient();
        List<CoordinatorJob> coords = InstanceUtil.getBundleCoordinators(oozieClient, bundleId);
        List<String> replicationCoordID = new ArrayList<String>();
        for (CoordinatorJob coord : coords) {
            if (coord.getAppName().contains("FEED_REPLICATION")) {
                replicationCoordID.add(coord.getId());
            }
        }
        return replicationCoordID;
    }

    /**
     * Forms and sends process instance request based on url of action to be performed and it's
     * parameters.
     *
     * @param colo - servers on which action should be performed
     * @param user - whose credentials will be used for this action
     * @return result from API
     */
    public static APIResult createAndSendRequestProcessInstance(
            String url, String params, String colo, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        if (params != null && !colo.equals("")) {
            url = url + params + "&" + colo.substring(1);
        } else if (params != null) {
            url = url + params;
        } else {
            url = url + colo;
        }
        return InstanceUtil.sendRequestProcessInstance(url, user);
    }

    public static org.apache.oozie.client.WorkflowJob.Status getInstanceStatusFromCoord(
            ColoHelper coloHelper, String coordID, int instanceNumber) throws OozieClientException {
        OozieClient oozieClient = coloHelper.getProcessHelper().getOozieClient();
        CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);
        String jobId = coordInfo.getActions().get(instanceNumber).getExternalId();
        LOGGER.info("jobId = " + jobId);
        if (jobId == null) {
            return null;
        }
        WorkflowJob actionInfo = oozieClient.getJobInfo(jobId);
        return actionInfo.getStatus();
    }

    public static List<String> getInputFoldersForInstanceForReplication(
            ColoHelper coloHelper, String coordID, int instanceNumber) throws OozieClientException {
        OozieClient oozieClient = coloHelper.getProcessHelper().getOozieClient();
        CoordinatorAction x = oozieClient.getCoordActionInfo(coordID + "@" + instanceNumber);
        String jobId = x.getExternalId();
        WorkflowJob wfJob = oozieClient.getJobInfo(jobId);
        return InstanceUtil.getReplicationFolderFromInstanceRunConf(wfJob.getConf());
    }

    public static List<String> getReplicationFolderFromInstanceRunConf(String runConf) {
        String conf;
        conf = runConf.substring(runConf.indexOf("falconInPaths</name>") + 20);
        conf = conf.substring(conf.indexOf("<value>") + 7);
        conf = conf.substring(0, conf.indexOf("</value>"));
        return new ArrayList<String>(Arrays.asList(conf.split(",")));
    }

    public static int getInstanceRunIdFromCoord(ColoHelper colo, String coordID, int instanceNumber)
        throws OozieClientException {
        OozieClient oozieClient = colo.getProcessHelper().getOozieClient();
        CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);

        WorkflowJob actionInfo =
                oozieClient.getJobInfo(coordInfo.getActions().get(instanceNumber).getExternalId());
        return actionInfo.getRun();
    }

    public static int checkIfFeedCoordExist(AbstractEntityHelper helper,
            String feedName, String coordType) throws OozieClientException {
        LOGGER.info("feedName: " + feedName);
        int numberOfCoord = 0;

        final OozieClient oozieClient = helper.getOozieClient();
        if (OozieUtil.getBundles(oozieClient, feedName, EntityType.FEED).size() == 0) {
            return 0;
        }
        List<String> bundleIds = OozieUtil.getBundles(oozieClient, feedName, EntityType.FEED);
        LOGGER.info("bundleIds: " + bundleIds);

        for (String aBundleId : bundleIds) {
            LOGGER.info("aBundleId: " + aBundleId);
            OozieUtil.waitForCoordinatorJobCreation(oozieClient, aBundleId);
            List<CoordinatorJob> coords =
                    InstanceUtil.getBundleCoordinators(oozieClient, aBundleId);
            LOGGER.info("coords: " + coords);
            for (CoordinatorJob coord : coords) {
                if (coord.getAppName().contains(coordType)) {
                    numberOfCoord++;
                }
            }
        }
        return numberOfCoord;
    }

    public static List<CoordinatorAction> getProcessInstanceListFromAllBundles(
            ColoHelper coloHelper, String processName, EntityType entityType)
        throws OozieClientException {
        OozieClient oozieClient = coloHelper.getProcessHelper().getOozieClient();
        List<CoordinatorAction> list = new ArrayList<CoordinatorAction>();
        final List<String> bundleIds = OozieUtil.getBundles(oozieClient, processName, entityType);
        LOGGER.info("bundle size for process is " + bundleIds.size());
        for (String bundleId : bundleIds) {
            BundleJob bundleInfo = oozieClient.getBundleJobInfo(bundleId);
            List<CoordinatorJob> coordJobs = bundleInfo.getCoordinators();
            LOGGER.info("number of coordJobs in bundle " + bundleId + "=" + coordJobs.size());
            for (CoordinatorJob coordJob : coordJobs) {
                List<CoordinatorAction> actions =
                        oozieClient.getCoordJobInfo(coordJob.getId()).getActions();
                LOGGER.info("number of actions in coordinator " + coordJob.getId() + " is "
                        + actions.size());
                list.addAll(actions);
            }
        }
        String coordId = getLatestCoordinatorID(oozieClient, processName, entityType);
        LOGGER.info("default coordID: " + coordId);
        return list;
    }

    public static String getOutputFolderForInstanceForReplication(ColoHelper coloHelper,
            String coordID, int instanceNumber) throws OozieClientException {
        OozieClient oozieClient = coloHelper.getProcessHelper().getOozieClient();
        CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);
        final CoordinatorAction coordAction = coordInfo.getActions().get(instanceNumber);
        final String actionConf = oozieClient.getJobInfo(coordAction.getExternalId()).getConf();
        return InstanceUtil.getReplicatedFolderFromInstanceRunConf(actionConf);
    }

    private static String getReplicatedFolderFromInstanceRunConf(
            String runConf) {
        String inputPathExample =
                InstanceUtil.getReplicationFolderFromInstanceRunConf(runConf).get(0);
        String postFix = inputPathExample
                .substring(inputPathExample.length() - 7, inputPathExample.length());
        return getReplicatedFolderBaseFromInstanceRunConf(runConf) + postFix;
    }

    public static String getOutputFolderBaseForInstanceForReplication(
            ColoHelper coloHelper, String coordID, int instanceNumber) throws OozieClientException {
        OozieClient oozieClient = coloHelper.getProcessHelper().getOozieClient();
        CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);

        final CoordinatorAction coordAction = coordInfo.getActions().get(instanceNumber);
        final String actionConf = oozieClient.getJobInfo(coordAction.getExternalId()).getConf();
        return InstanceUtil.getReplicatedFolderBaseFromInstanceRunConf(actionConf);
    }

    private static String getReplicatedFolderBaseFromInstanceRunConf(String runConf) {
        String conf = runConf.substring(runConf.indexOf("distcpTargetPaths</name>") + 24);
        conf = conf.substring(conf.indexOf("<value>") + 7);
        conf = conf.substring(0, conf.indexOf("</value>"));
        return conf;
    }

    /**
     * Waits till supplied number of instances of process/feed reach expected state during
     * specific time.
     *
     * @param client             oozie client to retrieve info about instances
     * @param entityName         name of feed or process
     * @param instancesNumber    instance number for which we wait to reach the required status
     * @param expectedStatus     expected status we are waiting for
     * @param entityType         type of entity - feed or process expected
     * @param totalMinutesToWait time in minutes for which instance state should be polled
     * @throws OozieClientException
     */
    public static void waitTillInstanceReachState(OozieClient client, String entityName,
            int instancesNumber,
            CoordinatorAction.Status expectedStatus,
            EntityType entityType, int totalMinutesToWait)
        throws OozieClientException {
        String filter;
        // get the bundle ids
        if (entityType.equals(EntityType.FEED)) {
            filter = "name=FALCON_FEED_" + entityName;
        } else {
            filter = "name=FALCON_PROCESS_" + entityName;
        }
        List<BundleJob> bundleJobs = new ArrayList<BundleJob>();
        for (int retries = 0; retries < 20; ++retries) {
            bundleJobs = OozieUtil.getBundles(client, filter, 0, 10);
            if (bundleJobs.size() > 0) {
                break;
            }
            TimeUtil.sleepSeconds(5);
        }
        if (bundleJobs.size() == 0) {
            Assert.assertTrue(false, "Could not retrieve bundles");
        }
        List<String> bundleIds = OozieUtil.getBundleIds(bundleJobs);
        String bundleId = OozieUtil.getMaxId(bundleIds);
        LOGGER.info(String.format("Using bundle %s", bundleId));
        final String coordId;
        final Status bundleStatus = client.getBundleJobInfo(bundleId).getStatus();
        Assert.assertTrue(RUNNING_PREP_SUCCEEDED.contains(bundleStatus),
                String.format("Bundle job %s is should be prep/running but is %s", bundleId, bundleStatus));
        OozieUtil.waitForCoordinatorJobCreation(client, bundleId);
        List<CoordinatorJob> coords = client.getBundleJobInfo(bundleId).getCoordinators();
        List<String> cIds = new ArrayList<String>();
        if (entityType == EntityType.PROCESS) {
            for (CoordinatorJob coord : coords) {
                cIds.add(coord.getId());
            }
            coordId = OozieUtil.getMinId(cIds);
        } else {
            for (CoordinatorJob coord : coords) {
                if (coord.getAppName().contains("FEED_REPLICATION")) {
                    cIds.add(coord.getId());
                }
            }
            coordId = cIds.get(0);
        }
        LOGGER.info(String.format("Using coordinator id: %s", coordId));
        int maxTries = 50;
        int totalSleepTime = totalMinutesToWait * 60;
        int sleepTime = totalSleepTime / maxTries;
        LOGGER.info(String.format("Sleep for %d seconds", sleepTime));
        for (int i = 0; i < maxTries; i++) {
            LOGGER.info(String.format("Try %d of %d", (i + 1), maxTries));
            CoordinatorJob coordinatorJob = client.getCoordJobInfo(coordId);
            final Status coordinatorStatus = coordinatorJob.getStatus();
            if (expectedStatus != CoordinatorAction.Status.TIMEDOUT){
                Assert.assertTrue(RUNNING_PREP_SUCCEEDED.contains(coordinatorStatus),
                        String.format("Coordinator %s should be running/prep but is %s.", coordId, coordinatorStatus));
            }
            List<CoordinatorAction> coordinatorActions = coordinatorJob.getActions();
            int instanceWithStatus = 0;
            for (CoordinatorAction coordinatorAction : coordinatorActions) {
                LOGGER.info(String.format("Coordinator Action %s status is %s on oozie %s",
                    coordinatorAction.getId(), coordinatorAction.getStatus(), client.getOozieUrl()));
                if (expectedStatus == coordinatorAction.getStatus()) {
                    instanceWithStatus++;
                }
            }
            if (instanceWithStatus >= instancesNumber) {
                return;
            } else {
                TimeUtil.sleepSeconds(sleepTime);
            }
        }
        Assert.assertTrue(false, "expected state of instance was never reached");
    }

    /**
     * Waits till supplied number of instances of process/feed reach expected state during
     * specific time.
     *
     * @param client           oozie client to retrieve info about instances
     * @param entityName       name of feed or process
     * @param numberOfInstance number of instances which status we are waiting for
     * @param expectedStatus   expected status we are waiting for
     * @param entityType       type of entity - feed or process expected
     */
    public static void waitTillInstanceReachState(OozieClient client, String entityName,
            int numberOfInstance,
            CoordinatorAction.Status expectedStatus,
            EntityType entityType)
        throws OozieClientException {
        int totalMinutesToWait = getMinutesToWait(entityType, expectedStatus);
        waitTillInstanceReachState(client, entityName, numberOfInstance, expectedStatus,
                entityType, totalMinutesToWait);
    }

    /**
     * Waits till bundle job will reach expected status.
     * Generates time according to expected status.
     *
     * @param coloHelper     colo helper of cluster job is running on
     * @param processName    name of process which job is being analyzed
     * @param expectedStatus job status we are waiting for
     * @throws OozieClientException
     */
    public static void waitForBundleToReachState(ColoHelper coloHelper,
            String processName, Job.Status expectedStatus) throws
            OozieClientException {
        int totalMinutesToWait = getMinutesToWait(expectedStatus);
        waitForBundleToReachState(coloHelper, processName, expectedStatus, totalMinutesToWait);
    }

    /**
     * Waits till bundle job will reach expected status during specific time.
     * Use it directly in test cases when timeouts are different from trivial, in other cases use
     * waitForBundleToReachState(ColoHelper, String, Status)
     *
     * @param coloHelper         colo helper of cluster job is running on
     * @param processName        name of process which job is being analyzed
     * @param expectedStatus     job status we are waiting for
     * @param totalMinutesToWait specific time to wait expected state
     * @throws OozieClientException
     */
    public static void waitForBundleToReachState(ColoHelper coloHelper,
            String processName, Job.Status expectedStatus, int totalMinutesToWait) throws
            OozieClientException {

        int sleep = totalMinutesToWait * 60 / 20;
        for (int sleepCount = 0; sleepCount < sleep; sleepCount++) {
            String bundleID =
                    InstanceUtil.getLatestBundleID(coloHelper, processName, EntityType.PROCESS);
            OozieClient oozieClient =
                    coloHelper.getProcessHelper().getOozieClient();
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
     * Generates time which is presumably needed for process/feed instances to reach particular
     * state.
     * Feed instances are running faster then process, so feed timeouts are less then process.
     *
     * @param entityType     type of entity which instances status we are waiting for
     * @param expectedStatus expected status we are waiting for
     * @return minutes to wait for expected status
     */
    private static int getMinutesToWait(EntityType entityType,
            CoordinatorAction.Status expectedStatus) {
        switch (expectedStatus) {
        case RUNNING:
            if (entityType == EntityType.PROCESS) {
                return OSUtil.IS_WINDOWS ? 20 : 10;
            } else if (entityType == EntityType.FEED) {
                return OSUtil.IS_WINDOWS ? 10 : 5;
            }
        case WAITING:
            return OSUtil.IS_WINDOWS ? 6 : 3;
        case SUCCEEDED:
            if (entityType == EntityType.PROCESS) {
                return OSUtil.IS_WINDOWS ? 25 : 15;
            } else if (entityType == EntityType.FEED) {
                return OSUtil.IS_WINDOWS ? 20 : 10;
            }
        case KILLED:
        case TIMEDOUT:
            return OSUtil.IS_WINDOWS ? 40 : 20;
        default:
            return OSUtil.IS_WINDOWS ? 30 : 15;
        }
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

    /**
     * Waits till instances of specific job will be created during specific time.
     * Use this method directly in unusual test cases where timeouts are different from trivial.
     * In other cases use waitTillInstancesAreCreated(ColoHelper,String,int)
     *
     * @param oozieClient oozie client of the cluster on which job is running
     * @param entity      definition of entity which describes job
     * @param bundleSeqNo bundle number if update has happened.
     * @throws OozieClientException
     */
    public static void waitTillInstancesAreCreated(OozieClient oozieClient,
            String entity,
            int bundleSeqNo,
            int totalMinutesToWait
    ) throws OozieClientException {
        String entityName = Util.readEntityName(entity);
        EntityType type = Util.getEntityType(entity);
        String bundleID = getSequenceBundleID(oozieClient, entityName,
            type, bundleSeqNo);
        String coordID = getDefaultCoordIDFromBundle(oozieClient, bundleID);
        for (int sleepCount = 0; sleepCount < totalMinutesToWait; sleepCount++) {
            CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);

            if (coordInfo.getActions().size() > 0) {
                break;
            }
            LOGGER.info("Coord " + coordInfo.getId() + " still doesn't have "
                + "instance created on oozie: " + oozieClient.getOozieUrl());
            TimeUtil.sleepSeconds(5);
        }
    }

    /**
     * Waits till instances of specific job will be created during timeout.
     * Timeout is common for most of usual test cases.
     *
     * @param coloHelper  colo helper of cluster job is running on
     * @param entity      definition of entity which describes job
     * @param bundleSeqNo bundle number if update has happened.
     * @throws OozieClientException
     */
    public static void waitTillInstancesAreCreated(ColoHelper coloHelper,
            String entity,
            int bundleSeqNo
    ) throws OozieClientException {
        int sleep = INSTANCES_CREATED_TIMEOUT * 60 / 5;
        final OozieClient oozieClient = coloHelper.getClusterHelper().getOozieClient();
        waitTillInstancesAreCreated(oozieClient, entity, bundleSeqNo, sleep);
    }
}

