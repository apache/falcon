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

/**
 * util functions related to instanceTest.
 */
public final class InstanceUtil {

    public static final int INSTANCES_CREATED_TIMEOUT = OSUtil.IS_WINDOWS ? 20 : 10;
    private static final Logger LOGGER = Logger.getLogger(InstanceUtil.class);
    private static final EnumSet<Status> RUNNING_PREP_SUCCEEDED = EnumSet.of(Status.RUNNING,
        Status.PREP, Status.SUCCEEDED);

    private InstanceUtil() {
        throw new AssertionError("Instantiating utility class...");
    }

    public static APIResult sendRequestProcessInstance(String url, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        return hitUrl(url, Util.getMethodType(url), user);
    }

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
            new ArrayList<>();
        for (InstancesResult.Instance instance : instances) {
            LOGGER.info("instance: " + instance + " status = " + instance.getStatus());
            statuses.add(instance.getStatus());
        }
        return Collections.frequency(statuses, workflowStatus);
    }

    /**
     * Validates that response doesn't contains instances.
     * @param r response
     */
    public static void validateSuccessWOInstances(InstancesResult r) {
        AssertUtil.assertSucceeded(r);
        Assert.assertNull(r.getInstances(), "Unexpected :" + Arrays.toString(r.getInstances()));
    }

    /**
     * Validates that failed response contains specific error message.
     * @param instancesResult response
     * @param error expected error
     */
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
        List<InstancesResult.WorkflowStatus> statuses = new ArrayList<>();
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

    /**
     * Retrieves workflow IDs from every instances from response.
     * @param instancesResult response
     * @return list of workflow IDs
     */
    public static List<String> getWorkflowJobIds(InstancesResult instancesResult) {
        InstancesResult.Instance[] instances = instancesResult.getInstances();
        LOGGER.info("Instances: " + Arrays.toString(instances));
        Assert.assertNotNull(instances, "Instances should be not null");
        List<String> wfIds = new ArrayList<>();
        for (InstancesResult.Instance instance : instances) {
            LOGGER.warn(String.format(
                "instance: %s, status: %s, logs : %s", instance, instance.getStatus(), instance.getLogFile()));
            if (instance.getStatus().name().equals("RUNNING") || instance.getStatus().name().equals("SUCCEEDED")) {
                wfIds.add(instance.getLogFile());
            }
            if (instance.getStatus().name().equals("KILLED") || instance.getStatus().name().equals("WAITING")) {
                Assert.assertNull(instance.getLogFile());
            }
        }
        return wfIds;
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
            + "match to expected number of failed instances.");
    }

    /**
     * Gets process workflows by given statuses.
     * @param oozieClient oozie client of cluster where process is running
     * @param processName process name
     * @param statuses statuses workflows will be selected by
     * @return list of matching workflows
     * @throws OozieClientException
     */
    public static List<String> getWorkflows(OozieClient oozieClient, String processName,
            WorkflowJob.Status... statuses) throws OozieClientException {
        String bundleID = OozieUtil.getBundles(oozieClient, processName, EntityType.PROCESS).get(0);
        List<String> workflowJobIds = OozieUtil.getWorkflowJobs(oozieClient, bundleID);

        List<String> toBeReturned = new ArrayList<>();
        for (String jobId : workflowJobIds) {
            WorkflowJob wfJob = oozieClient.getJobInfo(jobId);
            LOGGER.info("wfJob.getId(): " + wfJob.getId() + " wfJob.getStartTime(): "
                + wfJob.getStartTime() + "jobId: " + jobId + "  wfJob.getStatus(): " + wfJob.getStatus());
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
        final List<WorkflowJob.Status> statuses = new ArrayList<>();
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

    public static List<CoordinatorAction> getProcessInstanceList(OozieClient oozieClient,
            String processName, EntityType entityType) throws OozieClientException {
        String coordId = OozieUtil.getLatestCoordinatorID(oozieClient, processName, entityType);
        //String coordId = getDefaultCoordinatorFromProcessName(processName);
        LOGGER.info("default coordID: " + coordId);
        return oozieClient.getCoordJobInfo(coordId).getActions();
    }

    public static int getInstanceCountWithStatus(OozieClient oozieClient, String processName,
            CoordinatorAction.Status status, EntityType entityType) throws OozieClientException {
        List<CoordinatorAction> coordActions = getProcessInstanceList(oozieClient, processName, entityType);
        List<CoordinatorAction.Status> statuses = new ArrayList<>();
        for (CoordinatorAction action : coordActions) {
            statuses.add(action.getStatus());
        }
        return Collections.frequency(statuses, status);
    }

    /**
     * Retrieves status of one instance.
     *
     * @param oozieClient     - server from which instance status will be retrieved.
     * @param processName    - name of process which mentioned instance belongs to.
     * @param bundleNumber   - ordinal number of one of the bundle which are related to that
     *                         process.
     * @param instanceNumber - ordinal number of instance which state will be returned.
     * @return - state of mentioned instance.
     * @throws OozieClientException
     */
    public static CoordinatorAction.Status getInstanceStatus(OozieClient oozieClient, String processName,
            int bundleNumber, int instanceNumber) throws OozieClientException {
        String bundleID = OozieUtil.getSequenceBundleID(oozieClient, processName, EntityType.PROCESS, bundleNumber);
        if (StringUtils.isEmpty(bundleID)) {
            return null;
        }
        String coordID = OozieUtil.getDefaultCoordIDFromBundle(oozieClient, bundleID);
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
     * Forms and sends process instance request based on url of action to be performed and it's
     * parameters.
     *
     * @param colo - servers on which action should be performed
     * @param user - whose credentials will be used for this action
     * @return result from API
     */
    public static APIResult createAndSendRequestProcessInstance(String url, String params, String colo, String user)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        if (params != null && !colo.equals("")) {
            url = url + params + "&" + colo.substring(1);
        } else if (params != null) {
            url = url + params;
        } else {
            url = url + colo;
        }
        return sendRequestProcessInstance(url, user);
    }

    public static org.apache.oozie.client.WorkflowJob.Status getInstanceStatusFromCoord(
            OozieClient oozieClient, String coordID, int instanceNumber) throws OozieClientException {
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
            OozieClient oozieClient, String coordID, int instanceNumber) throws OozieClientException {
        CoordinatorAction x = oozieClient.getCoordActionInfo(coordID + "@" + instanceNumber);
        String jobId = x.getExternalId();
        WorkflowJob wfJob = oozieClient.getJobInfo(jobId);
        return getReplicationFolderFromInstanceRunConf(wfJob.getConf());
    }

    private static List<String> getReplicationFolderFromInstanceRunConf(String runConf) {
        String conf;
        conf = runConf.substring(runConf.indexOf("falconInPaths</name>") + 20);
        conf = conf.substring(conf.indexOf("<value>") + 7);
        conf = conf.substring(0, conf.indexOf("</value>"));
        return new ArrayList<>(Arrays.asList(conf.split(",")));
    }

    public static int getInstanceRunIdFromCoord(OozieClient oozieClient, String coordID, int instanceNumber)
        throws OozieClientException {
        CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);
        WorkflowJob actionInfo = oozieClient.getJobInfo(coordInfo.getActions().get(instanceNumber).getExternalId());
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

        for (String bundleId : bundleIds) {
            LOGGER.info("bundleId: " + bundleId);
            OozieUtil.waitForCoordinatorJobCreation(oozieClient, bundleId);
            List<CoordinatorJob> coords =
                    OozieUtil.getBundleCoordinators(oozieClient, bundleId);
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
            OozieClient oozieClient, String processName, EntityType entityType)
        throws OozieClientException {
        List<CoordinatorAction> list = new ArrayList<>();
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
        String coordId = OozieUtil.getLatestCoordinatorID(oozieClient, processName, entityType);
        LOGGER.info("default coordID: " + coordId);
        return list;
    }

    public static String getOutputFolderForInstanceForReplication(OozieClient oozieClient,
            String coordID, int instanceNumber) throws OozieClientException {
        CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);
        final CoordinatorAction coordAction = coordInfo.getActions().get(instanceNumber);
        final String actionConf = oozieClient.getJobInfo(coordAction.getExternalId()).getConf();
        return getReplicatedFolderFromInstanceRunConf(actionConf);
    }

    private static String getReplicatedFolderFromInstanceRunConf(String runConf) {
        String inputPathExample = getReplicationFolderFromInstanceRunConf(runConf).get(0);
        String postFix = inputPathExample.substring(inputPathExample.length() - 7, inputPathExample.length());
        return getReplicatedFolderBaseFromInstanceRunConf(runConf) + postFix;
    }

    public static String getOutputFolderBaseForInstanceForReplication(
            OozieClient oozieClient, String coordID, int instanceNumber) throws OozieClientException {
        CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);
        final CoordinatorAction coordAction = coordInfo.getActions().get(instanceNumber);
        final String actionConf = oozieClient.getJobInfo(coordAction.getExternalId()).getConf();
        return getReplicatedFolderBaseFromInstanceRunConf(actionConf);
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
    public static void waitTillInstanceReachState(OozieClient client, String entityName, int instancesNumber,
            CoordinatorAction.Status expectedStatus, EntityType entityType, int totalMinutesToWait)
        throws OozieClientException {
        String filter;
        // get the bundle ids
        if (entityType.equals(EntityType.FEED)) {
            filter = "name=FALCON_FEED_" + entityName;
        } else {
            filter = "name=FALCON_PROCESS_" + entityName;
        }
        List<BundleJob> bundleJobs = new ArrayList<>();
        for (int retries = 0; retries < 20; ++retries) {
            bundleJobs = OozieUtil.getBundles(client, filter, 0, 10);
            if (bundleJobs.size() > 0) {
                break;
            }
            TimeUtil.sleepSeconds(5);
        }
        if (bundleJobs.size() == 0) {
            Assert.fail("Could not retrieve bundles");
        }
        List<String> bundleIds = OozieUtil.getBundleIds(bundleJobs);
        Collections.sort(bundleIds, Collections.reverseOrder());
        String coordId = null;
        for (String bundleId : bundleIds) {
            LOGGER.info(String.format("Using bundle %s", bundleId));
            final Status status = client.getBundleJobInfo(bundleId).getStatus();
            Assert.assertTrue(RUNNING_PREP_SUCCEEDED.contains(status),
                String.format("Bundle job %s is should be prep/running but is %s", bundleId, status));
            OozieUtil.waitForCoordinatorJobCreation(client, bundleId);
            List<CoordinatorJob> coords = client.getBundleJobInfo(bundleId).getCoordinators();
            List<String> cIds = new ArrayList<>();
            if (entityType == EntityType.PROCESS) {
                for (CoordinatorJob coord : coords) {
                    cIds.add(coord.getId());
                }
                coordId = OozieUtil.getMinId(cIds);
                break;
            } else {
                for (CoordinatorJob coord : coords) {
                    if (coord.getAppName().contains("FEED_REPLICATION")) {
                        cIds.add(coord.getId());
                    }
                }
                if (!cIds.isEmpty()) {
                    coordId = cIds.get(0);
                    break;
                }
            }
        }
        Assert.assertNotNull(coordId, "Coordinator id not found");
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
        Assert.fail("expected state of instance was never reached");
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
     * Generates time which is presumably needed for process/feed instances to reach particular
     * state.
     * Feed instances are running faster then process, so feed timeouts are less then process.
     *
     * @param entityType     type of entity which instances status we are waiting for
     * @param expectedStatus expected status we are waiting for
     * @return minutes to wait for expected status
     */
    private static int getMinutesToWait(EntityType entityType, CoordinatorAction.Status expectedStatus) {
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
     * Waits till instances of specific job will be created during specific time.
     * Use this method directly in unusual test cases where timeouts are different from trivial.
     * In other cases use waitTillInstancesAreCreated(OozieClient,String,int)
     *
     * @param oozieClient oozie client of the cluster on which job is running
     * @param entity      definition of entity which describes job
     * @param bundleSeqNo bundle number if update has happened.
     * @throws OozieClientException
     */
    public static void waitTillInstancesAreCreated(OozieClient oozieClient, String entity, int bundleSeqNo,
            int totalMinutesToWait) throws OozieClientException {
        String entityName = Util.readEntityName(entity);
        EntityType type = Util.getEntityType(entity);
        String bundleID = OozieUtil.getSequenceBundleID(oozieClient, entityName,
            type, bundleSeqNo);
        String coordID = OozieUtil.getDefaultCoordIDFromBundle(oozieClient, bundleID);
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
     * @param oozieClient  oozieClient of cluster job is running on
     * @param entity      definition of entity which describes job
     * @param bundleSeqNo bundle number if update has happened.
     * @throws OozieClientException
     */
    public static void waitTillInstancesAreCreated(OozieClient oozieClient, String entity, int bundleSeqNo
    ) throws OozieClientException {
        int sleep = INSTANCES_CREATED_TIMEOUT * 60 / 5;
        waitTillInstancesAreCreated(oozieClient, entity, bundleSeqNo, sleep);
    }
}

