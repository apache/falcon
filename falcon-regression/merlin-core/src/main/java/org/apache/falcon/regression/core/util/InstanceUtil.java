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
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.ACL;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.entity.v0.feed.Retention;
import org.apache.falcon.entity.v0.feed.Validity;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;
import org.apache.falcon.regression.core.response.APIResult;
import org.apache.falcon.regression.core.response.InstancesResult;
import org.apache.falcon.regression.core.response.InstancesSummaryResult;
import org.apache.falcon.regression.core.response.ResponseKeys;
import org.apache.falcon.request.BaseRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.testng.Assert;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
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

    public static APIResult sendRequestProcessInstance(String
            url, String user)
        throws IOException, URISyntaxException, AuthenticationException {
        return hitUrl(url, Util.getMethodType(url), user);
    }

    public static final int INSTANCES_CREATED_TIMEOUT = OSUtil.IS_WINDOWS ? 20 : 10;

    public static APIResult hitUrl(String url,
            String method, String user) throws URISyntaxException,
            IOException, AuthenticationException {
        BaseRequest request = new BaseRequest(url, method, user);
        HttpResponse response = request.run();
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(response.getEntity().getContent(), "UTF-8"));
        StringBuilder stringResponse = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            stringResponse.append(line).append("\n");
        }
        String jsonString = stringResponse.toString();
        LOGGER.info("The web service response is:\n" + Util.prettyPrintXmlOrJson(jsonString));
        APIResult r = null;
        try {
            if (url.contains("/summary/")) {
                //Order is not guaranteed in the getDeclaredConstructors() call
                Constructor<?>[] constructors = InstancesSummaryResult.class
                        .getDeclaredConstructors();
                for (Constructor<?> constructor : constructors) {
                    //we want to invoke the constructor that has no parameters
                    if (constructor.getParameterTypes().length == 0) {
                        constructor.setAccessible(true);
                        r = (InstancesSummaryResult) constructor.newInstance();
                        break;
                    }
                }
            } else {
                //Order is not guaranteed in the getDeclaredConstructors() call
                Constructor<?>[] constructors = InstancesResult.class
                        .getDeclaredConstructors();
                for (Constructor<?> constructor : constructors) {
                    //we want to invoke the constructor that has no parameters
                    if (constructor.getParameterTypes().length == 0) {
                        constructor.setAccessible(true);
                        r = (InstancesResult) constructor.newInstance();
                        break;
                    }
                }
            }
        } catch (IllegalAccessException e) {
            Assert.fail("Could not create InstancesSummaryResult or "
                    +
                    "InstancesResult constructor\n" + ExceptionUtils.getStackTrace(e));
        } catch (InstantiationException e) {
            Assert.fail("Could not create InstancesSummaryResult or "
                    +
                    "InstancesResult constructor\n" + ExceptionUtils.getStackTrace(e));
        } catch (InvocationTargetException e) {
            Assert.fail("Could not create InstancesSummaryResult or "
                    +
                    "InstancesResult constructor\n" + ExceptionUtils.getStackTrace(e));
        }
        Assert.assertNotNull(r, "APIResult is null");
        if (jsonString.contains("(PROCESS) not found")) {
            r.setStatusCode(ResponseKeys.PROCESS_NOT_FOUND);
            return r;
        } else if (jsonString.contains("Parameter start is empty")
                ||
                jsonString.contains("Unparseable date:")) {
            r.setStatusCode(ResponseKeys.UNPARSEABLE_DATE);
            return r;
        } else if (response.getStatusLine().getStatusCode() == 400
                &&
                jsonString.contains("(FEED) not found")) {
            r.setStatusCode(400);
            return r;
        } else if (
                (response.getStatusLine().getStatusCode() == 400
                        &&
                        jsonString.contains("is beforePROCESS  start"))
                        ||
                        response.getStatusLine().getStatusCode() == 400
                                &&
                                jsonString.contains("is after end date")
                        || (response.getStatusLine().getStatusCode() == 400
                        &&
                        jsonString.contains("is after PROCESS's end"))
                        ||
                        (response.getStatusLine().getStatusCode() == 400
                                &&
                                jsonString.contains("is before PROCESS's  start"))) {
            r.setStatusCode(400);
            return r;
        }
        if (url.contains("/summary/")) {
            r = new GsonBuilder().setPrettyPrinting().create()
                    .fromJson(jsonString, InstancesSummaryResult.class);
        } else {

            r = new GsonBuilder().create()
                    .fromJson(jsonString, InstancesResult.class);
        }

        LOGGER.info("r.getMessage(): " + r.getMessage());
        LOGGER.info("r.getStatusCode(): " + r.getStatusCode());
        LOGGER.info("r.getStatus() " + r.getStatus());
        return r;
    }

    /**
     * Checks if API response reflects success and if it's instances match to expected status.
     *
     * @param r  - kind of response from API which should contain information about instances
     * @param b  - bundle from which process instances are being analyzed
     * @param ws - - expected status of instances
     */
    public static void validateSuccess(InstancesResult r, Bundle b,
            InstancesResult.WorkflowStatus ws) {
        Assert.assertEquals(r.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertEquals(runningInstancesInResult(r, ws), b.getProcessConcurrency());
    }

    /**
     * Check the number of instances in response which have the same status as expected.
     *
     * @param r  kind of response from API which should contain information about instances
     * @param ws expected status of instances
     * @return number of instances which have expected status
     */
    public static int runningInstancesInResult(InstancesResult r,
            InstancesResult.WorkflowStatus ws) {
        InstancesResult.Instance[] pArray = r.getInstances();
        int runningCount = 0;
        LOGGER.info("pArray: " + Arrays.toString(pArray));
        for (int instanceIndex = 0; instanceIndex < pArray.length; instanceIndex++) {
            LOGGER.info(
                    "pArray[" + instanceIndex + "]: " + pArray[instanceIndex].getStatus() + " , "
                            +
                            pArray[instanceIndex].getInstance()
            );

            if (pArray[instanceIndex].getStatus() == ws) {
                runningCount++;
            }
        }
        return runningCount;
    }

    public static void validateSuccessWOInstances(InstancesResult r) {
        AssertUtil.assertSucceeded(r);
        Assert.assertNull(r.getInstances(), "Unexpected :" + Arrays.toString(r.getInstances()));
    }

    public static void validateSuccessWithStatusCode(InstancesResult r,
            int expectedErrorCode) {
        Assert.assertEquals(r.getStatusCode(), expectedErrorCode,
                "Parameter start is empty should have the response");
    }

    /**
     * Checks that API action succeed and the instance on which it has been performed on has
     * expected status.
     *
     * @param r  kind of response from API which should contain information about instance
     * @param ws expected status of instance
     */
    public static void validateSuccessOnlyStart(InstancesResult r,
            InstancesResult.WorkflowStatus ws) {
        Assert.assertEquals(r.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertEquals(1, runningInstancesInResult(r, ws));
    }

    /**
     * Checks that actual number of instances with different statuses are equal to expected number
     * of instances with matching statuses.
     *
     * @param r                  kind of response from API which should contain information about instances
     *                           <p/>
     *                           All parameters below reflect number of expected instances with some kind of status.
     * @param totalInstances     total instance.
     * @param runningInstances   number of running instances.
     * @param suspendedInstances number of suspended instance.
     * @param waitingInstances   number of waiting instance.
     * @param killedInstances    number of killed instance.
     */
    public static void validateResponse(InstancesResult r, int totalInstances,
            int runningInstances,
            int suspendedInstances, int waitingInstances,
            int killedInstances) {

        int actualRunningInstances = 0;
        int actualSuspendedInstances = 0;
        int actualWaitingInstances = 0;
        int actualKilledInstances = 0;
        InstancesResult.Instance[] pArray = r.getInstances();
        LOGGER.info("pArray: " + Arrays.toString(pArray));
        Assert.assertNotNull(pArray, "pArray should be not null");
        Assert.assertEquals(pArray.length, totalInstances, "Total Instances");
        for (int instanceIndex = 0; instanceIndex < pArray.length; instanceIndex++) {
            LOGGER.info(
                    "pArray[" + instanceIndex + "]: " + pArray[instanceIndex].getStatus() + " , "
                            +
                            pArray[instanceIndex].getInstance());

            switch (pArray[instanceIndex].getStatus()) {
            case RUNNING:
                actualRunningInstances++;
                break;
            case SUSPENDED:
                actualSuspendedInstances++;
                break;
            case WAITING:
                actualWaitingInstances++;
                break;
            case KILLED:
                actualKilledInstances++;
                break;
            default:
                Assert.fail("Unexpected status=" + pArray[instanceIndex].getStatus());
            }
        }
        Assert.assertEquals(actualRunningInstances, runningInstances, "Running Instances");
        Assert.assertEquals(actualSuspendedInstances, suspendedInstances, "Suspended Instances");
        Assert.assertEquals(actualWaitingInstances, waitingInstances, "Waiting Instances");
        Assert.assertEquals(actualKilledInstances, killedInstances, "Killed Instances");
    }

    /**
     * Checks that expected number of failed instances matches actual number of failed ones.
     *
     * @param r         kind of response from API which should contain information about instances.
     * @param failCount number of instances which should be failed.
     */
    public static void validateFailedInstances(InstancesResult r, int failCount) {
        AssertUtil.assertSucceeded(r);
        int counter = 0;
        for (InstancesResult.Instance processInstance : r.getInstances()) {
            if (processInstance.getStatus() == InstancesResult.WorkflowStatus.FAILED) {
                counter++;
            }
        }
        Assert.assertEquals(counter, failCount, "Actual number of failed instances does not "
                +
                "match expected number of failed instances.");
    }

    public static List<String> getWorkflows(ColoHelper prismHelper, String processName,
            WorkflowJob.Status... ws) throws OozieClientException {

        String bundleID = OozieUtil.getBundles(prismHelper.getFeedHelper().getOozieClient(),
                processName, EntityType.PROCESS).get(0);
        OozieClient oozieClient = prismHelper.getClusterHelper().getOozieClient();

        List<String> workflows = OozieUtil.getWorkflowJobs(prismHelper, bundleID);

        List<String> toBeReturned = new ArrayList<String>();
        for (String jobID : workflows) {
            WorkflowJob wfJob = oozieClient.getJobInfo(jobID);
            LOGGER.info("wa.getExternalId(): " + wfJob.getId() + " wa"
                    +
                    ".getExternalStatus"
                    +
                    "():  "
                    +
                    wfJob.getStartTime());
            LOGGER.info("wf id: " + jobID + "  wf status: " + wfJob.getStatus());
            if (ws.length == 0) {
                toBeReturned.add(jobID);
            } else {
                for (WorkflowJob.Status status : ws) {
                    if (wfJob.getStatus().name().equals(status.name())) {
                        toBeReturned.add(jobID);
                    }
                }
            }
        }
        return toBeReturned;
    }

    public static boolean isWorkflowRunning(OozieClient oozieClient, String workflowID) throws
            OozieClientException {
        String status = oozieClient.getJobInfo(workflowID).getStatus().toString();
        return status.equals("RUNNING");
    }

    public static void areWorkflowsRunning(OozieClient oozieClient, List<String> wfIDs,
            int totalWorkflows,
            int runningWorkflows, int killedWorkflows,
            int succeededWorkflows) throws OozieClientException {

        List<WorkflowJob> wfJobs = new ArrayList<WorkflowJob>();
        for (String wdID : wfIDs) {
            wfJobs.add(oozieClient.getJobInfo(wdID));
        }
        if (totalWorkflows != -1) {
            Assert.assertEquals(wfJobs.size(), totalWorkflows);
        }
        int actualRunningWorkflows = 0;
        int actualKilledWorkflows = 0;
        int actualSucceededWorkflows = 0;
        LOGGER.info("wfJobs: " + wfJobs);
        for (int instanceIndex = 0; instanceIndex < wfJobs.size(); instanceIndex++) {
            LOGGER.info("was.get(" + instanceIndex + ").getStatus(): "
                    +
                    wfJobs.get(instanceIndex).getStatus());

            if (wfJobs.get(instanceIndex).getStatus().toString().equals("RUNNING")) {
                actualRunningWorkflows++;
            } else if (wfJobs.get(instanceIndex).getStatus().toString().equals("KILLED")) {
                actualKilledWorkflows++;
            } else if (wfJobs.get(instanceIndex).getStatus().toString().equals("SUCCEEDED")) {
                actualSucceededWorkflows++;
            }
        }
        if (runningWorkflows != -1) {
            Assert.assertEquals(actualRunningWorkflows, runningWorkflows);
        }
        if (killedWorkflows != -1) {
            Assert.assertEquals(actualKilledWorkflows, killedWorkflows);
        }
        if (succeededWorkflows != -1) {
            Assert.assertEquals(actualSucceededWorkflows, succeededWorkflows);
        }
    }

    public static List<CoordinatorAction> getProcessInstanceList(ColoHelper coloHelper,
            String processName,
            EntityType entityType)
        throws OozieClientException {

        OozieClient oozieClient = coloHelper.getProcessHelper().getOozieClient();
        String coordId = getLatestCoordinatorID(oozieClient, processName, entityType);
        //String coordId = getDefaultCoordinatorFromProcessName(processName);
        LOGGER.info("default coordID: " + coordId);
        return oozieClient.getCoordJobInfo(coordId).getActions();
    }

    public static String getLatestCoordinatorID(OozieClient oozieClient, String processName,
            EntityType entityType)
        throws OozieClientException {
        return getDefaultCoordIDFromBundle(oozieClient,
                getLatestBundleID(oozieClient, processName, entityType));
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
            org.apache.oozie.client.CoordinatorAction.Status
                    status,
            EntityType entityType)
        throws OozieClientException {
        List<CoordinatorAction> list = getProcessInstanceList(coloHelper, processName, entityType);
        int instanceCount = 0;
        for (CoordinatorAction aList : list) {
            if (aList.getStatus().equals(status)) {
                instanceCount++;
            }
        }
        return instanceCount;
    }

    public static Status getDefaultCoordinatorStatus(ColoHelper colohelper, String processName,
            int bundleNumber) throws OozieClientException {
        OozieClient oozieClient = colohelper.getProcessHelper().getOozieClient();
        String coordId =
                getDefaultCoordinatorFromProcessName(colohelper, processName, bundleNumber);
        return oozieClient.getCoordJobInfo(coordId).getStatus();
    }

    public static String getDefaultCoordinatorFromProcessName(
            ColoHelper coloHelper, String processName, int bundleNumber) throws OozieClientException {
        String bundleID =
                getSequenceBundleID(coloHelper, processName, EntityType.PROCESS, bundleNumber);
        return getDefaultCoordIDFromBundle(coloHelper.getClusterHelper().getOozieClient(), bundleID);
    }

    /**
     * Retrieves all coordinators of bundle.
     *
     * @param bundleID specific bundle ID
     * @param helper   entity helper which is related to job
     * @return list of bundle coordinators
     * @throws OozieClientException
     */
    public static List<CoordinatorJob> getBundleCoordinators(String bundleID,
            IEntityManagerHelper helper)
        throws OozieClientException {
        OozieClient localOozieClient = helper.getOozieClient();
        BundleJob bundleInfo = localOozieClient.getBundleJobInfo(bundleID);
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
        return getLatestBundleID(coloHelper.getFeedHelper().getOozieClient(),
                entityName, entityType);
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
            String entityName, EntityType entityType)
        throws OozieClientException {
        List<String> bundleIds = OozieUtil.getBundles(oozieClient,
                entityName, entityType);
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
    public static String getSequenceBundleID(ColoHelper prismHelper, String entityName,
            EntityType entityType, int bundleNumber)
        throws OozieClientException {
        return getSequenceBundleID(prismHelper.getClusterHelper().getOozieClient(), entityName,
                entityType, bundleNumber);
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
        for (Integer key : bundleMap.keySet()) {
            bundleID = bundleMap.get(key);
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
     *                       process.
     * @param instanceNumber - ordinal number of instance which state will be returned.
     * @return - state of mentioned instance.
     * @throws OozieClientException
     */
    public static CoordinatorAction.Status getInstanceStatus(ColoHelper coloHelper,
            String processName,
            int bundleNumber, int
            instanceNumber) throws OozieClientException {
        String bundleID = InstanceUtil
                .getSequenceBundleID(coloHelper, processName, EntityType.PROCESS, bundleNumber);
        if (StringUtils.isEmpty(bundleID)) {
            return null;
        }
        String coordID = InstanceUtil.getDefaultCoordIDFromBundle(coloHelper.getClusterHelper().getOozieClient(),
                bundleID);
        if (StringUtils.isEmpty(coordID)) {
            return null;
        }
        OozieClient oozieClient = coloHelper.getProcessHelper().getOozieClient();
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


    public static void createHDFSFolders(ColoHelper helper, List<String> folderList)
        throws IOException {
        LOGGER.info("creating folders.....");
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://" + helper.getFeedHelper().getHadoopURL());
        final FileSystem fs = FileSystem.get(conf);
        for (final String folder : folderList) {
            if (StringUtils.isNotEmpty(folder)) {
                fs.mkdirs(new Path(folder));
            }
        }
        LOGGER.info("created folders.....");
    }

    /**
     * Sets one more cluster to feed.
     *
     * @param feed          feed which is to be modified
     * @param feedValidity  validity of the feed on the cluster
     * @param feedRetention set retention of the feed on the cluster
     * @param clusterName   cluster name, if null would erase all the cluster details from the feed
     * @param clusterType   cluster type
     * @param partition     - partition where data is available for feed
     * @param locations     - location where data is picked
     * @return - string representation of the modified feed
     */
    public static String setFeedCluster(String feed, Validity feedValidity, Retention feedRetention,
            String clusterName,
            ClusterType clusterType, String partition,
            String... locations) {
        return setFeedClusterWithTable(feed, feedValidity, feedRetention, clusterName, clusterType,
                partition, null, locations);
    }

    public static String setFeedClusterWithTable(String feed, Validity feedValidity,
            Retention feedRetention, String clusterName,
            ClusterType clusterType, String partition,
            String tableUri, String... locations) {
        Feed f = (Feed) Entity.fromString(EntityType.FEED, feed);
        if (clusterName == null) {
            f.getClusters().getClusters().clear();
        } else {
            Cluster feedCluster = createFeedCluster(feedValidity, feedRetention, clusterName,
                    clusterType, partition, tableUri, locations);
            f.getClusters().getClusters().add(feedCluster);
        }
        return f.toString();
    }

    private static CatalogTable getCatalogTable(String tableUri) {
        CatalogTable catalogTable = new CatalogTable();
        catalogTable.setUri(tableUri);
        return catalogTable;
    }

    private static Cluster createFeedCluster(
            Validity feedValidity, Retention feedRetention, String clusterName, ClusterType clusterType,
            String partition, String tableUri, String[] locations) {

        Cluster cluster = new Cluster();
        cluster.setName(clusterName);
        cluster.setRetention(feedRetention);
        if (clusterType != null) {
            cluster.setType(clusterType);
        }
        cluster.setValidity(feedValidity);
        if (partition != null) {
            cluster.setPartition(partition);
        }

        // if table uri is not empty or null then set it.
        if (StringUtils.isNotEmpty(tableUri)) {
            cluster.setTable(getCatalogTable(tableUri));
        }
        Locations feedLocations = new Locations();
        if (ArrayUtils.isNotEmpty(locations)) {
            for (int i = 0; i < locations.length; i++) {
                Location oneLocation = new Location();
                oneLocation.setPath(locations[i]);
                if (i == 0) {
                    oneLocation.setType(LocationType.DATA);
                } else if (i == 1) {
                    oneLocation.setType(LocationType.STATS);
                } else if (i == 2) {
                    oneLocation.setType(LocationType.META);
                } else if (i == 3) {
                    oneLocation.setType(LocationType.TMP);
                } else {
                    Assert.fail("unexpected value of locations: " + Arrays.toString(locations));
                }
                feedLocations.getLocations().add(oneLocation);
            }
            cluster.setLocations(feedLocations);
        }
        return cluster;
    }

    /**
     * Retrieves replication coordinatorID from bundle of coordinators.
     */
    public static List<String> getReplicationCoordID(String bundlID,
            IEntityManagerHelper helper)
        throws OozieClientException {
        List<CoordinatorJob> coords = InstanceUtil.getBundleCoordinators(bundlID, helper);
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
        throws IOException, URISyntaxException, AuthenticationException {
        if (params != null && !colo.equals("")) {
            url = url + params + "&" + colo.substring(1);
        } else if (params != null) {
            url = url + params;
        } else {
            url = url + colo;
        }
        return InstanceUtil.sendRequestProcessInstance(url, user);
    }

    /**
     * Retrieves prefix (main sub-folders) of feed data path.
     */
    public static String getFeedPrefix(String feed) {
        Feed feedElement = (Feed) Entity.fromString(EntityType.FEED, feed);
        String p = feedElement.getLocations().getLocations().get(0).getPath();
        p = p.substring(0, p.indexOf('$'));
        return p;
    }

    /**
     * Sets one more cluster to process definition.
     *
     * @param process     - process definition string representation
     * @param clusterName - name of cluster
     * @param validity    - cluster validity
     * @return - string representation of modified process
     */
    public static String setProcessCluster(String process, String clusterName,
            org.apache.falcon.entity.v0.process.Validity validity) {
        org.apache.falcon.entity.v0.process.Cluster c =
                new org.apache.falcon.entity.v0.process.Cluster();
        c.setName(clusterName);
        c.setValidity(validity);
        Process p = (Process) Entity.fromString(EntityType.PROCESS, process);

        if (clusterName == null) {
            p.getClusters().getClusters().set(0, null);
        } else {
            p.getClusters().getClusters().add(c);
        }
        return p.toString();
    }

    /**
     * Adds one input into process.
     *
     * @param process - where input should be inserted
     * @param feed    - feed which will be used as input feed
     * @return - string representation of process definition
     */
    public static String addProcessInputFeed(String process, String feed, String feedName) {
        Process processElement = (Process) Entity.fromString(EntityType.PROCESS, process);
        Input in1 = processElement.getInputs().getInputs().get(0);
        Input in2 = new Input();
        in2.setEnd(in1.getEnd());
        in2.setFeed(feed);
        in2.setName(feedName);
        in2.setPartition(in1.getPartition());
        in2.setStart(in1.getStart());
        processElement.getInputs().getInputs().add(in2);
        return processElement.toString();
    }

    public static org.apache.oozie.client.WorkflowJob.Status getInstanceStatusFromCoord(
            ColoHelper ua1, String coordID, int instanceNumber) throws OozieClientException {
        OozieClient oozieClient = ua1.getProcessHelper().getOozieClient();
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


    /**
     * Sets new feed data path.
     *
     * @param feed feed which is to be modified
     * @param path new feed data path
     * @return modified feed
     */
    public static String setFeedFilePath(String feed, String path) {
        Feed feedElement = (Feed) Entity.fromString(EntityType.FEED, feed);
        feedElement.getLocations().getLocations().get(0).setPath(path);
        return feedElement.toString();
    }

    public static int checkIfFeedCoordExist(IEntityManagerHelper helper,
            String feedName, String coordType) throws OozieClientException {
        LOGGER.info("feedName: " + feedName);
        int numberOfCoord = 0;

        if (OozieUtil.getBundles(helper.getOozieClient(), feedName, EntityType.FEED).size() == 0) {
            return 0;
        }
        List<String> bundleID =
                OozieUtil.getBundles(helper.getOozieClient(), feedName, EntityType.FEED);
        LOGGER.info("bundleID: " + bundleID);

        for (String aBundleID : bundleID) {
            LOGGER.info("aBundleID: " + aBundleID);
            OozieUtil.waitForCoordinatorJobCreation(helper.getOozieClient(), aBundleID);
            List<CoordinatorJob> coords =
                    InstanceUtil.getBundleCoordinators(aBundleID, helper);
            LOGGER.info("coords: " + coords);
            for (CoordinatorJob coord : coords) {
                if (coord.getAppName().contains(coordType)) {
                    numberOfCoord++;
                }
            }
        }
        return numberOfCoord;
    }

    /**
     * Sets process frequency.
     *
     * @return modified process definition
     */
    public static String setProcessFrequency(String process,
            Frequency frequency) {
        Process p = (Process) Entity.fromString(EntityType.PROCESS, process);

        p.setFrequency(frequency);

        return p.toString();
    }

    /**
     * Sets new process name.
     */
    public static String setProcessName(String process, String newName) {
        Process p = (Process) Entity.fromString(EntityType.PROCESS, process);

        p.setName(newName);

        return p.toString();
    }

    /**
     * Sets new process validity on all the process clusters.
     *
     * @param process   process entity to be modified
     * @param startTime start of process validity
     * @param endTime   end of process validity
     * @return modified process definition
     */
    public static String setProcessValidity(String process,
            String startTime, String endTime) {
        Process processElement = (Process) Entity.fromString(EntityType.PROCESS, process);

        for (int i = 0; i < processElement.getClusters().getClusters().size(); i++) {
            processElement.getClusters().getClusters().get(i).getValidity().setStart(
                    TimeUtil.oozieDateToDate(startTime).toDate());
            processElement.getClusters().getClusters().get(i).getValidity()
                    .setEnd(TimeUtil.oozieDateToDate(endTime).toDate());
        }
        return processElement.toString();
    }

    public static List<CoordinatorAction> getProcessInstanceListFromAllBundles(
            ColoHelper coloHelper, String processName, EntityType entityType)
        throws OozieClientException {
        OozieClient oozieClient = coloHelper.getProcessHelper().getOozieClient();
        List<CoordinatorAction> list = new ArrayList<CoordinatorAction>();
        LOGGER.info("bundle size for process is "
                +
                OozieUtil.getBundles(coloHelper.getFeedHelper().getOozieClient(), processName,
                        entityType).size());
        for (String bundleId : OozieUtil.getBundles(coloHelper.getFeedHelper().getOozieClient(),
                processName, entityType)) {
            BundleJob bundleInfo = oozieClient.getBundleJobInfo(bundleId);
            List<CoordinatorJob> coords = bundleInfo.getCoordinators();
            LOGGER.info("number of coords in bundle " + bundleId + "=" + coords.size());
            for (CoordinatorJob coord : coords) {
                List<CoordinatorAction> actions =
                        oozieClient.getCoordJobInfo(coord.getId()).getActions();
                LOGGER.info("number of actions in coordinator " + coord.getId() + " is "
                        +
                        actions.size());
                list.addAll(actions);
            }
        }
        String coordId = getLatestCoordinatorID(coloHelper.getClusterHelper().getOozieClient(), processName,
                entityType);
        LOGGER.info("default coordID: " + coordId);
        return list;
    }

    public static String getOutputFolderForInstanceForReplication(ColoHelper coloHelper,
            String coordID,
            int instanceNumber)
        throws OozieClientException {
        OozieClient oozieClient = coloHelper.getProcessHelper().getOozieClient();
        CoordinatorJob coordInfo = oozieClient.getCoordJobInfo(coordID);
        return InstanceUtil.getReplicatedFolderFromInstanceRunConf(
                oozieClient.getJobInfo(coordInfo.getActions().get(instanceNumber).getExternalId())
                        .getConf());
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

        return InstanceUtil.getReplicatedFolderBaseFromInstanceRunConf(
                oozieClient.getJobInfo(coordInfo.getActions().get(instanceNumber).getExternalId())
                        .getConf());
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
     * @param instancesNumber     instance number for which we wait to reach the required status
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
                String.format("Bundle job %s is should be prep/running but is %s", bundleId,
                        bundleStatus));
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
            Assert.assertTrue(RUNNING_PREP_SUCCEEDED.contains(coordinatorStatus),
                    String.format("Coordinator %s should be running/prep but is %s.", coordId,
                            coordinatorStatus));
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
            if (j.getStatus() == expectedStatus) {
                break;
            }
            TimeUtil.sleepSeconds(20);
        }
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
     * Sets feed frequency.
     *
     * @return modified feed
     */
    public static String setFeedFrequency(String feed, Frequency f) {
        Feed feedElement = (Feed) Entity.fromString(EntityType.FEED, feed);
        feedElement.setFrequency(f);
        return feedElement.toString();
    }

    /**
     * Waits till instances of specific job will be created during specific time.
     * Use this method directly in unusual test cases where timeouts are different from trivial.
     * In other cases use waitTillInstancesAreCreated(ColoHelper,String,int)
     *
     * @param coloHelper  colo helper of cluster job is running on
     * @param entity      definition of entity which describes job
     * @param bundleSeqNo bundle number if update has happened.
     * @throws OozieClientException
     */
    public static void waitTillInstancesAreCreated(ColoHelper coloHelper,
            String entity,
            int bundleSeqNo,
            int totalMinutesToWait
    ) throws OozieClientException {
        String entityName = Util.readEntityName(entity);
        EntityType type = Util.getEntityType(entity);
        waitTillInstancesAreCreated(coloHelper, entityName, type, bundleSeqNo, totalMinutesToWait);
    }

    /**
     * Waits till instances of specific job will be created during specific time.
     * Use this method directly in unusual test cases where timeouts are different from trivial.
     * In other cases use waitTillInstancesAreCreated(ColoHelper,String,int)
     *
     * @param coloHelper  colo helper of cluster job is running on
     * @param entityName  name of entity job is related to
     * @param type        type of entity
     * @param bundleSeqNo bundle number if update has happened.
     * @throws OozieClientException
     */
    public static void waitTillInstancesAreCreated(ColoHelper coloHelper,
            String entityName,
            EntityType type,
            int bundleSeqNo,
            int totalMinutesToWait
    ) throws OozieClientException {
        String bundleID = getSequenceBundleID(coloHelper, entityName, type,
                bundleSeqNo);
        String coordID = getDefaultCoordIDFromBundle(coloHelper.getClusterHelper().getOozieClient(), bundleID);
        for (int sleepCount = 0; sleepCount < totalMinutesToWait; sleepCount++) {
            CoordinatorJob coordInfo = coloHelper.getProcessHelper().getOozieClient()
                    .getCoordJobInfo(coordID);

            if (coordInfo.getActions().size() > 0) {
                break;
            }
            LOGGER.info("Coord " + coordInfo.getId() + " still doesn't have "
                    +
                    "instance created on oozie: " + coloHelper.getProcessHelper()
                    .getOozieClient().getOozieUrl());
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
        waitTillInstancesAreCreated(coloHelper, entity, bundleSeqNo, sleep);
    }

    public static String setFeedACL(String feed, String... ownerGroup) {
        FeedMerlin feedObject = new FeedMerlin(feed);
        ACL acl = feedObject.getACL();
        acl.setOwner(MerlinConstants.ACL_OWNER);
        acl.setGroup(MerlinConstants.ACL_GROUP);
        if (ownerGroup.length > 0) {
            acl.setOwner(ownerGroup[0]);
            if (ownerGroup.length == 2) {
                acl.setGroup(ownerGroup[1]);
            }
        }
        feedObject.setACL(acl);
        return feedObject.toString();
    }
}

