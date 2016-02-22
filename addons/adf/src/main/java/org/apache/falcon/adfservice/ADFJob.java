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

package org.apache.falcon.adfservice;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.io.IOUtils;
import org.apache.falcon.adfservice.util.ADFJsonConstants;
import org.apache.falcon.adfservice.util.FSUtils;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.FalconException;
import org.apache.falcon.resource.AbstractSchedulableEntityManager;
import org.apache.falcon.security.CurrentUser;
import org.apache.hadoop.fs.Path;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for Azure ADF jobs.
 */
public abstract class ADFJob {
    private static final Logger LOG = LoggerFactory.getLogger(ADFJob.class);

    // name prefix for all adf related entity, e.g. an adf hive process and the feeds associated with it
    public static final String ADF_ENTITY_NAME_PREFIX = "ADF-";
    public static final int ADF_ENTITY_NAME_PREFIX_LENGTH = ADF_ENTITY_NAME_PREFIX.length();
    // name prefix for all adf related job entity, i.e. adf hive/pig process and replication feed
    public static final String ADF_JOB_ENTITY_NAME_PREFIX = ADF_ENTITY_NAME_PREFIX + "JOB-";
    public static final int ADF_JOB_ENTITY_NAME_PREFIX_LENGTH = ADF_JOB_ENTITY_NAME_PREFIX.length();

    public static final String TEMPLATE_PATH_PREFIX = "/apps/falcon/adf/";
    public static final String PROCESS_SCRIPTS_PATH = TEMPLATE_PATH_PREFIX
            + Path.SEPARATOR + "generatedscripts";
    private static final String DEFAULT_FREQUENCY = "days(1)";

    public static boolean isADFJobEntity(String entityName) {
        return entityName.startsWith(ADF_JOB_ENTITY_NAME_PREFIX);
    }

    public static String getSessionID(String entityName) throws FalconException {
        if (!isADFJobEntity(entityName)) {
            throw new FalconException("The entity, " + entityName + ", is not an ADF Job Entity.");
        }
        return entityName.substring(ADF_JOB_ENTITY_NAME_PREFIX_LENGTH);
    }

    /**
     * Enum for job type.
     */
    public static enum JobType {
        HIVE, PIG, REPLICATION
    }

    private static enum RequestType {
        HADOOPMIRROR, HADOOPHIVE, HADOOPPIG
    }

    public static JobType getJobType(String msg) throws FalconException {
        try {
            JSONObject obj = new JSONObject(msg);
            JSONObject activity = obj.getJSONObject(ADFJsonConstants.ADF_REQUEST_ACTIVITY);
            if (activity == null) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_ACTIVITY + " not found in ADF"
                        + " request.");
            }

            JSONObject activityProperties = activity.getJSONObject(ADFJsonConstants.ADF_REQUEST_TRANSFORMATION);
            if (activityProperties == null) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_TRANSFORMATION + " not found "
                        + "in ADF request.");
            }

            String type = activityProperties.getString(ADFJsonConstants.ADF_REQUEST_TYPE);
            if (StringUtils.isBlank(type)) {
                throw new FalconException(ADFJsonConstants.ADF_REQUEST_TYPE + " not found in ADF request msg");
            }

            switch (RequestType.valueOf(type.toUpperCase())) {
            case HADOOPMIRROR:
                return JobType.REPLICATION;
            case HADOOPHIVE:
                return JobType.HIVE;
            case HADOOPPIG:
                return JobType.PIG;
            default:
                throw new FalconException("Unrecognized ADF job type: " + type);
            }
        } catch (JSONException e) {
            throw new FalconException("Error while parsing ADF JSON message: " + msg, e);
        }
    }

    public abstract void startJob() throws FalconException;
    public abstract void cleanup() throws FalconException;

    protected JSONObject message;
    protected JSONObject activity;
    protected JSONObject activityExtendedProperties;
    protected String id;
    protected JobType type;
    protected String startTime, endTime;
    protected String frequency;
    protected String proxyUser;
    protected long timeout;
    protected ADFJobManager jobManager = new ADFJobManager();

    private Map<String, JSONObject> linkedServicesMap = new HashMap<String, JSONObject>();
    protected Map<String, JSONObject> tablesMap = new HashMap<String, JSONObject>();

    public ADFJob(String msg, String id) throws FalconException {
        this.id = id;
        FSUtils.createDir(new Path(PROCESS_SCRIPTS_PATH));
        try {
            message = new JSONObject(msg);

            frequency = DEFAULT_FREQUENCY;
            startTime = transformTimeFormat(message.getString(ADFJsonConstants.ADF_REQUEST_START_TIME));
            endTime = transformTimeFormat(message.getString(ADFJsonConstants.ADF_REQUEST_END_TIME));

            JSONArray linkedServices = message.getJSONArray(ADFJsonConstants.ADF_REQUEST_LINKED_SERVICES);
            for (int i = 0; i < linkedServices.length(); i++) {
                JSONObject linkedService = linkedServices.getJSONObject(i);
                linkedServicesMap.put(linkedService.getString(ADFJsonConstants.ADF_REQUEST_NAME), linkedService);
            }

            JSONArray tables = message.getJSONArray(ADFJsonConstants.ADF_REQUEST_TABLES);
            for (int i = 0; i < tables.length(); i++) {
                JSONObject table = tables.getJSONObject(i);
                tablesMap.put(table.getString(ADFJsonConstants.ADF_REQUEST_NAME), table);
            }

            // Set the activity extended properties
            activity = message.getJSONObject(ADFJsonConstants.ADF_REQUEST_ACTIVITY);
            if (activity == null) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_ACTIVITY + " not found in ADF"
                        + " request.");
            }

            JSONObject policy = activity.getJSONObject(ADFJsonConstants.ADF_REQUEST_POLICY);
            /* IS policy mandatory */
            if (policy == null) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_POLICY + " not found"
                        + " in ADF request.");
            }
            String adfTimeout = policy.getString(ADFJsonConstants.ADF_REQUEST_TIMEOUT);
            if (StringUtils.isBlank(adfTimeout)) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_TIMEOUT + " not found"
                        + " in ADF request.");
            }
            timeout = parseADFRequestTimeout(adfTimeout);

            JSONObject activityProperties = activity.getJSONObject(ADFJsonConstants.ADF_REQUEST_TRANSFORMATION);
            if (activityProperties == null) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_TRANSFORMATION + " not found"
                        + " in ADF request.");
            }

            activityExtendedProperties = activityProperties.getJSONObject(
                    ADFJsonConstants.ADF_REQUEST_EXTENDED_PROPERTIES);
            if (activityExtendedProperties == null) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_EXTENDED_PROPERTIES + " not"
                        + " found in ADF request.");
            }

            // should be called after setting activityExtendedProperties
            proxyUser = getRunAsUser();

            // log in the user
            CurrentUser.authenticate(proxyUser);
        } catch (JSONException e) {
            throw new FalconException("Error while parsing ADF JSON message: " + msg, e);
        }
    }

    public String jobEntityName() {
        return ADF_JOB_ENTITY_NAME_PREFIX + id;
    }

    public JobType jobType() {
        return type;
    }

    protected String getClusterName(String linkedServiceName) throws FalconException {
        JSONObject linkedService = linkedServicesMap.get(linkedServiceName);
        if (linkedService == null) {
            throw new FalconException("Linked service " + linkedServiceName + " not found in ADF request.");
        }

        try {
            return linkedService.getJSONObject(ADFJsonConstants.ADF_REQUEST_PROPERTIES)
                    .getJSONObject(ADFJsonConstants.ADF_REQUEST_EXTENDED_PROPERTIES)
                    .getString(ADFJsonConstants.ADF_REQUEST_CLUSTER_NAME);
        } catch (JSONException e) {
            throw new FalconException("Error while parsing linked service " + linkedServiceName + " in ADF request.");
        }
    }

    protected String getRunAsUser() throws FalconException {
        if (activityExtendedProperties.has(ADFJsonConstants.ADF_REQUEST_RUN_ON_BEHALF_USER)) {
            String runAsUser = null;
            try {
                runAsUser = activityExtendedProperties.getString(ADFJsonConstants.ADF_REQUEST_RUN_ON_BEHALF_USER);
            } catch (JSONException e) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_RUN_ON_BEHALF_USER + " not"
                        + " found in ADF request.");
            }

            if (StringUtils.isBlank(runAsUser)) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_RUN_ON_BEHALF_USER + " in"
                        + " ADF request activity extended properties cannot be empty.");
            }
            return runAsUser;
        } else {
            String hadoopLinkedService = getHadoopLinkedService();
            JSONObject linkedService = linkedServicesMap.get(hadoopLinkedService);
            if (linkedService == null) {
                throw new FalconException("JSON object " + hadoopLinkedService + " not"
                        + " found in ADF request.");
            }

            try {
                return linkedService.getJSONObject(ADFJsonConstants.ADF_REQUEST_PROPERTIES)
                        .getJSONObject(ADFJsonConstants.ADF_REQUEST_EXTENDED_PROPERTIES)
                        .getString(ADFJsonConstants.ADF_REQUEST_RUN_ON_BEHALF_USER);
            } catch (JSONException e) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_RUN_ON_BEHALF_USER + " not"
                        + " found in ADF request.");
            }
        }
    }

    protected List<String> getInputTables() throws FalconException {
        List<String> tables = new ArrayList<String>();
        try {
            JSONArray inputs = message.getJSONObject(ADFJsonConstants.ADF_REQUEST_ACTIVITY)
                    .getJSONArray(ADFJsonConstants.ADF_REQUEST_INPUTS);
            for (int i = 0; i < inputs.length(); i++) {
                tables.add(inputs.getJSONObject(i).getString(ADFJsonConstants.ADF_REQUEST_NAME));
            }
        } catch (JSONException e) {
            throw new FalconException("Error while reading input table names in ADF request.");
        }
        return tables;
    }

    protected List<String> getOutputTables() throws FalconException {
        List<String> tables = new ArrayList<String>();
        try {
            JSONArray outputs = message.getJSONObject(ADFJsonConstants.ADF_REQUEST_ACTIVITY)
                    .getJSONArray(ADFJsonConstants.ADF_REQUEST_OUTPUTS);
            for (int i = 0; i < outputs.length(); i++) {
                tables.add(outputs.getJSONObject(i).getString(ADFJsonConstants.ADF_REQUEST_NAME));
            }
        } catch (JSONException e) {
            throw new FalconException("Error while reading output table names in ADF request.");
        }
        return tables;
    }

    protected String getADFTablePath(String tableName) throws FalconException {
        JSONObject table = tablesMap.get(tableName);
        if (table == null) {
            throw new FalconException("JSON object " + tableName + " not"
                    + " found in ADF request.");
        }

        try {
            JSONObject location = table.getJSONObject(ADFJsonConstants.ADF_REQUEST_PROPERTIES)
                    .getJSONObject(ADFJsonConstants.ADF_REQUEST_LOCATION);
            String requestType = location.getString(ADFJsonConstants.ADF_REQUEST_TYPE);
            if (requestType.equals(ADFJsonConstants.ADF_REQUEST_LOCATION_TYPE_AZURE_BLOB)) {
                String blobPath = location.getString(ADFJsonConstants.ADF_REQUEST_FOLDER_PATH);
                int index = blobPath.indexOf('/');
                if (index == -1) {
                    throw new FalconException("Invalid azure blob path: " + blobPath);
                }

                String linkedServiceName = location.getString(ADFJsonConstants.ADF_REQUEST_LINKED_SERVICE_NAME);
                JSONObject linkedService = linkedServicesMap.get(linkedServiceName);
                if (linkedService == null) {
                    throw new FalconException("Can't find linked service " + linkedServiceName + " for azure blob");
                }
                String connectionString = linkedService.getJSONObject(ADFJsonConstants.ADF_REQUEST_PROPERTIES)
                        .getString(ADFJsonConstants.ADF_REQUEST_CONNECTION_STRING);
                int accountNameIndex = connectionString.indexOf(ADFJsonConstants.ADF_REQUEST_BLOB_ACCOUNT_NAME)
                        + ADFJsonConstants.ADF_REQUEST_BLOB_ACCOUNT_NAME.length();
                String accountName = connectionString.substring(accountNameIndex,
                        connectionString.indexOf(';', accountNameIndex));

                StringBuilder blobUrl = new StringBuilder("wasb://")
                        .append(blobPath.substring(0, index)).append("@")
                        .append(accountName).append(".blob.core.windows.net")
                        .append(blobPath.substring(index));
                return blobUrl.toString();
            }
            return location.getJSONObject(ADFJsonConstants.ADF_REQUEST_EXTENDED_PROPERTIES)
                    .getString(ADFJsonConstants.ADF_REQUEST_FOLDER_PATH);
        } catch (JSONException e) {
            throw new FalconException("Error while parsing ADF JSON message: " + tableName, e);
        }
    }

    protected String getTableCluster(String tableName) throws FalconException {
        JSONObject table = tablesMap.get(tableName);
        if (table == null) {
            throw new FalconException("Table " + tableName + " not found in ADF request.");
        }

        try {
            String linkedServiceName = table.getJSONObject(ADFJsonConstants.ADF_REQUEST_PROPERTIES)
                    .getJSONObject(ADFJsonConstants.ADF_REQUEST_LOCATION)
                    .getString(ADFJsonConstants.ADF_REQUEST_LINKED_SERVICE_NAME);
            return getClusterName(linkedServiceName);
        } catch (JSONException e) {
            throw new FalconException("Error while parsing table cluster " + tableName + " in ADF request.");
        }
    }

    protected boolean activityHasScriptPath() throws FalconException {
        if (JobType.REPLICATION == jobType()) {
            return false;
        }

        return activityExtendedProperties.has(
                ADFJsonConstants.ADF_REQUEST_SCRIPT_PATH);
    }

    protected String getScriptPath() throws FalconException {
        if (!activityHasScriptPath()) {
            throw new FalconException("JSON object does not have object: "
                    + ADFJsonConstants.ADF_REQUEST_SCRIPT_PATH);
        }

        try {
            String scriptPath = activityExtendedProperties.getString(ADFJsonConstants.ADF_REQUEST_SCRIPT_PATH);
            if (StringUtils.isBlank(scriptPath)) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_SCRIPT_PATH + " not"
                        + " found or empty in ADF request.");
            }
            return scriptPath;
        } catch (JSONException jsonException) {
            throw new FalconException("Error while parsing ADF JSON object: "
                    + activityExtendedProperties, jsonException);
        }
    }

    protected String getScriptContent() throws FalconException {
        if (activityHasScriptPath()) {
            throw new FalconException("JSON object does not have object: " + ADFJsonConstants.ADF_REQUEST_SCRIPT);
        }
        try {
            String script = activityExtendedProperties.getString(ADFJsonConstants.ADF_REQUEST_SCRIPT);
            if (StringUtils.isBlank(script)) {
                throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_SCRIPT + " cannot"
                        + " be empty in ADF request.");
            }
            return script;
        } catch (JSONException jsonException) {
            throw new FalconException("Error while parsing ADF JSON object: "
                    + activityExtendedProperties, jsonException);
        }
    }

    protected String getClusterNameToRunProcessOn() throws FalconException {
        return getClusterName(getHadoopLinkedService());
    }

    protected Entity submitAndScheduleJob(String entityType, String msg) throws FalconException {
        Entity entity = jobManager.submitJob(entityType, msg);
        jobManager.scheduleJob(entityType, jobEntityName());
        return entity;
    }

    private String getHadoopLinkedService() throws FalconException {
        String hadoopLinkedService;
        try {
            hadoopLinkedService = activity.getString(ADFJsonConstants.ADF_REQUEST_LINKED_SERVICE_NAME);
        } catch (JSONException jsonException) {
            throw new FalconException("Error while parsing ADF JSON object: "
                    + activity, jsonException);
        }

        if (StringUtils.isBlank(hadoopLinkedService)) {
            throw new FalconException("JSON object " + ADFJsonConstants.ADF_REQUEST_LINKED_SERVICE_NAME
                    + " in the activity cannot be empty in ADF request.");
        }
        return hadoopLinkedService;
    }

    protected void startProcess(Feed inputFeed, Feed outputFeed,
                                String engineType, String scriptPath) throws FalconException {
        // submit input/output feeds
        LOG.info("submitting input feed {} for {} process", inputFeed.getName(), engineType);
        jobManager.submitJob(EntityType.FEED.name(), inputFeed.getEntityxml());

        LOG.info("submitting output feed {} for {} process", outputFeed.getName(), engineType);
        jobManager.submitJob(EntityType.FEED.name(), outputFeed.getEntityxml());

        // submit and schedule process
        String processRequest = new Process.Builder().withProcessName(jobEntityName()).withFrequency(frequency)
                .withStartTime(startTime).withEndTime(endTime).withClusterName(getClusterNameToRunProcessOn())
                .withInputFeedName(inputFeed.getName()).withOutputFeedName(outputFeed.getName())
                .withEngineType(engineType).withWFPath(scriptPath).withAclOwner(proxyUser)
                .build().getEntityxml();

        LOG.info("submitting/scheduling {} process: {}", engineType, processRequest);
        submitAndScheduleJob(EntityType.PROCESS.name(), processRequest);
        LOG.info("submitted and scheduled {} process: {}", engineType, jobEntityName());
    }

    protected void cleanupProcess(Feed inputFeed, Feed outputFeed) throws FalconException {
        // delete the entities. Should be called after the job execution success/failure.
        jobManager.deleteEntity(EntityType.PROCESS.name(), jobEntityName());
        jobManager.deleteEntity(EntityType.FEED.name(), inputFeed.getName());
        jobManager.deleteEntity(EntityType.FEED.name(), outputFeed.getName());

        // delete script file
        FSUtils.removeDir(new Path(ADFJob.PROCESS_SCRIPTS_PATH, jobEntityName()));
    }

    protected String createScriptFile(String fileExt) throws FalconException {
        String content = getScriptContent();

        // create dir; dir path is unique as job name is always unique
        final Path dir = new Path(ADFJob.PROCESS_SCRIPTS_PATH, jobEntityName());
        FSUtils.createDir(dir);

        // create script file
        final Path path = new Path(dir, jobEntityName() + fileExt);
        return FSUtils.createFile(path, content);
    }

    private static long parseADFRequestTimeout(String timeout) throws FalconException {
        timeout = timeout.trim();
        //  [ws][-]{ d | d.hh:mm[:ss[.ff]] | hh:mm[:ss[.ff]] }[ws]
        if (timeout.startsWith("-")) {
            return -1;
        }

        long totalMinutes = 0;
        String [] dotParts = timeout.split(Pattern.quote("."));
        if (dotParts.length == 1) {
            // no d or ff
            // chk if only d
            // Formats can be d|hh:mm[:ss]
            String[] parts = timeout.split(":");
            if (parts.length == 1) {
                // only day. Convert days to minutes
                return Integer.parseInt(parts[0]) * 1440;
            } else {
                // hh:mm[:ss]
                return computeMinutes(parts);
            }
        }

        // if . is present, formats can be d.hh:mm[:ss[.ff]] | hh:mm[:ss[.ff]]
        if (dotParts.length == 2) {
            // can be d.hh:mm[:ss] or hh:mm[:ss[.ff]
            // check if first part has colons
            String [] parts = dotParts[0].split(":");
            if (parts.length == 1) {
                // format is d.hh:mm[:ss]
                totalMinutes = Integer.parseInt(dotParts[0]) * 1440;
                parts = dotParts[1].split(":");
                totalMinutes += computeMinutes(parts);
                return totalMinutes;
            } else {
                // format is hh:mm[:ss[.ff]
                parts = dotParts[0].split(":");
                totalMinutes += computeMinutes(parts);
                // round off ff
                totalMinutes +=  1;
                return totalMinutes;
            }
        } else if (dotParts.length == 3) {
            // will be d.hh:mm[:ss[.ff]
            totalMinutes = Integer.parseInt(dotParts[0]) * 1440;
            String [] parts = dotParts[1].split(":");
            totalMinutes += computeMinutes(parts);
            // round off ff
            totalMinutes +=  1;
            return totalMinutes;
        } else {
            throw new FalconException("Error parsing policy timeout: " + timeout);
        }
    }

    // format hh:mm[:ss]
    private static long computeMinutes(String[] parts) {
        // hh:mm[:ss]
        int totalMinutes = Integer.parseInt(parts[0]) * 60;
        totalMinutes += Integer.parseInt(parts[1]);
        if (parts.length == 3) {
            // Second round off to minutes
            totalMinutes +=  1;
        }
        return totalMinutes;
    }

    private static String transformTimeFormat(String adfTime) {
        return adfTime.substring(0, adfTime.length()-4) + "Z";
    }

    protected class ADFJobManager extends AbstractSchedulableEntityManager {
        public Entity submitJob(String entityType, String msg) throws FalconException {
            try {
                InputStream stream = IOUtils.toInputStream(msg);
                Entity entity = submitInternal(stream, entityType, proxyUser);
                return entity;
            } catch (Exception e) {
                LOG.info(e.toString());
                throw new FalconException("Error when submitting job: " + e.toString());
            }
        }

        public void scheduleJob(String entityType, String entityName) throws FalconException {
            try {
                scheduleInternal(entityType, entityName, null, EntityUtil.getPropertyMap(null));
            } catch (Exception e) {
                LOG.info(e.toString());
                throw new FalconException("Error when scheduling job: " + e.toString());
            }
        }

        public void deleteEntity(String entityType, String entityName) throws FalconException {
            delete(entityType, entityName, null);
        }
    }
}
