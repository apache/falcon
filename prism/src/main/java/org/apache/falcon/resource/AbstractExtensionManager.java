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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.FalconWebException;
import org.apache.falcon.entity.EntityNotRegisteredException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.parser.ValidationException;
import org.apache.falcon.extensions.ExtensionStatus;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.extensions.jdbc.ExtensionMetaStore;
import org.apache.falcon.extensions.store.ExtensionStore;
import org.apache.falcon.persistence.ExtensionBean;
import org.apache.falcon.persistence.ExtensionJobsBean;
import org.apache.falcon.security.CurrentUser;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A base class for managing Extension Operations.
 */
public class AbstractExtensionManager extends AbstractSchedulableEntityManager {
    public static final Logger LOG = LoggerFactory.getLogger(AbstractExtensionManager.class);

    private static final String JOB_NAME = "jobName";
    protected static final String TAG_PREFIX_EXTENSION_JOB = "_falcon_extension_job=";
    private static final String EXTENSION_NAME = "extensionName";
    private static final String FEEDS = "feeds";
    private static final String PROCESSES = "processes";
    private static final String CONFIG  = "config";
    private static final String CREATION_TIME  = "creationTime";
    private static final String LAST_UPDATE_TIME  = "lastUpdatedTime";
    protected static final String ASCENDING_SORT_ORDER = "asc";
    protected static final String DESCENDING_SORT_ORDER = "desc";

    public static final String NAME = "name";
    public static final String STATUS = "status";
    private static final String EXTENSION_TYPE = "type";
    private static final String EXTENSION_DESC = "description";
    private static final String EXTENSION_LOCATION = "location";
    private static final String ENTITY_EXISTS_STATUS = "EXISTS";
    private static final String ENTITY_NOT_EXISTS_STATUS = "NOT_EXISTS";

    protected static void validateExtensionName(final String extensionName) {
        if (StringUtils.isBlank(extensionName)) {
            throw FalconWebException.newAPIException("Extension name is mandatory and shouldn't be blank",
                    Response.Status.BAD_REQUEST);
        }
    }

    protected APIResult registerExtensionMetadata(String extensionName, String path, String description, String owner) {
        validateExtensionName(extensionName);
        try {
            return new APIResult(APIResult.Status.SUCCEEDED, ExtensionStore.get().registerExtension(extensionName, path,
                    description, owner));
        } catch (Throwable e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    public APIResult getExtensionJobDetail(String jobName) {
        try {
            return new APIResult(APIResult.Status.SUCCEEDED, buildExtensionJobDetailResult(jobName).toString());
        } catch (FalconException e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    protected APIResult getExtensionDetail(String extensionName) {
        try {
            return new APIResult(APIResult.Status.SUCCEEDED, buildExtensionDetailResult(extensionName).toString());
        } catch (FalconException e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    public APIResult getExtensions() {
        try {
            return new APIResult(APIResult.Status.SUCCEEDED, buildEnumerateResult().toString());
        } catch (FalconException e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    public ExtensionJobList getExtensionJobs(String extensionName, String sortOrder, String doAsUser) {

        Comparator<ExtensionJobsBean> compareByJobName = new Comparator<ExtensionJobsBean>() {
            @Override
            public int compare(ExtensionJobsBean o1, ExtensionJobsBean o2) {
                return o1.getJobName().compareToIgnoreCase(o2.getJobName());
            }
        };

        Map<String, String> jobAndExtensionNames = new HashMap<>();
        List<ExtensionJobsBean> extensionJobs = null;
        if (extensionName != null) {
            extensionJobs = ExtensionStore.getMetaStore().getJobsForAnExtension(extensionName);
        } else {
            extensionJobs = ExtensionStore.getMetaStore().getAllExtensionJobs();
        }

        sortOrder = (sortOrder == null) ? ASCENDING_SORT_ORDER : sortOrder;
        switch (sortOrder.toLowerCase()) {
        case DESCENDING_SORT_ORDER:
            Collections.sort(extensionJobs, Collections.reverseOrder(compareByJobName));
            break;

        default:
            Collections.sort(extensionJobs, compareByJobName);
        }

        for (ExtensionJobsBean job : extensionJobs) {
            jobAndExtensionNames.put(job.getJobName(), job.getExtensionName());
        }
        return new ExtensionJobList(extensionJobs.size(), jobAndExtensionNames);
    }

    public APIResult deleteExtensionMetadata(String extensionName) {
        validateExtensionName(extensionName);
        ExtensionStore metaStore = ExtensionStore.get();
        try {
            canDeleteExtension(extensionName);
            return new APIResult(APIResult.Status.SUCCEEDED,
                    metaStore.deleteExtension(extensionName, CurrentUser.getUser()));
        } catch (FalconException e) {
            throw FalconWebException.newAPIException(e);
        }
    }

    private void canDeleteExtension(String extensionName) throws FalconException {
        ExtensionMetaStore metaStore = ExtensionStore.getMetaStore();
        List<ExtensionJobsBean> extensionJobs = metaStore.getJobsForAnExtension(extensionName);
        if (!extensionJobs.isEmpty()) {
            StringBuilder jobs = new StringBuilder();
            for(ExtensionJobsBean extensionJobsBean : extensionJobs) {
                jobs.append("\n" + extensionJobsBean.getJobName());
            }
            LOG.error("Extension:" + extensionName + " cannot be unregistered as following instances are dependent on "
                    + "the extension:" + jobs.toString());
            throw new FalconException("Extension:" + extensionName + " cannot be unregistered as following instances"
                    + " are dependent on the extension:" + jobs.toString());
        }
    }

    protected SortedMap<EntityType, List<String>> getJobEntities(ExtensionJobsBean extensionJobsBean)
        throws FalconException {
        TreeMap<EntityType, List<String>> entityMap = new TreeMap<>(Collections.<EntityType>reverseOrder());
        entityMap.put(EntityType.PROCESS, extensionJobsBean.getProcesses());
        entityMap.put(EntityType.FEED, extensionJobsBean.getFeeds());
        return entityMap;
    }

    private JSONObject buildExtensionJobDetailResult(final String jobName) throws FalconException {
        ExtensionMetaStore metaStore = ExtensionStore.getMetaStore();
        ExtensionJobsBean jobsBean = metaStore.getExtensionJobDetails(jobName);
        if (jobsBean == null) {
            throw new ValidationException("Job name not found:" + jobName);
        }
        ExtensionBean extensionBean = metaStore.getDetail(jobsBean.getExtensionName());
        JSONObject detailsObject = new JSONObject();
        try {
            detailsObject.put(JOB_NAME, jobsBean.getJobName());
            detailsObject.put(EXTENSION_NAME, jobsBean.getExtensionName());
            detailsObject.put(FEEDS, getEntitiesStatus(jobsBean.getFeeds(), EntityType.FEED));
            detailsObject.put(PROCESSES, getEntitiesStatus(jobsBean.getProcesses(), EntityType.PROCESS));
            detailsObject.put(CONFIG, jobsBean.getConfig());
            detailsObject.put(CREATION_TIME, jobsBean.getCreationTime());
            detailsObject.put(LAST_UPDATE_TIME, jobsBean.getLastUpdatedTime());
            detailsObject.put(EXTENSION_LOCATION, extensionBean.getLocation());
            detailsObject.put(EXTENSION_TYPE, extensionBean.getExtensionType());
        } catch (JSONException e) {
            LOG.error("Exception while building extension jon details for job {}", jobName, e);
        }
        return detailsObject;
    }

    public static String getJobNameFromTag(String tags) {
        int nameStart = tags.indexOf(TAG_PREFIX_EXTENSION_JOB);
        if (nameStart == -1) {
            return null;
        }

        nameStart = nameStart + TAG_PREFIX_EXTENSION_JOB.length();
        int nameEnd = tags.indexOf(',', nameStart);
        if (nameEnd == -1) {
            nameEnd = tags.length();
        }
        return tags.substring(nameStart, nameEnd);
    }

    protected String disableExtension(String extensionName, String currentUser) {
        validateExtensionName(extensionName);
        try {
            return ExtensionStore.get().updateExtensionStatus(extensionName, currentUser, ExtensionStatus.DISABLED);
        } catch (Throwable e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    public String enableExtension(String extensionName, String currentUser) {
        validateExtensionName(extensionName);
        try {
            return ExtensionStore.get().updateExtensionStatus(extensionName, currentUser, ExtensionStatus.ENABLED);
        } catch (Throwable e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    private JSONObject buildExtensionDetailResult(final String extensionName) throws FalconException {
        ExtensionMetaStore metaStore = ExtensionStore.getMetaStore();

        if (!metaStore.checkIfExtensionExists(extensionName)) {
            throw new ValidationException("No extension resources found for " + extensionName);
        }

        ExtensionBean extensionBean = metaStore.getDetail(extensionName);
        if (extensionBean == null) {
            LOG.error("Extension not found: " + extensionName);
            throw new FalconException("Extension not found:" + extensionName);
        }
        JSONObject resultObject = new JSONObject();
        try {
            resultObject.put(NAME, extensionBean.getExtensionName());
            resultObject.put(EXTENSION_TYPE, extensionBean.getExtensionType());
            resultObject.put(EXTENSION_DESC, extensionBean.getDescription());
            resultObject.put(EXTENSION_LOCATION, extensionBean.getLocation());
        } catch (JSONException e) {
            LOG.error("Exception in buildDetailResults:", e);
            throw new FalconException(e);
        }
        return resultObject;
    }

    private static JSONArray buildEnumerateResult() throws FalconException {
        JSONArray results = new JSONArray();
        ExtensionMetaStore metaStore = ExtensionStore.getMetaStore();
        List<ExtensionBean> extensionBeanList = metaStore.getAllExtensions();
        for (ExtensionBean extensionBean : extensionBeanList) {
            JSONObject resultObject = new JSONObject();

            try {
                resultObject.put(NAME, extensionBean.getExtensionName().toLowerCase());
                resultObject.put(EXTENSION_TYPE, extensionBean.getExtensionType());
                resultObject.put(EXTENSION_DESC, extensionBean.getDescription());
                resultObject.put(EXTENSION_LOCATION, extensionBean.getLocation());
            } catch (JSONException e) {
                throw new FalconException(e);
            }
            results.put(resultObject);
        }
        return results;
    }

    protected static void checkIfExtensionIsEnabled(String extensionName) {
        ExtensionMetaStore metaStore = ExtensionStore.getMetaStore();
        ExtensionBean extensionBean = metaStore.getDetail(extensionName);
        if (extensionBean == null) {
            LOG.error("Extension not found: " + extensionName);
            throw FalconWebException.newAPIException("Extension not found:" + extensionName,
                    Response.Status.NOT_FOUND);
        }
        if (!extensionBean.getStatus().equals(ExtensionStatus.ENABLED)) {
            LOG.error("Extension: " + extensionName + " is in disabled state.");
            throw FalconWebException.newAPIException("Extension: " + extensionName + " is in disabled state.",
                    Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    protected static void checkIfExtensionExists(String extensionName) {
        ExtensionMetaStore metaStore = ExtensionStore.getMetaStore();
        ExtensionBean extensionBean = metaStore.getDetail(extensionName);
        if (extensionBean == null) {
            LOG.error("Extension not found: " + extensionName);
            throw FalconWebException.newAPIException("Extension not found:" + extensionName,
                    Response.Status.NOT_FOUND);
        }
    }

    protected static void checkIfExtensionJobNameExists(String jobName, String extensionName) {
        ExtensionMetaStore metaStore = ExtensionStore.getMetaStore();
        ExtensionJobsBean extensionJobsBean = metaStore.getExtensionJobDetails(jobName);
        if (extensionJobsBean != null && !extensionJobsBean.getExtensionName().equals(extensionName)) {
            LOG.error("Extension job with name: " + extensionName + " already exists.");
            throw FalconWebException.newAPIException("Extension job with name: " + jobName + " already exists.",
                    Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    private JSONObject getEntitiesStatus(List<String> entities, EntityType type) throws JSONException, FalconException {
        JSONObject entityObject = new JSONObject();
        for (String entity : entities) {
            try {
                entityObject.put(NAME, entity);
                EntityUtil.getEntity(type, entity);
                entityObject.put(STATUS, ENTITY_EXISTS_STATUS);
            } catch (EntityNotRegisteredException e) {
                entityObject.put(STATUS, ENTITY_NOT_EXISTS_STATUS);
            }
        }
        return entityObject;
    }
}
