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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.FalconWebException;
import org.apache.falcon.entity.parser.ProcessEntityParser;
import org.apache.falcon.entity.parser.ValidationException;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.extensions.jdbc.ExtensionMetaStore;
import org.apache.falcon.extensions.store.ExtensionStore;
import org.apache.falcon.persistence.ExtensionJobsBean;
import org.apache.falcon.security.CurrentUser;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A base class for managing Extension Operations.
 */
public class AbstractExtensionManager extends AbstractSchedulableEntityManager {
    public static final Logger LOG = LoggerFactory.getLogger(AbstractExtensionManager.class);

    private static final String JOB_NAME = "jobName";
    public static final String TAG_PREFIX_EXTENSION_JOB = "_falcon_extension_job=";
    private static final String EXTENSION_NAME = "extensionName";
    private static final String FEEDS = "feeds";
    private static final String PROCESSES = "processes";
    private static final String CONFIG  = "config";
    private static final String CREATION_TIME  = "creationTime";
    private static final String LAST_UPDATE_TIME  = "lastUpdatedTime";

    public static void validateExtensionName(final String extensionName) {
        if (StringUtils.isBlank(extensionName)) {
            throw FalconWebException.newAPIException("Extension name is mandatory and shouldn't be blank",
                    Response.Status.BAD_REQUEST);
        }
    }

    public String registerExtensionMetadata(String extensionName, String path, String description, String owner) {
        validateExtensionName(extensionName);
        try {
            return ExtensionStore.get().registerExtension(extensionName, path, description, owner);
        } catch (Throwable e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    public String getExtensionJobDetail(String jobName) {
        try {
            return buildExtensionJobDetailResult(jobName).toString();
        } catch (FalconException e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    public String deleteExtensionMetadata(String extensionName){
        validateExtensionName(extensionName);
        try {
            return ExtensionStore.get().deleteExtension(extensionName, CurrentUser.getUser());
        } catch (Throwable e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    private JSONObject buildExtensionJobDetailResult(final String jobName) throws FalconException {
        ExtensionMetaStore metaStore = ExtensionStore.getMetaStore();
        ExtensionJobsBean jobsBean = metaStore.getExtensionJobDetails(jobName);
        if (jobsBean == null) {
            throw new ValidationException("Job name not found:" + jobName);
        }
        JSONObject detailsObject = new JSONObject();
        try {
            detailsObject.put(JOB_NAME, jobsBean.getJobName());
            detailsObject.put(EXTENSION_NAME, jobsBean.getExtensionName());
            detailsObject.put(FEEDS, StringUtils.join(jobsBean.getFeeds(), ","));
            detailsObject.put(PROCESSES, StringUtils.join(jobsBean.getProcesses(), ","));
            detailsObject.put(CONFIG, jobsBean.getConfig());
            detailsObject.put(CREATION_TIME, jobsBean.getCreationTime());
            detailsObject.put(LAST_UPDATE_TIME, jobsBean.getLastUpdatedTime());
        } catch (JSONException e) {
            LOG.error("Exception while building extension jon details for job {}", jobName, e);
        }
        return detailsObject;
    }


    protected void submitEntities(String extensionName, String doAsUser, String jobName,
                                  Map<EntityType, List<Entity>> entityMap, InputStream configStream)
        throws FalconException, IOException {
        List<Entity> feeds = entityMap.get(EntityType.FEED);
        List<Entity> processes = entityMap.get(EntityType.PROCESS);
        validateFeeds(feeds);
        validateProcesses(processes);
        List<String> feedNames = new ArrayList<>();
        List<String> processNames = new ArrayList<>();
        for (Entity feed : feeds) {
            submitInternal(feed, doAsUser);
            feedNames.add(feed.getName());
        }
        for (Entity process: processes) {
            submitInternal(process, doAsUser);
            processNames.add(process.getName());
        }

        ExtensionMetaStore metaStore = ExtensionStore.getMetaStore();
        byte[] configBytes = null;
        if (configStream != null) {
            configBytes = IOUtils.toByteArray(configStream);
        }
        metaStore.storeExtensionJob(jobName, extensionName, feedNames, processNames, configBytes);
    }


    private void validateFeeds(List<Entity> feeds) throws FalconException {
        for (Entity feed : feeds) {
            super.validate(feed);
        }
    }

    private void validateProcesses(List<Entity> processes) throws FalconException {
        ProcessEntityParser processEntityParser = new ProcessEntityParser();
        for (Entity process : processes) {
            processEntityParser.validate((Process)process, false);
        }
    }

    protected void scheduleEntities(Map<EntityType, List<Entity>> entityMap) throws FalconException,
            AuthorizationException {
        for (Object feed: entityMap.get(EntityType.FEED)) {
            scheduleInternal(EntityType.FEED.name(), ((Feed)feed).getName(), null, null);
        }
        for (Object process: entityMap.get(EntityType.PROCESS)) {
            scheduleInternal(EntityType.PROCESS.name(), ((Process)process).getName(), null, null);
        }
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
}
