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

package org.apache.falcon.resource.extensions;

import com.sun.jersey.multipart.FormDataParam;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.FalconWebException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.parser.ValidationException;
import org.apache.falcon.entity.store.StoreAccessException;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.extensions.Extension;
import org.apache.falcon.extensions.ExtensionProperties;
import org.apache.falcon.extensions.ExtensionService;
import org.apache.falcon.extensions.ExtensionType;
import org.apache.falcon.extensions.jdbc.ExtensionMetaStore;
import org.apache.falcon.extensions.store.ExtensionStore;
import org.apache.falcon.persistence.ExtensionBean;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.AbstractSchedulableEntityManager;
import org.apache.falcon.resource.EntityList;
import org.apache.falcon.resource.ExtensionInstanceList;
import org.apache.falcon.resource.ExtensionJobList;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.service.Services;
import org.apache.falcon.util.DeploymentUtil;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Jersey Resource for extension job operations.
 */
@Path("extension")
public class ExtensionManager extends AbstractSchedulableEntityManager {
    public static final Logger LOG = LoggerFactory.getLogger(ExtensionManager.class);

    public static final String TAG_PREFIX_EXTENSION_NAME = "_falcon_extension_name=";
    public static final String TAG_PREFIX_EXTENSION_JOB = "_falcon_extension_job=";
    public static final String ASCENDING_SORT_ORDER = "asc";
    public static final String DESCENDING_SORT_ORDER = "desc";

    private Extension extension = new Extension();
    private static final String EXTENSION_RESULTS = "extensions";
    private static final String TOTAL_RESULTS = "totalResults";
    private static final String README = "README";
    private static final String EXTENSION_NAME = "name";
    private static final String EXTENSION_TYPE = "type";
    private static final String EXTENSION_DESC = "description";
    public static final String EXTENSION_LOCATION = "location";

    private static final String EXTENSION_PROPERTY_JSON_SUFFIX = "-properties.json";
    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    @GET
    @Path("list/{extension-name}")
    @Produces({MediaType.TEXT_XML, MediaType.APPLICATION_JSON})
    public ExtensionJobList getExtensionJobs(
            @PathParam("extension-name") String extensionName,
            @DefaultValue("") @QueryParam("fields") String fields,
            @DefaultValue(ASCENDING_SORT_ORDER) @QueryParam("sortOrder") String sortOrder,
            @DefaultValue("0") @QueryParam("offset") Integer offset,
            @QueryParam("numResults") Integer resultsPerPage,
            @DefaultValue("") @QueryParam("doAs") String doAsUser) {
        checkIfExtensionServiceIsEnabled();
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        try {
            // get filtered entities
            List<Entity> entities = getEntityList("", "", "", TAG_PREFIX_EXTENSION_NAME + extensionName, "", doAsUser);
            if (entities.isEmpty()) {
                return new ExtensionJobList(0);
            }

            // group entities by extension job name
            Map<String, List<Entity>> groupedEntities = groupEntitiesByJob(entities);

            // sort by extension job name
            List<String> jobNames = new ArrayList<>(groupedEntities.keySet());
            switch (sortOrder.toLowerCase()) {
            case DESCENDING_SORT_ORDER :
                Collections.sort(jobNames, Collections.reverseOrder(String.CASE_INSENSITIVE_ORDER));
                break;
            default:
                Collections.sort(jobNames, String.CASE_INSENSITIVE_ORDER);
            }

            // pagination and format output
            int pageCount = getRequiredNumberOfResults(jobNames.size(), offset, resultsPerPage);
            HashSet<String> fieldSet = new HashSet<>(Arrays.asList(fields.toUpperCase().split(",")));
            ExtensionJobList jobList = new ExtensionJobList(pageCount);
            for (int i = offset; i < offset + pageCount; i++) {
                String jobName = jobNames.get(i);
                List<Entity> jobEntities = groupedEntities.get(jobName);
                EntityList entityList = new EntityList(buildEntityElements(fieldSet, jobEntities), jobEntities.size());
                jobList.addJob(new ExtensionJobList.JobElement(jobName, entityList));
            }
            return jobList;
        } catch (FalconException | IOException e) {
            LOG.error("Failed to get extension job list of " + extensionName + ": ", e);
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("instances/{job-name}")
    @Produces(MediaType.APPLICATION_JSON)
    public ExtensionInstanceList getInstances(
            @PathParam("job-name") final String jobName,
            @QueryParam("start") final String nominalStart,
            @QueryParam("end") final String nominalEnd,
            @DefaultValue("") @QueryParam("instanceStatus") String instanceStatus,
            @DefaultValue("") @QueryParam("fields") String fields,
            @DefaultValue("") @QueryParam("orderBy") String orderBy,
            @DefaultValue("") @QueryParam("sortOrder") String sortOrder,
            @DefaultValue("0") @QueryParam("offset") final Integer offset,
            @QueryParam("numResults") Integer resultsPerPage,
            @DefaultValue("") @QueryParam("doAs") String doAsUser) {
        checkIfExtensionServiceIsEnabled();
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        try {
            List<Entity> entities = getEntityList("", "", "", TAG_PREFIX_EXTENSION_JOB + jobName, "", doAsUser);
            if (entities.isEmpty()) {
                return new ExtensionInstanceList(0);
            }

            HashSet<String> fieldSet = new HashSet<>(Arrays.asList(fields.toUpperCase().split(",")));
            ExtensionInstanceList instances = new ExtensionInstanceList(entities.size());
            for (Entity entity : entities) {
                InstancesResult entityInstances = super.getStatus(
                        entity.getEntityType().name(), entity.getName(), nominalStart, nominalEnd,
                        null, null, "STATUS:" + instanceStatus, orderBy, sortOrder, offset, resultsPerPage, null);
                instances.addEntitySummary(new ExtensionInstanceList.EntitySummary(
                        getEntityElement(entity, fieldSet), entityInstances.getInstances()));
            }
            return instances;
        } catch (FalconException | IOException e) {
            LOG.error("Error when listing instances of extension job: " + jobName + ": ", e);
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Path("schedule/{job-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    public APIResult schedule(@PathParam("job-name") String jobName,
                              @DefaultValue("") @QueryParam("doAs") String doAsUser) {
        checkIfExtensionServiceIsEnabled();
        try {
            List<Entity> entities = getEntityList("", "", "", TAG_PREFIX_EXTENSION_JOB + jobName, "", doAsUser);
            if (entities.isEmpty()) {
                // return failure if the extension job doesn't exist
                return new APIResult(APIResult.Status.FAILED, "Extension job " + jobName + " doesn't exist.");
            }

            for (Entity entity : entities) {
                scheduleInternal(entity.getEntityType().name(), entity.getName(), null, null);
            }
        } catch (FalconException | IOException e) {
            LOG.error("Error when scheduling extension job: " + jobName + ": ", e);
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
        return new APIResult(APIResult.Status.SUCCEEDED, "Extension job " + jobName + " scheduled successfully");
    }

    @POST
    @Path("suspend/{job-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    public APIResult suspend(@PathParam("job-name") String jobName,
                             @DefaultValue("") @QueryParam("doAs") String doAsUser) {
        checkIfExtensionServiceIsEnabled();
        try {
            List<Entity> entities = getEntityList("", "", "", TAG_PREFIX_EXTENSION_JOB + jobName, "", doAsUser);
            if (entities.isEmpty()) {
                // return failure if the extension job doesn't exist
                return new APIResult(APIResult.Status.FAILED, "Extension job " + jobName + " doesn't exist.");
            }

            for (Entity entity : entities) {
                if (entity.getEntityType().isSchedulable()) {
                    if (getWorkflowEngine(entity).isActive(entity)) {
                        getWorkflowEngine(entity).suspend(entity);
                    }
                }
            }
        } catch (FalconException | IOException e) {
            LOG.error("Error when scheduling extension job: " + jobName + ": ", e);
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
        return new APIResult(APIResult.Status.SUCCEEDED, "Extension job " + jobName + " suspended successfully");
    }

    @POST
    @Path("resume/{job-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    public APIResult resume(@PathParam("job-name") String jobName,
                            @DefaultValue("") @QueryParam("doAs") String doAsUser) {
        checkIfExtensionServiceIsEnabled();
        try {
            List<Entity> entities = getEntityList("", "", "", TAG_PREFIX_EXTENSION_JOB + jobName, "", doAsUser);
            if (entities.isEmpty()) {
                // return failure if the extension job doesn't exist
                return new APIResult(APIResult.Status.FAILED, "Extension job " + jobName + " doesn't exist.");
            }

            for (Entity entity : entities) {
                if (entity.getEntityType().isSchedulable()) {
                    if (getWorkflowEngine(entity).isSuspended(entity)) {
                        getWorkflowEngine(entity).resume(entity);
                    }
                }
            }
        } catch (FalconException | IOException e) {
            LOG.error("Error when resuming extension job " + jobName + ": ", e);
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
        return new APIResult(APIResult.Status.SUCCEEDED, "Extension job " + jobName + " resumed successfully");
    }

    @POST
    @Path("delete/{job-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    public APIResult delete(@PathParam("job-name") String jobName,
                            @DefaultValue("") @QueryParam("doAs") String doAsUser) {
        checkIfExtensionServiceIsEnabled();
        try {
            List<Entity> entities = getEntityList("", "", "", TAG_PREFIX_EXTENSION_JOB + jobName, "", doAsUser);
            if (entities.isEmpty()) {
                // return failure if the extension job doesn't exist
                return new APIResult(APIResult.Status.SUCCEEDED,
                        "Extension job " + jobName + " doesn't exist. Nothing to delete.");
            }

            for (Entity entity : entities) {
                // TODO(yzheng): need to remember the entity dependency graph for clean ordered removal
                canRemove(entity);
                if (entity.getEntityType().isSchedulable() && !DeploymentUtil.isPrism()) {
                    getWorkflowEngine(entity).delete(entity);
                }
                configStore.remove(entity.getEntityType(), entity.getName());
            }
        } catch (FalconException | IOException e) {
            LOG.error("Error when deleting extension job: " + jobName + ": ", e);
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
        return new APIResult(APIResult.Status.SUCCEEDED, "Extension job " + jobName + " deleted successfully");
    }

    @POST
    @Path("submit/{extension-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.MULTIPART_FORM_DATA})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    public APIResult submit(
            @PathParam("extension-name") String extensionName,
            @Context HttpServletRequest request,
            @DefaultValue("") @QueryParam("doAs") String doAsUser,
            @QueryParam("jobName") String jobName,
            @FormDataParam("entities") List<Entity> entities,
            @FormDataParam("config") InputStream config) {
        checkIfExtensionServiceIsEnabled();
        try {
            entities = getEntityList(extensionName, entities, config);
            submitEntities(extensionName, doAsUser, jobName, entities, config);
        } catch (FalconException | IOException e) {
            LOG.error("Error when submitting extension job: ", e);
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
        return new APIResult(APIResult.Status.SUCCEEDED, "Extension job submitted successfully" + jobName);
    }

    private void validateEntities(List<Entity> entities) throws FalconException {
        for (Entity entity : entities) {
            if (!EntityType.FEED.equals(entity.getEntityType()) && !EntityType.PROCESS.equals(entity.getEntityType())) {
                LOG.error("Cluster entity is not allowed for submission via submitEntities: {}", entity.getName());
                throw new FalconException("Cluster entity is not allowed for submission in extensions submission");
            }
            super.validate(entity);
        }
    }

    private ExtensionType getExtensionType(String extensionName) {
        ExtensionMetaStore metaStore = ExtensionStore.get().getMetaStore();
        ExtensionBean extensionDetails = metaStore.getDetail(extensionName);
        return extensionDetails.getExtensionType();
    }

    @POST
    @Path("submitAndSchedule/{extension-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.MULTIPART_FORM_DATA})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    public APIResult submitAndSchedule(
            @PathParam("extension-name") String extensionName,
            @Context HttpServletRequest request,
            @DefaultValue("") @QueryParam("doAs") String doAsUser,
            @QueryParam("jobName") String jobName,
            @FormDataParam("entities") List<Entity> entities,
            @FormDataParam("config") InputStream config) {
        checkIfExtensionServiceIsEnabled();
        try {
            entities = getEntityList(extensionName, entities, config);
            submitEntities(extensionName, doAsUser, jobName, entities, config);
            for (Entity entity : entities) {
                scheduleInternal(entity.getEntityType().name(), entity.getName(), null, null);
            }
        } catch (FalconException | IOException e) {
            LOG.error("Error when submitting extension job: ", e);
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
        return new APIResult(APIResult.Status.SUCCEEDED, "Extension job submitted and scheduled successfully");
    }

    protected void submitEntities(String extensionName, String doAsUser, String jobName, List<Entity> entities,
                                InputStream configStream) throws FalconException, IOException {
        validateEntities(entities);
        List<String> feeds = new ArrayList<>();
        List<String> processes = new ArrayList<>();
        for (Entity entity : entities) {
            submitInternal(entity, doAsUser);
            if (EntityType.FEED.equals(entity.getEntityType())) {
                feeds.add(entity.getName());
            } else if (EntityType.PROCESS.equals(entity.getEntityType())) {
                processes.add(entity.getName());
            }
        }
        ExtensionMetaStore metaStore = ExtensionStore.get().getMetaStore();
        byte[] configBytes = null;
        if (configStream != null) {
            configBytes = IOUtils.toByteArray(configStream);
        }
        metaStore.storeExtensionJob(jobName, extensionName, feeds, processes, configBytes);
    }

    private List<Entity> getEntityList(String extensionName, List<Entity> entities, InputStream config)
        throws FalconException, IOException {
        ExtensionType extensionType = getExtensionType(extensionName);
        if (ExtensionType.TRUSTED.equals(extensionType)) {
            entities = generateEntities(extensionName, config);
        }
        return entities;
    }

    @POST
    @Path("update/{extension-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    public APIResult update(
            @PathParam("extension-name") String extensionName,
            @Context HttpServletRequest request,
            @DefaultValue("") @QueryParam("doAs") String doAsUser) {
        checkIfExtensionServiceIsEnabled();
        try {
            List<Entity> entities = generateEntities(extensionName, request.getInputStream());
            for (Entity entity : entities) {
                super.update(entity, entity.getEntityType().name(), entity.getName(), null);
            }
        } catch (FalconException | IOException e) {
            LOG.error("Error when updating extension job: ", e);
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
        return new APIResult(APIResult.Status.SUCCEEDED, "Updated successfully");
    }

    @POST
    @Path("validate/{extension-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    public APIResult validate(
            @PathParam("extension-name") String extensionName,
            @Context HttpServletRequest request,
            @DefaultValue("") @QueryParam("doAs") String doAsUser) {
        checkIfExtensionServiceIsEnabled();
        try {
            List<Entity> entities = generateEntities(extensionName, request.getInputStream());
            for (Entity entity : entities) {
                super.validate(entity);
            }
        } catch (FalconException | IOException e) {
            LOG.error("Error when validating extension job: ", e);
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
        return new APIResult(APIResult.Status.SUCCEEDED, "Validated successfully");
    }


    // Extension store related REST API's
    @GET
    @Path("enumerate")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getExtensions() {
        checkIfExtensionServiceIsEnabled();
        JSONArray results;

        try {
            results = buildEnumerateResult();
        } catch (StoreAccessException e) {
            LOG.error("Failed when accessing extension store.", e);
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        } catch (FalconException e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }

        try {
            JSONObject response = new JSONObject();
            response.put(EXTENSION_RESULTS, results);
            response.put(TOTAL_RESULTS, results.length());

            return Response.ok(response).build();
        } catch (Throwable e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("describe/{extension-name}")
    @Produces(MediaType.TEXT_PLAIN)
    public String getExtensionDescription(
            @PathParam("extension-name") String extensionName) {
        checkIfExtensionServiceIsEnabled();
        validateExtensionName(extensionName);
        try {
            return ExtensionStore.get().getResource(extensionName, README);
        } catch (Throwable e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("detail/{extension-name}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getDetail(@PathParam("extension-name") String extensionName){
        checkIfExtensionServiceIsEnabled();
        validateExtensionName(extensionName);
        try {
            return Response.ok(buildDetailResult(extensionName)).build();
        } catch (Throwable e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }


    @POST
    @Path("unregister/{extension-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces(MediaType.TEXT_PLAIN)
    public String deleteExtensionMetadata(
            @PathParam("extension-name") String extensionName){
        checkIfExtensionServiceIsEnabled();
        validateExtensionName(extensionName);
        try {
            return ExtensionStore.get().deleteExtension(extensionName);
        } catch (Throwable e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Path("register/{extension-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces(MediaType.TEXT_PLAIN)
    public String registerExtensionMetadata(
            @PathParam("extension-name") String extensionName,
            @QueryParam("path") String path,
            @QueryParam("description") String description){
        checkIfExtensionServiceIsEnabled();
        validateExtensionName(extensionName);
        try {
            return ExtensionStore.get().registerExtension(extensionName, path, description);
        } catch (Throwable e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("definition/{extension-name}")
    @Produces({MediaType.APPLICATION_JSON})
    public String getExtensionDefinition(
            @PathParam("extension-name") String extensionName) {
        checkIfExtensionServiceIsEnabled();
        validateExtensionName(extensionName);
        try {
            return ExtensionStore.get().getResource(extensionName,
                    extensionName.toLowerCase() + EXTENSION_PROPERTY_JSON_SUFFIX);
        } catch (Throwable e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    private static void validateExtensionName(final String extensionName) {
        if (StringUtils.isBlank(extensionName)) {
            throw FalconWebException.newAPIException("Extension name is mandatory and shouldn't be blank",
                    Response.Status.BAD_REQUEST);
        }
    }

    private static JSONArray buildEnumerateResult() throws FalconException {
        JSONArray results = new JSONArray();
        ExtensionMetaStore metaStore = ExtensionStore.get().getMetaStore();
        List<ExtensionBean> extensionBeanList = metaStore.getAllExtensions();
        for (ExtensionBean extensionBean : extensionBeanList) {
            JSONObject resultObject = new JSONObject();

            try {
                resultObject.put(EXTENSION_NAME, extensionBean.getExtensionName().toLowerCase());
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

    private List<Entity> generateEntities(String extensionName, InputStream configStream)
        throws FalconException, IOException {
        // get entities for extension job
        Properties properties = new Properties();
        properties.load(configStream);
        List<Entity> entities = extension.getEntities(extensionName, configStream);

        // add tags on extension name and job
        String jobName = properties.getProperty(ExtensionProperties.JOB_NAME.getName());
        EntityUtil.applyTags(extensionName, jobName, entities);

        return entities;
    }

    private JSONObject buildDetailResult(final String extensionName) throws FalconException {
        ExtensionMetaStore metaStore = ExtensionStore.get().getMetaStore();

        if (!metaStore.checkIfExtensionExists(extensionName)){
            throw new ValidationException("No extension resources found for " + extensionName);
        }

        ExtensionBean bean = metaStore.getDetail(extensionName);
        JSONObject resultObject = new JSONObject();
        try {
            resultObject.put(EXTENSION_NAME, bean.getExtensionName());
            resultObject.put(EXTENSION_TYPE, bean.getExtensionType());
            resultObject.put(EXTENSION_DESC, bean.getDescription());
            resultObject.put(EXTENSION_LOCATION, bean.getLocation());
        } catch (JSONException e) {
            LOG.error("Exception in buildDetailResults:", e);
            throw new FalconException(e);
        }
        return resultObject;
    }

    private Map<String, List<Entity>> groupEntitiesByJob(List<Entity> entities) {
        Map<String, List<Entity>> groupedEntities = new HashMap<>();
        for (Entity entity : entities) {
            String jobName = getJobNameFromTag(entity.getTags());
            if (!groupedEntities.containsKey(jobName)) {
                groupedEntities.put(jobName, new ArrayList<Entity>());
            }
            groupedEntities.get(jobName).add(entity);
        }
        return groupedEntities;
    }

    private String getJobNameFromTag(String tags) {
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

    private static void checkIfExtensionServiceIsEnabled() {
        if (!Services.get().isRegistered(ExtensionService.SERVICE_NAME)) {
            throw FalconWebException.newAPIException(
                    ExtensionService.SERVICE_NAME + " is not enabled.", Response.Status.NOT_FOUND);
        }
    }
}
