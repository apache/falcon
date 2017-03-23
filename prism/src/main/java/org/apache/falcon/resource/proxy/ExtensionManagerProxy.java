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

package org.apache.falcon.resource.proxy;

import com.sun.jersey.multipart.FormDataBodyPart;
import com.sun.jersey.multipart.FormDataParam;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Properties;
import javax.servlet.ServletInputStream;
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
import javax.xml.bind.JAXBException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.io.IOUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.FalconWebException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.parser.ProcessEntityParser;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.extensions.Extension;
import org.apache.falcon.extensions.ExtensionProperties;
import org.apache.falcon.extensions.ExtensionService;
import org.apache.falcon.extensions.ExtensionType;
import org.apache.falcon.extensions.jdbc.ExtensionMetaStore;
import org.apache.falcon.extensions.store.ExtensionStore;
import org.apache.falcon.persistence.ExtensionBean;
import org.apache.falcon.persistence.ExtensionJobsBean;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.AbstractExtensionManager;
import org.apache.falcon.resource.ExtensionInstanceList;
import org.apache.falcon.resource.ExtensionJobList;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.service.Services;
import org.apache.falcon.util.DeploymentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Jersey Resource for extension job operations.
 */
@Path("extension")
public class ExtensionManagerProxy extends AbstractExtensionManager {
    public static final Logger LOG = LoggerFactory.getLogger(ExtensionManagerProxy.class);

    private Extension extension = new Extension();
    private static final String README = "README";

    private boolean embeddedMode = DeploymentUtil.isEmbeddedMode();
    private String currentColo = DeploymentUtil.getCurrentColo();
    private EntityProxyUtil entityProxyUtil = new EntityProxyUtil();

    private static final String EXTENSION_PROPERTY_JSON_SUFFIX = "-properties.json";

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    @GET
    @Path("list{extension-name : (/[^/]+)?}")
    @Produces({MediaType.TEXT_XML, MediaType.APPLICATION_JSON})
    public ExtensionJobList getExtensionJobs(
            @PathParam("extension-name") String extensionName,
            @DefaultValue(ASCENDING_SORT_ORDER) @QueryParam("sortOrder") String sortOrder,
            @DefaultValue("") @QueryParam("doAs") String doAsUser) {
        checkIfExtensionServiceIsEnabled();
        if (StringUtils.isNotBlank(extensionName)) {
            extensionName = extensionName.substring(1);
            getExtensionIfExists(extensionName);
        }

        try {
            return super.getExtensionJobs(extensionName, sortOrder, doAsUser);
        } catch (Throwable e) {
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
        LOG.error("instances is not supported on Falcon extensions. Use Falcon instance api on individual entities.");
        throw FalconWebException.newAPIException("instances is not supported on Falcon extensions. Use Falcon instance "
                + "api on individual entities.");
    }

    @POST
    @Path("schedule/{job-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    public APIResult schedule(@PathParam("job-name") String jobName,
                              @Context HttpServletRequest request,
                              @QueryParam("colo") final String coloExpr,
                              @DefaultValue("") @QueryParam("doAs") String doAsUser) {
        checkIfExtensionServiceIsEnabled();
        ExtensionMetaStore metaStore = ExtensionStore.getMetaStore();
        ExtensionJobsBean extensionJobsBean = metaStore.getExtensionJobDetails(jobName);
        if (extensionJobsBean == null) {
            // return failure if the extension job doesn't exist
            LOG.error("Extension Job not found:" + jobName);
            throw FalconWebException.newAPIException("ExtensionJob not found:" + jobName,
                    Response.Status.NOT_FOUND);
        }
        checkIfExtensionIsEnabled(extensionJobsBean.getExtensionName());

        SortedMap<EntityType, List<String>> entityMap;
        try {
            entityMap = getJobEntities(extensionJobsBean);
            scheduleEntities(entityMap, request, coloExpr);
        } catch (FalconException e) {
            LOG.error("Error while scheduling entities of the extension: " + jobName + ": ", e);
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
        return new APIResult(APIResult.Status.SUCCEEDED, "Extension job " + jobName + " scheduled successfully");
    }

    @POST
    @Path("suspend/{job-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    public APIResult suspend(@PathParam("job-name") String jobName,
                             @Context HttpServletRequest request,
                             @DefaultValue("") @QueryParam("doAs") String doAsUser,
                             @QueryParam("colo") final String coloExpr) {
        checkIfExtensionServiceIsEnabled();
        ExtensionMetaStore metaStore = ExtensionStore.getMetaStore();
        ExtensionJobsBean extensionJobsBean = metaStore.getExtensionJobDetails(jobName);
        if (extensionJobsBean == null) {
            // return failure if the extension job doesn't exist
            LOG.error("Extension Job not found:" + jobName);
            throw FalconWebException.newAPIException("ExtensionJob not found:" + jobName,
                    Response.Status.NOT_FOUND);
        }

        try {
            SortedMap<EntityType, List<String>> entityNameMap = getJobEntities(extensionJobsBean);
            suspendEntities(entityNameMap, coloExpr, request);
        } catch (FalconException e) {
            LOG.error("Error while suspending entities of the extension: " + jobName + ": ", e);
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
        return new APIResult(APIResult.Status.SUCCEEDED, "Extension job " + jobName + " suspended successfully");
    }

    private void suspendEntities(SortedMap<EntityType, List<String>> entityNameMap, String coloExpr,
                                 final HttpServletRequest request) throws FalconException {
        HttpServletRequest bufferedRequest = new BufferedRequest(request);
        for (Map.Entry<EntityType, List<String>> entityTypeEntry : entityNameMap.entrySet()) {
            for (final String entityName : entityTypeEntry.getValue()) {
                entityProxyUtil.proxySuspend(entityTypeEntry.getKey().name(), entityName, coloExpr, bufferedRequest);
            }
        }
    }

    private void resumeEntities(SortedMap<EntityType, List<String>> entityNameMap, String coloExpr,
                                final HttpServletRequest request) throws FalconException {
        HttpServletRequest bufferedRequest = new BufferedRequest(request);
        for (Map.Entry<EntityType, List<String>> entityTypeEntry : entityNameMap.entrySet()) {
            for (final String entityName : entityTypeEntry.getValue()) {
                entityProxyUtil.proxyResume(entityTypeEntry.getKey().name(), entityName, coloExpr, bufferedRequest);
            }
        }
    }

    @POST
    @Path("resume/{job-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    public APIResult resume(@PathParam("job-name") String jobName,
                            @Context HttpServletRequest request,
                            @QueryParam("colo") final String coloExpr,
                            @DefaultValue("") @QueryParam("doAs") String doAsUser) {
        checkIfExtensionServiceIsEnabled();
        ExtensionMetaStore metaStore = ExtensionStore.getMetaStore();
        ExtensionJobsBean extensionJobsBean = metaStore.getExtensionJobDetails(jobName);
        if (extensionJobsBean == null) {
            // return failure if the extension job doesn't exist
            LOG.error("Extension Job not found:" + jobName);
            throw FalconWebException.newAPIException("ExtensionJob not found:" + jobName,
                    Response.Status.NOT_FOUND);
        }
        try {
            SortedMap<EntityType, List<String>> entityNameMap = getJobEntities(extensionJobsBean);
            resumeEntities(entityNameMap, coloExpr, request);
        } catch (FalconException e) {
            LOG.error("Error while resuming entities of the extension: " + jobName + ": ", e);
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
        return new APIResult(APIResult.Status.SUCCEEDED, "Extension job " + jobName + " resumed successfully");
    }

    @POST
    @Path("delete/{job-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    public APIResult delete(@PathParam("job-name") String jobName,
                            @Context HttpServletRequest request,
                            @DefaultValue("") @QueryParam("doAs") String doAsUser) {
        checkIfExtensionServiceIsEnabled();
        ExtensionMetaStore metaStore = ExtensionStore.getMetaStore();
        ExtensionJobsBean extensionJobsBean = metaStore.getExtensionJobDetails(jobName);
        if (extensionJobsBean == null) {
            // return failure if the extension job doesn't exist
            return new APIResult(APIResult.Status.SUCCEEDED,
                    "Extension job " + jobName + " doesn't exist. Nothing to delete.");
        }

        SortedMap<EntityType, List<String>> entityMap;
        try {
            entityMap = getJobEntities(extensionJobsBean);
            deleteEntities(entityMap, request);
        } catch (FalconException e) {
            LOG.error("Error when deleting extension job: " + jobName + ": ", e);
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
        metaStore.deleteExtensionJob(jobName);
        return new APIResult(APIResult.Status.SUCCEEDED, "Extension job " + jobName + " deleted successfully");
    }

    @POST
    @Path("submit/{extension-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.MULTIPART_FORM_DATA,
            MediaType.APPLICATION_OCTET_STREAM})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    public APIResult submit(
            @PathParam("extension-name") String extensionName,
            @Context HttpServletRequest request,
            @DefaultValue("") @QueryParam("doAs") String doAsUser,
            @QueryParam("jobName") String jobName,
            @FormDataParam("processes") List<FormDataBodyPart> processForms,
            @FormDataParam("feeds") List<FormDataBodyPart> feedForms,
            @FormDataParam("config") InputStream config) {
        checkIfExtensionServiceIsEnabled();
        checkIfExtensionIsEnabled(extensionName);
        checkIfExtensionJobNameExists(jobName, extensionName);
        SortedMap<EntityType, List<Entity>> entityMap;
        try {
            entityMap = getEntityList(extensionName, jobName, feedForms, processForms, config);
            submitEntities(extensionName, jobName, entityMap, config, request);
        } catch (FalconException | IOException | JAXBException e) {
            LOG.error("Error while submitting extension job: ", e);
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
        return new APIResult(APIResult.Status.SUCCEEDED, "Extension job submitted successfully:" + jobName);
    }

    private SortedMap<EntityType, List<Entity>> getEntityList(String extensionName, String jobName,
                                                              List<FormDataBodyPart> feedForms,
                                                              List<FormDataBodyPart> processForms, InputStream config)
        throws FalconException, IOException {
        List<Entity> processes = getProcesses(processForms);
        List<Entity> feeds = getFeeds(feedForms);
        ExtensionType extensionType = getExtensionType(extensionName);
        List<Entity> entities;
        TreeMap<EntityType, List<Entity>> entityMap = new TreeMap<>();
        if (ExtensionType.TRUSTED.equals(extensionType)) {
            entities = extension.getEntities(jobName, addJobNameToConf(config, jobName));
            feeds = new ArrayList<>();
            processes = new ArrayList<>();
            for (Entity entity : entities) {
                if (EntityType.FEED.equals(entity.getEntityType())) {
                    feeds.add(entity);
                } else {
                    processes.add(entity);
                }
            }
        }
        // add tags on extension name and job
        EntityUtil.applyTags(extensionName, jobName, processes);
        EntityUtil.applyTags(extensionName, jobName, feeds);
        entityMap.put(EntityType.PROCESS, processes);
        entityMap.put(EntityType.FEED, feeds);
        return entityMap;
    }

    private InputStream addJobNameToConf(InputStream conf, String jobName) throws  FalconException{
        Properties inputProperties = new Properties();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            inputProperties.load(conf);
            inputProperties.setProperty(ExtensionProperties.JOB_NAME.getName(), jobName);
            inputProperties.store(output, null);
        } catch (IOException e) {
            LOG.error("Error in reading the config stream");
            throw new FalconException("Error while reading the config stream", e);
        }
        return new ByteArrayInputStream(output.toByteArray());
    }

    private ExtensionType getExtensionType(String extensionName) {
        ExtensionBean extensionDetails = getExtensionIfExists(extensionName);
        return extensionDetails.getExtensionType();
    }

    private String getExtensionName(String jobName) {
        ExtensionMetaStore metaStore = ExtensionStore.getMetaStore();
        ExtensionJobsBean extensionJobDetails = metaStore.getExtensionJobDetails(jobName);
        if (extensionJobDetails == null) {
            // return failure if the extension job doesn't exist
            LOG.error("Extension job not found: " + jobName);
            throw FalconWebException.newAPIException("Extension Job not found:" + jobName,
                    Response.Status.NOT_FOUND);
        }
        return extensionJobDetails.getExtensionName();
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
            @QueryParam("colo") final String coloExpr,
            @FormDataParam("processes") List<FormDataBodyPart> processForms,
            @FormDataParam("feeds") List<FormDataBodyPart> feedForms,
            @FormDataParam("config") InputStream config) {
        checkIfExtensionServiceIsEnabled();
        checkIfExtensionIsEnabled(extensionName);
        checkIfExtensionJobNameExists(jobName, extensionName);
        SortedMap<EntityType, List<Entity>> entityMap;
        SortedMap<EntityType, List<String>> entityNameMap;
        ExtensionMetaStore metaStore = ExtensionStore.getMetaStore();
        try {
            entityMap = getEntityList(extensionName, jobName, feedForms, processForms, config);
            submitEntities(extensionName, jobName, entityMap, config, request);
            entityNameMap = getJobEntities(metaStore.getExtensionJobDetails(jobName));
            scheduleEntities(entityNameMap, request, coloExpr);
        } catch (FalconException | IOException | JAXBException e) {
            LOG.error("Error while submitting extension job: ", e);
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
        return new APIResult(APIResult.Status.SUCCEEDED, "Extension job submitted and scheduled successfully");
    }

    private void scheduleEntities(SortedMap<EntityType, List<String>> entityMap, HttpServletRequest request,
                                  String coloExpr) throws FalconException {
        HttpServletRequest bufferedRequest = new BufferedRequest(request);
        for (Map.Entry<EntityType, List<String>> entityTypeEntry : entityMap.entrySet()) {
            for (final String entityName : entityTypeEntry.getValue()) {
                entityProxyUtil.proxySchedule(entityTypeEntry.getKey().name(), entityName, coloExpr,
                        Boolean.FALSE, "", bufferedRequest);
            }
        }
    }

    private BufferedRequest getBufferedRequest(HttpServletRequest request) {
        if (request instanceof BufferedRequest) {
            return (BufferedRequest) request;
        }
        return new BufferedRequest(request);
    }

    private void deleteEntities(SortedMap<EntityType, List<String>> entityMap, HttpServletRequest request)
        throws FalconException {
        for (Map.Entry<EntityType, List<String>> entityTypeEntry : entityMap.entrySet()) {
            for (final String entityName : entityTypeEntry.getValue()) {
                HttpServletRequest bufferedRequest = new BufferedRequest(request);
                entityProxyUtil.proxyDelete(entityTypeEntry.getKey().name(), entityName, bufferedRequest);
                if (!embeddedMode) {
                    super.delete(bufferedRequest, entityTypeEntry.getKey().name(), entityName, currentColo);
                }
            }
        }
    }

    private void submitEntities(String extensionName, String jobName,
                                SortedMap<EntityType, List<Entity>> entityMap, InputStream configStream,
                                HttpServletRequest request)
        throws FalconException, IOException, JAXBException {
        List<Entity> feeds = entityMap.get(EntityType.FEED);
        List<Entity> processes = entityMap.get(EntityType.PROCESS);
        validateFeeds(feeds, jobName);
        validateProcesses(processes, jobName);
        List<String> feedNames = new ArrayList<>();
        List<String> processNames = new ArrayList<>();

        ExtensionMetaStore metaStore = ExtensionStore.getMetaStore();
        byte[] configBytes = null;
        if (configStream != null) {
            configBytes = IOUtils.toByteArray(configStream);
        }
        for (Map.Entry<EntityType, List<Entity>> entry : entityMap.entrySet()) {
            for (final Entity entity : entry.getValue()) {
                if (entity.getEntityType().equals(EntityType.FEED)) {
                    feedNames.add(entity.getName());
                } else {
                    processNames.add(entity.getName());
                }
            }
        }
        metaStore.storeExtensionJob(jobName, extensionName, feedNames, processNames, configBytes);

        for(Map.Entry<EntityType, List<Entity>> entry : entityMap.entrySet()){
            for(final Entity entity : entry.getValue()){
                final HttpServletRequest bufferedRequest = getEntityStream(entity, entity.getEntityType(), request);
                final Set<String> colos = getApplicableColos(entity.getEntityType().toString(), entity);
                entityProxyUtil.proxySubmit(entity.getEntityType().toString(), bufferedRequest, entity, colos);
                if (!embeddedMode) {
                    super.submit(bufferedRequest, entity.getEntityType().toString(), currentColo);
                }
            }
        }
    }

    private void updateEntities(String extensionName, String jobName,
                                SortedMap<EntityType, List<Entity>> entityMap, InputStream configStream,
                                HttpServletRequest request) throws FalconException, IOException, JAXBException {
        List<Entity> feeds = entityMap.get(EntityType.FEED);
        List<Entity> processes = entityMap.get(EntityType.PROCESS);
        validateFeeds(feeds, jobName);
        validateProcesses(processes, jobName);
        List<String> feedNames = new ArrayList<>();
        List<String> processNames = new ArrayList<>();

        for (Map.Entry<EntityType, List<Entity>> entry : entityMap.entrySet()) {
            for (final Entity entity : entry.getValue()) {
                final String entityType = entity.getEntityType().toString();
                final String entityName = entity.getName();
                final HttpServletRequest bufferedRequest = getEntityStream(entity, entity.getEntityType(), request);
                entityProxyUtil.proxyUpdate(entityType, entityName, Boolean.FALSE, bufferedRequest, entity);
                if (!embeddedMode) {
                    super.update(bufferedRequest, entity.getEntityType().toString(), entity.getName(), currentColo,
                            Boolean.FALSE);
                }
                if (entity.getEntityType().equals(EntityType.FEED)) {
                    feedNames.add(entity.getName());
                } else {
                    processNames.add(entity.getName());
                }
            }
        }

        ExtensionMetaStore metaStore = ExtensionStore.getMetaStore();
        byte[] configBytes = null;
        if (configStream != null) {
            configBytes = IOUtils.toByteArray(configStream);
        }
        metaStore.updateExtensionJob(jobName, extensionName, feedNames, processNames, configBytes);
    }

    private HttpServletRequest getEntityStream(Entity entity, EntityType type, HttpServletRequest request)
        throws IOException, JAXBException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        type.getMarshaller().marshal(entity, baos);
        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(baos.toByteArray());
        ServletInputStream servletInputStream = new ServletInputStream() {
            public int read() throws IOException {
                return byteArrayInputStream.read();
            }
        };
        return getBufferedRequest(new HttpServletRequestInputStreamWrapper(request, servletInputStream));
    }

    private void validateFeeds(List<Entity> feeds, String jobName) throws FalconException {
        for (Entity feed : feeds) {
            checkIfPartOfAnotherExtension(feed.getName(), EntityType.FEED, jobName);
            super.validate(feed);
        }
    }

    private void validateProcesses(List<Entity> processes, String jobName) throws FalconException {
        ProcessEntityParser processEntityParser = new ProcessEntityParser();
        for (Entity process : processes) {
            checkIfPartOfAnotherExtension(process.getName(), EntityType.PROCESS, jobName);
            processEntityParser.validate((Process) process, false);
        }
    }

    private List<Entity> getFeeds(List<FormDataBodyPart> feedForms) {
        List<Entity> feeds = new ArrayList<>();
        if (feedForms != null && !feedForms.isEmpty()) {
            for (FormDataBodyPart formDataBodyPart : feedForms) {
                feeds.add(formDataBodyPart.getValueAs(Feed.class));
            }
        }
        return feeds;
    }

    private List<Entity> getProcesses(List<FormDataBodyPart> processForms) {
        List<Entity> processes = new ArrayList<>();
        if (processForms != null && !processForms.isEmpty()) {
            for (FormDataBodyPart formDataBodyPart : processForms) {
                processes.add(formDataBodyPart.getValueAs(Process.class));
            }
        }
        return processes;
    }

    @POST
    @Path("update/{job-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.MULTIPART_FORM_DATA})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    public APIResult update(
            @PathParam("job-name") String jobName,
            @Context HttpServletRequest request,
            @DefaultValue("") @QueryParam("doAs") String doAsUser,
            @FormDataParam("processes") List<FormDataBodyPart> processForms,
            @FormDataParam("feeds") List<FormDataBodyPart> feedForms,
            @FormDataParam("config") InputStream config) {
        checkIfExtensionServiceIsEnabled();

        SortedMap<EntityType, List<Entity>> entityMap;
        String extensionName = getExtensionName(jobName);
        checkIfExtensionIsEnabled(extensionName);
        try {
            entityMap = getEntityList(extensionName, jobName, feedForms, processForms, config);
            if (entityMap.get(EntityType.FEED).isEmpty() && entityMap.get(EntityType.PROCESS).isEmpty()) {
                // return failure if the extension job doesn't exist
                return new APIResult(APIResult.Status.FAILED, "Extension job " + jobName + " doesn't exist.");
            }
            updateEntities(extensionName, jobName, entityMap, config, request);
        } catch (FalconException | IOException | JAXBException e) {
            LOG.error("Error while updating extension job: " + jobName, e);
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
        ExtensionType extensionType = getExtensionType(extensionName);
        if (!ExtensionType.TRUSTED.equals(extensionType)) {
            throw FalconWebException.newAPIException("Extension validation is supported only for trusted extensions");
        }
        try {
            List<Entity> entities = extension.getEntities(extensionName, request.getInputStream());
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
    @Produces({MediaType.TEXT_PLAIN, MediaType.TEXT_XML})
    public APIResult getExtensions() {
        checkIfExtensionServiceIsEnabled();
        try {
            return super.getExtensions();
        } catch (FalconWebException e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("describe/{extension-name}")
    @Produces({MediaType.TEXT_PLAIN, MediaType.TEXT_XML})
    public APIResult getExtensionDescription(
            @PathParam("extension-name") String extensionName) {
        checkIfExtensionServiceIsEnabled();
        ExtensionBean extensionBean = getExtensionIfExists(extensionName);
        try {
            String extensionResourcePath = extensionBean.getLocation() + File.separator +  README;
            return new APIResult(APIResult.Status.SUCCEEDED, ExtensionStore.get().getResource(extensionResourcePath));
        } catch (FalconException e) {
            throw FalconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } catch (Throwable e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("detail/{extension-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    public APIResult getDetail(@PathParam("extension-name") String extensionName) {
        checkIfExtensionServiceIsEnabled();
        validateExtensionName(extensionName);
        try {
            return super.getExtensionDetail(extensionName);
        } catch (Throwable e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("extensionJobDetails/{job-name}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    public APIResult getExtensionJobDetail(@PathParam("job-name") String jobName) {
        checkIfExtensionServiceIsEnabled();
        try {
            return super.getExtensionJobDetail(jobName);
        } catch (Throwable e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Path("unregister/{extension-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_PLAIN, MediaType.TEXT_XML})
    public APIResult deleteExtensionMetadata(
            @PathParam("extension-name") String extensionName) {
        checkIfExtensionServiceIsEnabled();
        try {
            return super.deleteExtensionMetadata(extensionName);
        } catch (Throwable e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Path("register/{extension-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_PLAIN, MediaType.TEXT_XML})
    public APIResult registerExtensionMetadata(
            @PathParam("extension-name") String extensionName,
            @QueryParam("path") String path,
            @QueryParam("description") String description) {
        checkIfExtensionServiceIsEnabled();
        try {
            return super.registerExtensionMetadata(extensionName, path, description, CurrentUser.getUser());
        } catch (Throwable e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @GET
    @Path("definition/{extension-name}")
    @Produces({MediaType.TEXT_PLAIN, MediaType.TEXT_XML})
    public APIResult getExtensionDefinition(
            @PathParam("extension-name") String extensionName) {
        checkIfExtensionServiceIsEnabled();
        ExtensionBean extensionBean = getExtensionIfExists(extensionName);
        try {
            ExtensionType extensionType = extensionBean.getExtensionType();
            String extensionResourcePath;
            if (ExtensionType.TRUSTED.equals(extensionType)) {
                extensionResourcePath = extensionBean.getLocation() + "/META/"
                        + extensionName.toLowerCase() + EXTENSION_PROPERTY_JSON_SUFFIX;
            } else {
                extensionResourcePath = extensionBean.getLocation() + "/META";
            }
            return new APIResult(APIResult.Status.SUCCEEDED,
                    ExtensionStore.get().getResource(extensionResourcePath));
        } catch (FalconException e) {
            throw FalconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
        } catch (Throwable e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Path("disable/{extension-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_PLAIN, MediaType.TEXT_XML})
    public APIResult disableExtension(
            @PathParam("extension-name") String extensionName) {
        checkIfExtensionServiceIsEnabled();
        try {
            return new APIResult(APIResult.Status.SUCCEEDED, super.disableExtension(extensionName,
                    CurrentUser.getUser()));
        } catch (Throwable e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    @POST
    @Path("enable/{extension-name}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_PLAIN, MediaType.TEXT_XML})
    public APIResult enableExtension(
            @PathParam("extension-name") String extensionName) {
        checkIfExtensionServiceIsEnabled();
        try {
            return new APIResult(APIResult.Status.SUCCEEDED, super.enableExtension(extensionName,
                    CurrentUser.getUser()));
        } catch (Throwable e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    private static void checkIfExtensionServiceIsEnabled() {
        if (!Services.get().isRegistered(ExtensionService.SERVICE_NAME)) {
            LOG.error(ExtensionService.SERVICE_NAME + " is not enabled.");
            throw FalconWebException.newAPIException(
                    ExtensionService.SERVICE_NAME + " is not enabled.", Response.Status.NOT_FOUND);
        }
    }
}
