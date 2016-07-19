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

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.FalconRuntimException;
import org.apache.falcon.FalconWebException;
import org.apache.falcon.entity.EntityNotRegisteredException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.monitors.Dimension;
import org.apache.falcon.monitors.Monitored;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.AbstractSchedulableEntityManager;
import org.apache.falcon.resource.EntityList;
import org.apache.falcon.resource.EntitySummaryResult;
import org.apache.falcon.resource.FeedLookupResult;
import org.apache.falcon.resource.SchedulableEntityInstanceResult;
import org.apache.falcon.resource.channel.Channel;
import org.apache.falcon.resource.channel.ChannelFactory;
import org.apache.falcon.util.DeploymentUtil;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A proxy implementation of the schedulable entity operations.
 */
@Path("entities")
public class SchedulableEntityManagerProxy extends AbstractSchedulableEntityManager {
    private static final String PRISM_TAG = "prism";
    public static final String FALCON_TAG = "falcon";

    private final Map<String, Channel> entityManagerChannels = new HashMap<String, Channel>();
    private final Map<String, Channel> configSyncChannels = new HashMap<String, Channel>();
    private boolean embeddedMode = DeploymentUtil.isEmbeddedMode();
    private String currentColo = DeploymentUtil.getCurrentColo();

    public SchedulableEntityManagerProxy() {
        try {
            Set<String> colos = getAllColos();

            for (String colo : colos) {
                initializeFor(colo);
            }

            DeploymentUtil.setPrismMode();
        } catch (FalconException e) {
            throw new FalconRuntimException("Unable to initialize channels", e);
        }
    }

    private void initializeFor(String colo) throws FalconException {
        entityManagerChannels.put(colo, ChannelFactory.get("SchedulableEntityManager", colo));
        configSyncChannels.put(colo, ChannelFactory.get("ConfigSyncService", colo));
    }

    private Channel getConfigSyncChannel(String colo) throws FalconException {
        if (!configSyncChannels.containsKey(colo)) {
            initializeFor(colo);
        }
        return configSyncChannels.get(colo);
    }

    private Channel getEntityManager(String colo) throws FalconException {
        if (!entityManagerChannels.containsKey(colo)) {
            initializeFor(colo);
        }
        return entityManagerChannels.get(colo);
    }

    private BufferedRequest getBufferedRequest(HttpServletRequest request) {
        if (request instanceof BufferedRequest) {
            return (BufferedRequest) request;
        }
        return new BufferedRequest(request);
    }

    @GET
    @Path("sla-alert/{type}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_XML})
    @Monitored(event = "feed-sla-misses")
    public SchedulableEntityInstanceResult getFeedSLAMissPendingAlerts(
            @Dimension("entityType") @PathParam("type") final String entityType,
            @Dimension("entityName") @QueryParam("name") final String entityName,
            @Dimension("start") @QueryParam("start") final String start,
            @Dimension("end") @QueryParam("end") final String end,
            @Dimension("colo") @QueryParam("colo") final String colo) {
        try {
            validateSlaParams(entityType, entityName, start, end, colo);
        } catch (Exception e) {
            throw FalconWebException.newAPIException(e);
        }
        return new EntityProxy<SchedulableEntityInstanceResult>(entityType, entityName,
                SchedulableEntityInstanceResult.class) {
            @Override
            protected Set<String> getColosToApply() {
                return getApplicableColos(entityType, entityName);
            }

            @Override
            protected SchedulableEntityInstanceResult doExecute(String colo) throws FalconException {
                return getEntityManager(colo).invoke("getEntitySLAMissPendingAlerts", entityType, entityName,
                        start, end, colo);
            }
        }.execute();
    }

    /**
     * Submit the given entity.
     * @param request Servlet Request
     * @param type Valid options are cluster, feed or process.
     * @param ignore colo is ignored
     * @return Result of the submission.
     */
    @POST
    @Path("submit/{type}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "submit")
    @Override
    public APIResult submit(
            @Context HttpServletRequest request, @Dimension("entityType") @PathParam("type") final String type,
            @Dimension("colo") @QueryParam("colo") final String ignore) {

        final HttpServletRequest bufferedRequest = getBufferedRequest(request);

        final Entity entity = getEntity(bufferedRequest, type);
        Map<String, APIResult> results = new HashMap<String, APIResult>();
        final Set<String> colos = getApplicableColos(type, entity);

        validateEntity(entity, colos);

        results.put(FALCON_TAG, new EntityProxy(type, entity.getName()) {
            @Override
            protected Set<String> getColosToApply() {
                return colos;
            }

            @Override
            protected APIResult doExecute(String colo) throws FalconException {
                return getConfigSyncChannel(colo).invoke("submit", bufferedRequest, type, colo);
            }
        }.execute());

        if (!embeddedMode) {
            results.put(PRISM_TAG, super.submit(bufferedRequest, type, currentColo));
        }
        return consolidateResult(results, APIResult.class);
    }

    private void validateEntity(Entity entity, Set<String> applicableColos) {
        if (entity.getEntityType() != EntityType.CLUSTER || embeddedMode) {
            return;
        }
        // If the submitted entity is a cluster, ensure its spec. has one of the valid colos
        String colo = ((Cluster) entity).getColo();
        if (!applicableColos.contains(colo)) {
            throw FalconWebException.newAPIException("The colo mentioned in the cluster specification, "
                    + colo + ", is not listed in Prism runtime.");
        }
    }

    private Entity getEntity(HttpServletRequest request, String type) {
        try {
            request.getInputStream().reset();
            Entity entity = deserializeEntity(request.getInputStream(), EntityType.getEnum(type));
            request.getInputStream().reset();
            return entity;
        } catch (Exception e) {
            throw FalconWebException.newAPIException(e);
        }
    }

    /**
     * Validates the submitted entity.
     * @param request Servlet Request
     * @param type Valid options are cluster, feed or process.
     * @param skipDryRun Optional query param, Falcon skips oozie dryrun when value is set to true.
     * @return Result of the validation.
     */
    @POST
    @Path("validate/{type}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Override
    public APIResult validate(@Context final HttpServletRequest request, @PathParam("type") final String type,
                              @QueryParam("skipDryRun") final Boolean skipDryRun) {
        final HttpServletRequest bufferedRequest = getBufferedRequest(request);
        EntityType entityType = EntityType.getEnum(type);
        final Entity entity;
        try {
            entity = deserializeEntity(bufferedRequest.getInputStream(), entityType);
            bufferedRequest.getInputStream().reset();
        } catch (Exception e) {
            throw FalconWebException.newAPIException("Unable to parse entity definition");
        }
        return new EntityProxy(type, entity.getName()) {
            @Override
            protected Set<String> getColosToApply() {
                return getApplicableColos(type, entity);
            }

            @Override
            protected APIResult doExecute(String colo) throws FalconException {
                return getEntityManager(colo).invoke("validate", bufferedRequest, type, skipDryRun);
            }
        }.execute();
    }

    /**
     * Delete the specified entity.
     * @param request Servlet Request
     * @param type Valid options are cluster, feed or process.
     * @param entity Name of the entity.
     * @param ignore colo is ignored
     * @return Results of the delete operation.
     */
    @DELETE
    @Path("delete/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "delete")
    @Override
    public APIResult delete(
            @Context HttpServletRequest request, @Dimension("entityType") @PathParam("type") final String type,
            @Dimension("entityName") @PathParam("entity") final String entity,
            @Dimension("colo") @QueryParam("colo") String ignore) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        Map<String, APIResult> results = new HashMap<String, APIResult>();

        results.put(FALCON_TAG, new EntityProxy(type, entity) {
            @Override
            public APIResult execute() {
                try {
                    EntityUtil.getEntity(type, entity);
                    return super.execute();
                } catch (EntityNotRegisteredException e) {
                    return new APIResult(APIResult.Status.SUCCEEDED,
                            entity + "(" + type + ") doesn't exist. Nothing to do");
                } catch (FalconException e) {
                    throw FalconWebException.newAPIException(e);
                }
            }

            @Override
            protected APIResult doExecute(String colo) throws FalconException {
                return getConfigSyncChannel(colo).invoke("delete", bufferedRequest, type, entity, colo);
            }
        }.execute());

        // delete only if deleted from everywhere
        if (!embeddedMode && results.get(FALCON_TAG).getStatus() == APIResult.Status.SUCCEEDED) {
            results.put(PRISM_TAG, super.delete(bufferedRequest, type, entity, currentColo));
        }
        return consolidateResult(results, APIResult.class);
    }

    /**
     * Updates the submitted entity.
     * @param request Servlet Request
     * @param type Valid options are feed or process.
     * @param entityName Name of the entity.
     * @param ignore colo is ignored
     * @param skipDryRun Optional query param, Falcon skips oozie dryrun when value is set to true.
     * @return Result of the validation.
     */
    @POST
    @Path("update/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "update")
    @Override
    public APIResult update(
            @Context HttpServletRequest request, @Dimension("entityType") @PathParam("type") final String type,
            @Dimension("entityName") @PathParam("entity") final String entityName,
            @Dimension("colo") @QueryParam("colo") String ignore,
            @QueryParam("skipDryRun") final Boolean skipDryRun) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        final Set<String> oldColos = getApplicableColos(type, entityName);
        final Set<String> newColos = getApplicableColos(type, getEntity(bufferedRequest, type));
        final Set<String> mergedColos = new HashSet<String>();
        mergedColos.addAll(oldColos);
        mergedColos.retainAll(newColos);    //Common colos where update should be called
        newColos.removeAll(oldColos);   //New colos where submit should be called
        oldColos.removeAll(mergedColos);   //Old colos where delete should be called

        Map<String, APIResult> results = new HashMap<String, APIResult>();
        boolean result = true;
        if (!oldColos.isEmpty()) {
            results.put(FALCON_TAG + "/delete", new EntityProxy(type, entityName) {
                @Override
                protected Set<String> getColosToApply() {
                    return oldColos;
                }

                @Override
                protected APIResult doExecute(String colo) throws FalconException {
                    return getConfigSyncChannel(colo).invoke("delete", bufferedRequest, type, entityName, colo);
                }
            }.execute());
        }

        if (!mergedColos.isEmpty()) {
            results.put(FALCON_TAG + "/update", new EntityProxy(type, entityName) {
                @Override
                protected Set<String> getColosToApply() {
                    return mergedColos;
                }

                @Override
                protected APIResult doExecute(String colo) throws FalconException {
                    return getConfigSyncChannel(colo).invoke("update", bufferedRequest, type, entityName,
                            colo, skipDryRun);
                }
            }.execute());
        }

        if (!newColos.isEmpty()) {
            results.put(FALCON_TAG + "/submit", new EntityProxy(type, entityName) {
                @Override
                protected Set<String> getColosToApply() {
                    return newColos;
                }

                @Override
                protected APIResult doExecute(String colo) throws FalconException {
                    return getConfigSyncChannel(colo).invoke("submit", bufferedRequest, type, colo);
                }
            }.execute());
        }

        for (APIResult apiResult : results.values()) {
            if (apiResult.getStatus() != APIResult.Status.SUCCEEDED) {
                result = false;
            }
        }

        // update only if all are updated
        if (!embeddedMode && result) {
            results.put(PRISM_TAG, super.update(bufferedRequest, type, entityName, currentColo, skipDryRun));
        }

        return consolidateResult(results, APIResult.class);
    }

    /**
     * Updates the dependent entities of a cluster in workflow engine.
     * @param clusterName Name of cluster.
     * @param ignore colo.
     * @param skipDryRun Optional query param, Falcon skips oozie dryrun when value is set to true.
     * @return Result of the validation.
     */
    @POST
    @Path("updateClusterDependents/{clusterName}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "updateClusterDependents")
    @Override
    public APIResult updateClusterDependents(
            @Dimension("entityName") @PathParam("clusterName") final String clusterName,
            @Dimension("colo") @QueryParam("colo") String ignore,
            @QueryParam("skipDryRun") final Boolean skipDryRun) {

        final Set<String> allColos = getApplicableColos("cluster", clusterName);
        Map<String, APIResult> results = new HashMap<String, APIResult>();
        boolean result = true;

        if (!allColos.isEmpty()) {
            results.put(FALCON_TAG + "/updateClusterDependents", new EntityProxy("cluster", clusterName) {
                @Override
                protected Set<String> getColosToApply() {
                    return allColos;
                }

                @Override
                protected APIResult doExecute(String colo) throws FalconException {
                    return getConfigSyncChannel(colo).invoke("updateClusterDependents", clusterName,
                            colo, skipDryRun);
                }
            }.execute());
        }

        for (APIResult apiResult : results.values()) {
            if (apiResult.getStatus() != APIResult.Status.SUCCEEDED) {
                result = false;
            }
        }
        // update only if all are updated
        if (!embeddedMode && result) {
            results.put(PRISM_TAG, super.updateClusterDependents(clusterName, currentColo, skipDryRun));
        }

        return consolidateResult(results, APIResult.class);
    }

    /**
     * Force updates the entity.
     * @param type Valid options are feed or process.
     * @param entityName Name of the entity.
     * @param coloExpr Colo on which the query should be run.
     * @param skipDryRun Optional query param, Falcon skips oozie dryrun when value is set to true.
     * @return Result of the validation.
     */
    @POST
    @Path("touch/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "touch")
    @Override
    public APIResult touch(
            @Dimension("entityType") @PathParam("type") final String type,
            @Dimension("entityName") @PathParam("entity") final String entityName,
            @Dimension("colo") @QueryParam("colo") final String coloExpr,
            @QueryParam("skipDryRun") final Boolean skipDryRun) {
        final Set<String> colosFromExp = getColosFromExpression(coloExpr, type, entityName);
        return new EntityProxy(type, entityName) {
            @Override
            protected Set<String> getColosToApply() {
                return colosFromExp;
            }

            @Override
            protected APIResult doExecute(String colo) throws FalconException {
                return getEntityManager(colo).invoke("touch", type, entityName, colo, skipDryRun);
            }
        }.execute();
    }

    /**
     * Get status of the entity.
     * @param type Valid options are cluster, feed or process.
     * @param entity Name of the entity.
     * @param coloExpr Colo on which the query should be run.
     * @param showScheduler whether the call should return the scheduler on which the entity is scheduled.
     * @return Status of the entity.
     */
    @GET
    @Path("status/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "status")
    @Override
    public APIResult getStatus(@Dimension("entityType") @PathParam("type") final String type,
                               @Dimension("entityName") @PathParam("entity") final String entity,
                               @Dimension("colo") @QueryParam("colo") final String coloExpr,
                               @Dimension("showScheduler") @QueryParam("showScheduler") final Boolean showScheduler) {
        return new EntityProxy(type, entity) {
            @Override
            protected Set<String> getColosToApply() {
                return getColosFromExpression(coloExpr, type, entity);
            }

            @Override
            protected APIResult doExecute(String colo) throws FalconException {
                return getEntityManager(colo).invoke("getStatus", type, entity, colo, showScheduler);
            }
        }.execute();
    }

    /**
     * Get dependencies of the entity.
     * @param type Valid options are cluster, feed or process.
     * @param entity Name of the entity.
     * @return Dependencies of the entity.
     */
    @GET
    @Path("dependencies/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.APPLICATION_JSON})
    @Monitored(event = "dependencies")
    @Override
    public EntityList getDependencies(@Dimension("entityType") @PathParam("type") String type,
                                      @Dimension("entityName") @PathParam("entity") String entity) {
        return super.getDependencies(type, entity);
    }

    /**
     * Get definition of the entity.
     * @param type Valid options are cluster, feed or process.
     * @param entityName Name of the entity.
     * @return Definition of the entity.
     */
    @GET
    @Path("definition/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Override
    public String getEntityDefinition(@PathParam("type") String type, @PathParam("entity") String entityName) {
        return super.getEntityDefinition(type, entityName);
    }

    /**
     * Schedule an entity.
     * @param request Servlet Request
     * @param type Valid options are feed or process.
     * @param entity Name of the entity.
     * @param coloExpr Colo on which the query should be run.
     * @param skipDryRun Optional query param, Falcon skips oozie dryrun when value is set to true.
     * @return Result of the schedule command.
     */
    @POST
    @Path("schedule/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "schedule")
    @Override
    public APIResult schedule(@Context final HttpServletRequest request,
                              @Dimension("entityType") @PathParam("type") final String type,
                              @Dimension("entityName") @PathParam("entity") final String entity,
                              @Dimension("colo") @QueryParam("colo") final String coloExpr,
                              @QueryParam("skipDryRun") final Boolean skipDryRun,
                              @QueryParam("properties") final String properties) {

        final HttpServletRequest bufferedRequest = getBufferedRequest(request);
        return new EntityProxy(type, entity) {
            @Override
            protected Set<String> getColosToApply() {
                return getColosFromExpression(coloExpr, type, entity);
            }

            @Override
            protected APIResult doExecute(String colo) throws FalconException {
                return getEntityManager(colo).invoke("schedule", bufferedRequest, type, entity, colo, skipDryRun,
                        properties);
            }
        }.execute();
    }

    /**
     * Submits and schedules an entity.
     * @param request Servlet Request
     * @param type Valid options are feed or process.
     * @param coloExpr Colo on which the query should be run.
     * @param skipDryRun Optional query param, Falcon skips oozie dryrun when value is set to true.
     * @return Result of the submit and schedule command.
     */
    @POST
    @Path("submitAndSchedule/{type}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "submitAndSchedule")
    @Override
    public APIResult submitAndSchedule(
            @Context HttpServletRequest request, @Dimension("entityType") @PathParam("type") String type,
            @Dimension("colo") @QueryParam("colo") String coloExpr,
            @QueryParam("skipDryRun") Boolean skipDryRun,
            @QueryParam("properties") String properties) {
        BufferedRequest bufferedRequest = new BufferedRequest(request);
        String entity = getEntity(bufferedRequest, type).getName();
        Map<String, APIResult> results = new HashMap<String, APIResult>();
        results.put("submit", submit(bufferedRequest, type, coloExpr));
        results.put("schedule", schedule(bufferedRequest, type, entity, coloExpr, skipDryRun, properties));
        return consolidateResult(results, APIResult.class);
    }

    /**
     * Suspend an entity.
     * @param request Servlet Request
     * @param type Valid options are feed or process.
     * @param entity Name of the entity.
     * @param coloExpr Colo on which the query should be run.
     * @return Status of the entity.
     */
    @POST
    @Path("suspend/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "suspend")
    @Override
    public APIResult suspend(@Context final HttpServletRequest request,
                             @Dimension("entityType") @PathParam("type") final String type,
                             @Dimension("entityName") @PathParam("entity") final String entity,
                             @Dimension("colo") @QueryParam("colo") final String coloExpr) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        return new EntityProxy(type, entity) {
            @Override
            protected Set<String> getColosToApply() {
                return getColosFromExpression(coloExpr, type, entity);
            }

            @Override
            protected APIResult doExecute(String colo) throws FalconException {
                return getEntityManager(colo).invoke("suspend", bufferedRequest, type, entity, colo);
            }
        }.execute();
    }

    /**
     * Resume a supended entity.
     * @param request Servlet Request
     * @param type Valid options are feed or process.
     * @param entity Name of the entity.
     * @param coloExpr Colo on which the query should be run.
     * @return Result of the resume command.
     */
    @POST
    @Path("resume/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "resume")
    @Override
    public APIResult resume(
            @Context final HttpServletRequest request, @Dimension("entityType") @PathParam("type") final String type,
            @Dimension("entityName") @PathParam("entity") final String entity,
            @Dimension("colo") @QueryParam("colo") final String coloExpr) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        return new EntityProxy(type, entity) {
            @Override
            protected Set<String> getColosToApply() {
                return getColosFromExpression(coloExpr, type, entity);
            }

            @Override
            protected APIResult doExecute(String colo) throws FalconException {
                return getEntityManager(colo).invoke("resume", bufferedRequest, type, entity, colo);
            }
        }.execute();
    }

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck

    /**
     *
     * Get list of the entities.
     * We have two filtering parameters for entity tags: "tags" and "tagkeys".
     * "tags" does the exact match in key=value fashion, while "tagkeys" finds all the entities with the given key as a
     * substring in the tags. This "tagkeys" filter is introduced for the user who doesn't remember the exact tag but
     * some keywords in the tag. It also helps users to save the time of typing long tags.
     * The returned entities will match all the filtering criteria.
     * @param type Comma-separated entity types. Can be empty. Valid entity types are cluster, feed or process.
     * @param fields <optional param> Fields of entity that the user wants to view, separated by commas.
     *               Valid options are STATUS, TAGS, PIPELINES, CLUSTERS.
     * @param nameSubsequence <optional param> Subsequence of entity name. Not case sensitive.
     *                        The entity name needs to contain all the characters in the subsequence in the same order.
     *                        Example 1: "sample1" will match the entity named "SampleFeed1-2".
     *                        Example 2: "mhs" will match the entity named "New-My-Hourly-Summary".
     * @param tagKeywords <optional param> Keywords in tags, separated by comma. Not case sensitive.
     *                    The returned entities will have tags that match all the tag keywords.
     * @param tags <optional param> Return list of entities that have specified tags, separated by a comma.
     *             Query will do AND on tag values.
     *             Example: tags=consumer=consumer@xyz.com,owner=producer@xyz.com
     * @param filterBy <optional param> Filter results by list of field:value pairs.
     *                 Example: filterBy=STATUS:RUNNING,PIPELINES:clickLogs
     *                 Supported filter fields are NAME, STATUS, PIPELINES, CLUSTER.
     *                 Query will do an AND among filterBy fields.
     * @param orderBy <optional param> Field by which results should be ordered.
     *                Supports ordering by "name".
     * @param sortOrder <optional param> Valid options are "asc" and "desc"
     * @param offset <optional param> Show results from the offset, used for pagination. Defaults to 0.
     * @param resultsPerPage <optional param> Number of results to show per request, used for pagination. Only
     *                       integers > 0 are valid, Default is 10.
     * @return Total number of results and a list of entities.
     */
    @GET
    @Path("list{type : (/[^/]+)?}")
    @Produces({MediaType.TEXT_XML, MediaType.APPLICATION_JSON})
    @Monitored(event = "list")
    @Override
    public EntityList getEntityList(@PathParam("type") String type,
                                    @DefaultValue("") @QueryParam("fields") String fields,
                                    @DefaultValue("") @QueryParam("nameseq") String nameSubsequence,
                                    @DefaultValue("") @QueryParam("tagkeys") String tagKeywords,
                                    @DefaultValue("") @QueryParam("tags") String tags,
                                    @DefaultValue("") @QueryParam("filterBy") String filterBy,
                                    @DefaultValue("") @QueryParam("orderBy") String orderBy,
                                    @DefaultValue("asc") @QueryParam("sortOrder") String sortOrder,
                                    @DefaultValue("0") @QueryParam("offset") Integer offset,
                                    @QueryParam("numResults") Integer resultsPerPage,
                                    @QueryParam("doAs") String doAsUser) {
        if (StringUtils.isNotEmpty(type)) {
            type = type.substring(1);
        }
        resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
        return super.getEntityList(fields, nameSubsequence, tagKeywords, type, tags, filterBy,
                orderBy, sortOrder, offset, resultsPerPage, doAsUser);
    }

    /**
     * Given an EntityType and cluster, get list of entities along with summary of N recent instances of each entity.
     * @param type Valid options are feed or process.
     * @param clusterName Show entities that belong to this cluster.
     * @param startStr <optional param> Show entity summaries from this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
     *                 By default, it is set to (end - 2 days).
     * @param endStr <optional param> Show entity summary up to this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
     *               Default is set to now.
     * @param entityFields <optional param> Fields of entity that the user wants to view, separated by commas.
     *                     Valid options are STATUS, TAGS, PIPELINES.
     * @param entityFilter <optional param> Filter results by list of field:value pairs.
     *                     Example: filterBy=STATUS:RUNNING,PIPELINES:clickLogs
     *                     Supported filter fields are NAME, STATUS, PIPELINES, CLUSTER.
     *                     Query will do an AND among filterBy fields.
     * @param entityTags <optional param> Return list of entities that have specified tags, separated by a comma.
     *                   Query will do AND on tag values.
     *                   Example: tags=consumer=consumer@xyz.com,owner=producer@xyz.com
     * @param entityOrderBy <optional param> Field by which results should be ordered.
     *                      Supports ordering by "name".
     * @param entitySortOrder <optional param> Valid options are "asc" and "desc"
     * @param entityOffset <optional param> Show results from the offset, used for pagination. Defaults to 0.
     * @param numEntities <optional param> Number of results to show per request, used for pagination. Only
     *                    integers > 0 are valid, Default is 10.
     * @param numInstanceResults <optional param> Number of recent instances to show per entity. Only integers > 0 are
     *                           valid, Default is 7.
     * @return Show entities along with summary of N instances for each entity.
     */
    @GET
    @Path("summary/{type}")
    @Produces({MediaType.TEXT_XML, MediaType.APPLICATION_JSON})
    @Monitored(event = "summary")
    @Override
    public EntitySummaryResult getEntitySummary(
            @Dimension("type") @PathParam("type") final String type,
            @Dimension("cluster") @QueryParam("cluster") final String clusterName,
            @DefaultValue("") @QueryParam("start") final String startStr,
            @DefaultValue("") @QueryParam("end") final String endStr,
            @DefaultValue("") @QueryParam("fields") final String entityFields,
            @DefaultValue("") @QueryParam("filterBy") final String entityFilter,
            @DefaultValue("") @QueryParam("tags") final String entityTags,
            @DefaultValue("") @QueryParam("orderBy") final String entityOrderBy,
            @DefaultValue("asc") @QueryParam("sortOrder") final String entitySortOrder,
            @DefaultValue("0") @QueryParam("offset") final Integer entityOffset,
            @DefaultValue("10") @QueryParam("numResults") final Integer numEntities,
            @DefaultValue("7") @QueryParam("numInstances") final Integer numInstanceResults,
            @DefaultValue("") @QueryParam("doAs") final String doAsUser) {
        final String entityName = null;
        return new EntityProxy<EntitySummaryResult>(type, null, EntitySummaryResult.class) {
            @Override
            protected Set<String> getColosToApply() {
                Set<String> result = new HashSet<>();
                try {
                    Cluster cluster = EntityUtil.getEntity(EntityType.CLUSTER, clusterName);
                    result.add(cluster.getColo());
                } catch (FalconException e) {
                    // ignore, just return blank result
                }
                return result;
            }

            @Override
            protected EntitySummaryResult doExecute(String colo) throws FalconException {
                EntitySummaryResult es = getEntityManager(colo).invoke("getEntitySummary", type, clusterName, startStr,
                    endStr, entityFields, entityFilter, entityTags, entityOrderBy, entitySortOrder, entityOffset,
                    numEntities, numInstanceResults, doAsUser);
                return es;
            }
        }.execute();
    }

    /**
     * Get the name of the feed along with the location type(meta/data/stats) and cluster on which the given path
     * belongs to this feed.
     * @param type Valid option is feed.
     * @param path path of the instance for which you want to determine the feed
     *             Example: /data/project1/2014/10/10/23/ Path has to be the complete path and can't be a part of it.
     * @return Returns the name of the feed along with the location type(meta/data/stats) and cluster on which the given
     *         path belongs to this feed.
     */
    @GET
    @Path("lookup/{type}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    @Monitored(event = "reverse-lookup")
    public FeedLookupResult reverseLookup(
            @Dimension("type") @PathParam("type") final String type,
            @Dimension("path") @QueryParam("path") final String path) {
        String entity = "DummyEntity"; // A dummy entity name to get around
        return new EntityProxy<FeedLookupResult>(type, entity, FeedLookupResult.class) {
            @Override
            protected Set<String> getColosToApply() {
                return getAllColos();
            }

            @Override
            protected FeedLookupResult doExecute(String colo) throws FalconException {
                return getEntityManager(colo).invoke("reverseLookup", type, path);
            }
        }.execute();

    }
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    private abstract class EntityProxy<T extends APIResult> {
        private final Class<T> clazz;
        private String type;
        private String name;

        public EntityProxy(String type, String name, Class<T> resultClazz) {
            this.clazz = resultClazz;
            this.type = type;
            this.name = name;
        }


        private T getResultInstance(APIResult.Status status, String message) {
            try {
                Constructor<T> constructor = clazz.getConstructor(APIResult.Status.class, String.class);
                return constructor.newInstance(status, message);
            } catch (Exception e) {
                throw new FalconRuntimException("Unable to consolidate result.", e);
            }
        }

        public EntityProxy(String type, String name) {
            this(type, name, (Class<T>) APIResult.class);
        }

        public T execute() {
            Set<String> colos = getColosToApply();

            Map<String, T> results = new HashMap();

            for (String colo : colos) {
                try {
                    results.put(colo, doExecute(colo));
                } catch (FalconWebException e) {
                    String message = ((APIResult) e.getResponse().getEntity()).getMessage();
                    results.put(colo, getResultInstance(APIResult.Status.FAILED, message));
                } catch (Throwable throwable) {
                    results.put(colo, getResultInstance(APIResult.Status.FAILED, throwable.getClass().getName() + "::"
                        + throwable.getMessage()));
                }
            }

            T finalResult = consolidateResult(results, clazz);
            if (finalResult.getStatus() == APIResult.Status.FAILED) {
                throw FalconWebException.newAPIException(finalResult.getMessage());
            } else {
                return finalResult;
            }
        }

        protected Set<String> getColosToApply() {
            return getApplicableColos(type, name);
        }

        protected abstract T doExecute(String colo) throws FalconException;
    }
}
