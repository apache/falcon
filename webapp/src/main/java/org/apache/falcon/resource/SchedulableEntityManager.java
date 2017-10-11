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

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconWebException;
import org.apache.falcon.monitors.Dimension;
import org.apache.falcon.monitors.Monitored;

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
import javax.ws.rs.core.Response;

/**
 * Entity management operations as REST API for feed and process.
 */
@Path("entities")
public class SchedulableEntityManager extends AbstractSchedulableEntityManager {

    @GET
    @Path("status/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "status")
    @Override
    public APIResult getStatus(@Dimension("entityType") @PathParam("type") String type,
                               @Dimension("entityName") @PathParam("entity") String entity,
                               @Dimension("colo") @QueryParam("colo") final String colo,
                               @Dimension("showScheduler") @QueryParam("showScheduler") final Boolean showScheduler) {
        try {
            return super.getStatus(type, entity, colo, showScheduler);
        } catch (Throwable throwable) {
            throw FalconWebException.newAPIException(throwable);
        }
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
        throw FalconWebException.newAPIException("delete on server is not"
                + " supported.Please run your operation on Prism.", Response.Status.FORBIDDEN);
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
        throw FalconWebException.newAPIException("update on server is not"
                + " supported.Please run your operation on Prism.", Response.Status.FORBIDDEN);
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
        throw FalconWebException.newAPIException("update on server is not"
                + " supported.Please run your operation on Prism.", Response.Status.FORBIDDEN);
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
        throw FalconWebException.newAPIException("submitAndSchedule on server is not"
                + " supported.Please run your operation on Prism.", Response.Status.FORBIDDEN);
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
        throw FalconWebException.newAPIException("submit on server is not"
                + " supported.Please run your operation on Prism.", Response.Status.FORBIDDEN);
    }

    @GET
    @Path("sla-alert/{type}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_XML})
    @Monitored(event = "entity-sla-misses")
    public SchedulableEntityInstanceResult getEntitySLAMissPendingAlerts(
            @Dimension("entityType") @PathParam("type") String entityType,
            @Dimension("entityName") @QueryParam("name") String entityName,
            @Dimension("start") @QueryParam("start") String start,
            @Dimension("end") @QueryParam("end") String end,
            @Dimension("colo") @QueryParam("colo") final String colo) {
        try {
            validateSlaParams(entityType, entityName, start, end, colo);
            return super.getEntitySLAMissPendingAlerts(entityName, entityType, start, end, colo);
        } catch (Throwable e) {
            throw FalconWebException.newAPIException(e);
        }
    }

    @GET
    @Path("dependencies/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.APPLICATION_JSON})
    @Monitored(event = "dependencies")
    @Override
    public EntityList getDependencies(@Dimension("entityType") @PathParam("type") String type,
                                      @Dimension("entityName") @PathParam("entity") String entity) {
        try {
            return super.getDependencies(type, entity);
        } catch (Throwable throwable) {
            throw FalconWebException.newAPIException(throwable);
        }
    }

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    @GET
    @Path("list{type : (/[^/]+)?}")
    @Produces({MediaType.TEXT_XML, MediaType.APPLICATION_JSON})
    @Monitored(event = "list")
    @Override
    public EntityList getEntityList(@Dimension("type") @PathParam("type") String type,
                                    @DefaultValue("") @QueryParam("fields") String fields,
                                    @DefaultValue("") @QueryParam("nameseq") String nameSubsequence,
                                    @DefaultValue("") @QueryParam("tagkeys") String tagKeywords,
                                    @DefaultValue("") @QueryParam("tags") String tags,
                                    @DefaultValue("") @QueryParam("filterBy") String filterBy,
                                    @DefaultValue("") @QueryParam("orderBy") String orderBy,
                                    @DefaultValue("asc") @QueryParam("sortOrder") String sortOrder,
                                    @DefaultValue("0") @QueryParam("offset") Integer offset,
                                    @QueryParam("numResults") Integer resultsPerPage,
                                    @DefaultValue("") @QueryParam("doAs") String doAsUser) {
        try {
            if (StringUtils.isNotEmpty(type)) {
                type = type.substring(1);
            }
            resultsPerPage = resultsPerPage == null ? getDefaultResultsPerPage() : resultsPerPage;
            return super.getEntityList(fields, nameSubsequence, tagKeywords, type, tags, filterBy,
                orderBy, sortOrder, offset, resultsPerPage, doAsUser);
        } catch (Throwable throwable) {
            throw FalconWebException.newAPIException(throwable);
        }
    }

    @GET
    @Path("summary/{type}")
    @Produces({MediaType.TEXT_XML, MediaType.APPLICATION_JSON})
    @Monitored(event = "summary")
    @Override
    public EntitySummaryResult getEntitySummary(
            @Dimension("type") @PathParam("type") String type,
            @Dimension("cluster") @QueryParam("cluster") String cluster,
            @DefaultValue("") @QueryParam("start") String startStr,
            @DefaultValue("") @QueryParam("end") String endStr,
            @DefaultValue("") @QueryParam("fields") String fields,
            @DefaultValue("") @QueryParam("filterBy") String entityFilter,
            @DefaultValue("") @QueryParam("tags")  String entityTags,
            @DefaultValue("") @QueryParam("orderBy") String entityOrderBy,
            @DefaultValue("asc") @QueryParam("sortOrder") String entitySortOrder,
            @DefaultValue("0") @QueryParam("offset") Integer entityOffset,
            @DefaultValue("10") @QueryParam("numResults") Integer numEntities,
            @DefaultValue("7") @QueryParam("numInstances") Integer numInstanceResults,
            @DefaultValue("") @QueryParam("doAs") final String doAsUser) {
        try {
            return super.getEntitySummary(type, cluster, startStr, endStr, fields, entityFilter, entityTags,
                entityOrderBy, entitySortOrder, entityOffset, numEntities, numInstanceResults, doAsUser);
        } catch (Throwable throwable) {
            throw FalconWebException.newAPIException(throwable);
        }
    }
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    @GET
    @Path("definition/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "definition")
    @Override
    public String getEntityDefinition(@Dimension("type") @PathParam("type") String type,
                                      @Dimension("entity") @PathParam("entity") String entityName) {
        try {
            return super.getEntityDefinition(type, entityName);
        } catch (Throwable throwable) {
            throw FalconWebException.newAPIException(throwable);
        }
    }

    @POST
    @Path("schedule/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "schedule")
    @Override
    public APIResult schedule(@Context HttpServletRequest request,
                              @Dimension("entityType") @PathParam("type") String type,
                              @Dimension("entityName") @PathParam("entity") String entity,
                              @Dimension("colo") @QueryParam("colo") String colo,
                              @QueryParam("skipDryRun") Boolean skipDryRun,
                              @QueryParam("properties") String properties) {
        try {
            return super.schedule(request, type, entity, colo, skipDryRun, properties);
        } catch (Throwable throwable) {
            throw FalconWebException.newAPIException(throwable);
        }
    }

    @POST
    @Path("suspend/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "suspend")
    @Override
    public APIResult suspend(@Context HttpServletRequest request,
                             @Dimension("entityType") @PathParam("type") String type,
                             @Dimension("entityName") @PathParam("entity") String entity,
                             @Dimension("colo") @QueryParam("colo") String colo) {
        try {
            return super.suspend(request, type, entity, colo);
        } catch (Throwable throwable) {
            throw FalconWebException.newAPIException(throwable);
        }
    }

    @POST
    @Path("resume/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "resume")
    @Override
    public APIResult resume(@Context HttpServletRequest request,
                            @Dimension("entityType") @PathParam("type") String type,
                            @Dimension("entityName") @PathParam("entity") String entity,
                            @Dimension("colo") @QueryParam("colo") String colo) {
        try {
            return super.resume(request, type, entity, colo);
        } catch (Throwable throwable) {
            throw FalconWebException.newAPIException(throwable);
        }
    }

    @POST
    @Path("validate/{type}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "validate")
    @Override
    public APIResult validate(@Context HttpServletRequest request, @PathParam("type") String type,
                              @QueryParam("skipDryRun") Boolean skipDryRun) {
        try {
            return super.validate(request, type, skipDryRun);
        } catch (Throwable throwable) {
            throw FalconWebException.newAPIException(throwable);
        }
    }

    @POST
    @Path("touch/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Monitored(event = "touch")
    @Override
    public APIResult touch(@Dimension("entityType") @PathParam("type") String type,
                           @Dimension("entityName") @PathParam("entity") String entityName,
                           @Dimension("colo") @QueryParam("colo") String colo,
                           @QueryParam("skipDryRun") Boolean skipDryRun) {
        try {
            return super.touch(type, entityName, colo, skipDryRun);
        } catch (Throwable throwable) {
            throw FalconWebException.newAPIException(throwable);
        }
    }

    @GET
    @Path("lookup/{type}/")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "reverse-lookup")
    public FeedLookupResult reverseLookup(
            @Dimension("type") @PathParam("type") String type,
            @Dimension("path") @QueryParam("path") String instancePath) {
        try {
            return super.reverseLookup(type, instancePath);
        } catch (Throwable throwable) {
            throw FalconWebException.newAPIException(throwable);
        }
    }
}
