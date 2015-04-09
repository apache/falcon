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

import org.apache.falcon.monitors.Dimension;
import org.apache.falcon.monitors.Monitored;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

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
                               @Dimension("colo") @QueryParam("colo") final String colo) {
        return super.getStatus(type, entity, colo);
    }

    @GET
    @Path("dependencies/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.APPLICATION_JSON})
    @Monitored(event = "dependencies")
    @Override
    public EntityList getDependencies(@Dimension("entityType") @PathParam("type") String type,
                                      @Dimension("entityName") @PathParam("entity") String entity) {
        return super.getDependencies(type, entity);
    }

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    @GET
    @Path("list/{type}")
    @Produces({MediaType.TEXT_XML, MediaType.APPLICATION_JSON})
    @Monitored(event = "list")
    @Override
    public EntityList getEntityList(@Dimension("type") @PathParam("type") String type,
                                    @DefaultValue("") @QueryParam("fields") String fields,
                                    @DefaultValue("") @QueryParam("filterBy") String filterBy,
                                    @DefaultValue("") @QueryParam("tags") String tags,
                                    @DefaultValue("") @QueryParam("orderBy") String orderBy,
                                    @DefaultValue("asc") @QueryParam("sortOrder") String sortOrder,
                                    @DefaultValue("0") @QueryParam("offset") Integer offset,
                                    @DefaultValue(DEFAULT_NUM_RESULTS)
                                    @QueryParam("numResults") Integer resultsPerPage,
                                    @QueryParam("pattern") String pattern) {
        return super.getEntityList(type, fields, filterBy, tags, orderBy, sortOrder, offset, resultsPerPage, pattern);
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
            @DefaultValue("7") @QueryParam("numInstances") Integer numInstanceResults) {
        return super.getEntitySummary(type, cluster, startStr, endStr, fields, entityFilter, entityTags,
                entityOrderBy, entitySortOrder, entityOffset, numEntities, numInstanceResults);
    }
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    @GET
    @Path("definition/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "definition")
    @Override
    public String getEntityDefinition(@Dimension("type") @PathParam("type") String type,
                                      @Dimension("entity") @PathParam("entity") String entityName) {
        return super.getEntityDefinition(type, entityName);
    }

    @POST
    @Path("schedule/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "schedule")
    @Override
    public APIResult schedule(@Context HttpServletRequest request,
                              @Dimension("entityType") @PathParam("type") String type,
                              @Dimension("entityName") @PathParam("entity") String entity,
                              @Dimension("colo") @QueryParam("colo") String colo) {
        return super.schedule(request, type, entity, colo);
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
        return super.suspend(request, type, entity, colo);
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
        return super.resume(request, type, entity, colo);
    }

    @POST
    @Path("validate/{type}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "validate")
    @Override
    public APIResult validate(@Context HttpServletRequest request, @PathParam("type") String type) {
        return super.validate(request, type);
    }

    @POST
    @Path("touch/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Monitored(event = "touch")
    @Override
    public APIResult touch(@Dimension("entityType") @PathParam("type") String type,
                           @Dimension("entityName") @PathParam("entity") String entityName,
                           @Dimension("colo") @QueryParam("colo") String colo) {
        return super.touch(type, entityName, colo);
    }

    @GET
    @Path("lookup/{type}/")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "reverse-lookup")
    public FeedLookupResult reverseLookup(
            @Dimension("type") @PathParam("type") String type,
            @Dimension("path") @QueryParam("path") String instancePath) {
        return super.reverseLookup(type, instancePath);
    }

}
