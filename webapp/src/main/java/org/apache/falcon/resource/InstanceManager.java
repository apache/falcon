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

import org.apache.falcon.LifeCycle;
import org.apache.falcon.monitors.Dimension;
import org.apache.falcon.monitors.Monitored;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.List;

/**
 * This class provides RESTful API for the lifecycle management of the entity instances.
 */
@Path("instance")
public class InstanceManager extends AbstractInstanceManager {

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    @GET
    @Path("running/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "running")
    @Override
    public InstancesResult getRunningInstances(
            @Dimension("type") @PathParam("type") String type,
            @Dimension("entity") @PathParam("entity") String entity,
            @Dimension("colo") @QueryParam("colo") String colo,
            @Dimension("lifecycle") @QueryParam("lifecycle") List<LifeCycle> lifeCycles,
            @DefaultValue("") @QueryParam("filterBy") String filterBy,
            @DefaultValue("") @QueryParam("orderBy") String orderBy,
            @DefaultValue("") @QueryParam("sortOrder") String sortOrder,
            @DefaultValue("0") @QueryParam("offset") Integer offset,
            @DefaultValue(DEFAULT_NUM_RESULTS) @QueryParam("numResults") Integer resultsPerPage) {
        return super.getRunningInstances(type, entity, colo, lifeCycles, filterBy,
                orderBy, sortOrder, offset, resultsPerPage);
    }

    /*
       getStatus(...) method actually gets all instances, filtered by a specific status. This is
       a better named API which achieves the same result
     */
    @GET
    @Path("list/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "instance-list")
    @Override
    public InstancesResult getInstances(
            @Dimension("type") @PathParam("type") String type,
            @Dimension("entity") @PathParam("entity") String entity,
            @Dimension("start-time") @QueryParam("start") String startStr,
            @Dimension("end-time") @QueryParam("end") String endStr,
            @Dimension("colo") @QueryParam("colo") String colo,
            @Dimension("lifecycle") @QueryParam("lifecycle") List<LifeCycle> lifeCycles,
            @DefaultValue("") @QueryParam("filterBy") String filterBy,
            @DefaultValue("") @QueryParam("orderBy") String orderBy,
            @DefaultValue("") @QueryParam("sortOrder") String sortOrder,
            @DefaultValue("0") @QueryParam("offset") Integer offset,
            @DefaultValue(DEFAULT_NUM_RESULTS) @QueryParam("numResults") Integer resultsPerPage) {
        return super.getInstances(type, entity, startStr, endStr, colo, lifeCycles,
                filterBy, orderBy, sortOrder, offset, resultsPerPage);
    }

    @GET
    @Path("status/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "instance-status")
    @Override
    public InstancesResult getStatus(
            @Dimension("type") @PathParam("type") String type,
            @Dimension("entity") @PathParam("entity") String entity,
            @Dimension("start-time") @QueryParam("start") String startStr,
            @Dimension("end-time") @QueryParam("end") String endStr,
            @Dimension("colo") @QueryParam("colo") String colo,
            @Dimension("lifecycle") @QueryParam("lifecycle") List<LifeCycle> lifeCycles,
            @DefaultValue("") @QueryParam("filterBy") String filterBy,
            @DefaultValue("") @QueryParam("orderBy") String orderBy,
            @DefaultValue("") @QueryParam("sortOrder") String sortOrder,
            @DefaultValue("0") @QueryParam("offset") Integer offset,
            @DefaultValue(DEFAULT_NUM_RESULTS) @QueryParam("numResults") Integer resultsPerPage) {
        return super.getStatus(type, entity, startStr, endStr, colo, lifeCycles,
                filterBy, orderBy, sortOrder, offset, resultsPerPage);
    }

    @GET
    @Path("summary/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "instance-summary")
    public InstancesSummaryResult getSummary(
            @Dimension("type") @PathParam("type") String type,
            @Dimension("entity") @PathParam("entity") String entity,
            @Dimension("start-time") @QueryParam("start") String startStr,
            @Dimension("end-time") @QueryParam("end") String endStr,
            @Dimension("colo") @QueryParam("colo") String colo,
            @Dimension("lifecycle") @QueryParam("lifecycle") List<LifeCycle> lifeCycles) {
        return super.getSummary(type, entity, startStr, endStr, colo, lifeCycles);
    }

    @GET
    @Path("listing/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "instance-listing")
    @Override
    public FeedInstanceResult getListing(
            @Dimension("type") @PathParam("type") String type,
            @Dimension("entity") @PathParam("entity") String entity,
            @Dimension("start-time") @QueryParam("start") String start,
            @Dimension("end-time") @QueryParam("end") String end,
            @Dimension("colo") @QueryParam("colo") String colo) {
        return super.getListing(type, entity, start, end, colo);
    }

    @GET
    @Path("logs/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "instance-logs")
    @Override
    public InstancesResult getLogs(
            @Dimension("type") @PathParam("type") String type,
            @Dimension("entity") @PathParam("entity") String entity,
            @Dimension("start-time") @QueryParam("start") String startStr,
            @Dimension("end-time") @QueryParam("end") String endStr,
            @Dimension("colo") @QueryParam("colo") String colo,
            @Dimension("run-id") @QueryParam("runid") String runId,
            @Dimension("lifecycle") @QueryParam("lifecycle") List<LifeCycle> lifeCycles,
            @DefaultValue("") @QueryParam("filterBy") String filterBy,
            @DefaultValue("") @QueryParam("orderBy") String orderBy,
            @DefaultValue("") @QueryParam("sortOrder") String sortOrder,
            @DefaultValue("0") @QueryParam("offset") Integer offset,
            @DefaultValue(DEFAULT_NUM_RESULTS) @QueryParam("numResults") Integer resultsPerPage) {
        return super.getLogs(type, entity, startStr, endStr, colo, runId, lifeCycles,
                filterBy, orderBy, sortOrder, offset, resultsPerPage);
    }

    @GET
    @Path("params/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "instance-params")
    @Override
    public InstancesResult getInstanceParams(
            @Dimension("type") @PathParam("type") String type,
            @Dimension("entity") @PathParam("entity") String entity,
            @Dimension("start-time") @QueryParam("start") String start,
            @Dimension("colo") @QueryParam("colo") String colo,
            @Dimension("lifecycle") @QueryParam("lifecycle") List<LifeCycle> lifeCycles) {
        return super.getInstanceParams(type, entity, start, colo, lifeCycles);
    }

    @POST
    @Path("kill/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "kill-instance")
    @Override
    public InstancesResult killInstance(
            @Context HttpServletRequest request,
            @Dimension("type") @PathParam("type") String type,
            @Dimension("entity") @PathParam("entity") String entity,
            @Dimension("start-time") @QueryParam("start") String startStr,
            @Dimension("end-time") @QueryParam("end") String endStr,
            @Dimension("colo") @QueryParam("colo") String colo,
            @Dimension("lifecycle") @QueryParam("lifecycle") List<LifeCycle> lifeCycles) {
        return super.killInstance(request, type, entity, startStr, endStr, colo, lifeCycles);
    }

    @POST
    @Path("suspend/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "suspend-instance")
    @Override
    public InstancesResult suspendInstance(
            @Context HttpServletRequest request,
            @Dimension("type") @PathParam("type") String type,
            @Dimension("entity") @PathParam("entity") String entity,
            @Dimension("start-time") @QueryParam("start") String startStr,
            @Dimension("end-time") @QueryParam("end") String endStr,
            @Dimension("colo") @QueryParam("colo") String colo,
            @Dimension("lifecycle") @QueryParam("lifecycle") List<LifeCycle> lifeCycles) {
        return super.suspendInstance(request, type, entity, startStr, endStr, colo, lifeCycles);
    }

    @POST
    @Path("resume/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "resume-instance")
    @Override
    public InstancesResult resumeInstance(
            @Context HttpServletRequest request,
            @Dimension("type") @PathParam("type") String type,
            @Dimension("entity") @PathParam("entity") String entity,
            @Dimension("start-time") @QueryParam("start") String startStr,
            @Dimension("end-time") @QueryParam("end") String endStr,
            @Dimension("colo") @QueryParam("colo") String colo,
            @Dimension("lifecycle") @QueryParam("lifecycle") List<LifeCycle> lifeCycles) {
        return super.resumeInstance(request, type, entity, startStr, endStr, colo, lifeCycles);
    }

    @POST
    @Path("rerun/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "re-run-instance")
    @Override
    public InstancesResult reRunInstance(
            @Dimension("type") @PathParam("type") String type,
            @Dimension("entity") @PathParam("entity") String entity,
            @Dimension("start-time") @QueryParam("start") String startStr,
            @Dimension("end-time") @QueryParam("end") String endStr,
            @Context HttpServletRequest request,
            @Dimension("colo") @QueryParam("colo") String colo,
            @Dimension("lifecycle") @QueryParam("lifecycle") List<LifeCycle> lifeCycles,
            @Dimension("force") @QueryParam("force") Boolean isForced) {
        return super.reRunInstance(type, entity, startStr, endStr, request, colo, lifeCycles, isForced);
    }
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

}
