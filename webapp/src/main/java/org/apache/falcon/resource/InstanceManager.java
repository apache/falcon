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
 * This class provides RESTful API for the lifecycle management of the entity instances.
 */
@Path("instance")
public class InstanceManager extends AbstractInstanceManager {

    @GET
    @Path("running/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "running")
    @Override
    public InstancesResult getRunningInstances(@PathParam("type") String type,
                                               @PathParam("entity") String entity,
                                               @QueryParam("colo") String colo) {
        return super.getRunningInstances(type, entity, colo);
    }

    @GET
    @Path("status/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "instance-status")
    @Override
    public InstancesResult getStatus(@Dimension("type") @PathParam("type") String type,
                                     @Dimension("entity") @PathParam("entity") String entity,
                                     @Dimension("start-time") @QueryParam("start") String startStr,
                                     @Dimension("end-time") @QueryParam("end") String endStr,
                                     @Dimension("colo") @QueryParam("colo") String colo) {
        return super.getStatus(type, entity, startStr, endStr, colo);
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
            @Dimension("run-id") @QueryParam("runid") String runId) {
        return super.getLogs(type, entity, startStr, endStr, colo, runId);
    }


    @POST
    @Path("kill/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "kill-instance")
    @Override
    public InstancesResult killInstance(@Context HttpServletRequest request,
                                        @Dimension("type") @PathParam("type") String type,
                                        @Dimension("entity") @PathParam("entity") String entity,
                                        @Dimension("start-time") @QueryParam("start") String startStr,
                                        @Dimension("end-time") @QueryParam("end") String endStr,
                                        @Dimension("colo") @QueryParam("colo") String colo) {
        return super.killInstance(request, type, entity, startStr, endStr, colo);
    }

    @POST
    @Path("suspend/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "suspend-instance")
    @Override
    public InstancesResult suspendInstance(@Context HttpServletRequest request,
                                           @Dimension("type") @PathParam("type") String type,
                                           @Dimension("entity") @PathParam("entity") String entity,
                                           @Dimension("start-time") @QueryParam("start") String startStr,
                                           @Dimension("end-time") @QueryParam("end") String endStr,
                                           @Dimension("colo") @QueryParam("colo") String colo) {
        return super.suspendInstance(request, type, entity, startStr, endStr, colo);
    }

    @POST
    @Path("resume/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "resume-instance")
    @Override
    public InstancesResult resumeInstance(@Context HttpServletRequest request,
                                          @Dimension("type") @PathParam("type") String type,
                                          @Dimension("entity") @PathParam("entity") String entity,
                                          @Dimension("start-time") @QueryParam("start") String startStr,
                                          @Dimension("end-time") @QueryParam("end") String endStr,
                                          @Dimension("colo") @QueryParam("colo") String colo) {
        return super.resumeInstance(request, type, entity, startStr, endStr, colo);
    }

    @POST
    @Path("rerun/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "re-run-instance")
    @Override
    public InstancesResult reRunInstance(@Dimension("type") @PathParam("type") String type,
                                         @Dimension("entity") @PathParam("entity") String entity,
                                         @Dimension("start-time") @QueryParam("start") String startStr,
                                         @Dimension("end-time") @QueryParam("end") String endStr,
                                         @Context HttpServletRequest request,
                                         @Dimension("colo") @QueryParam("colo") String colo) {
        return super.reRunInstance(type, entity, startStr, endStr, request, colo);
    }
}
