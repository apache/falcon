package org.apache.ivory.resource;

import org.apache.ivory.monitors.Dimension;
import org.apache.ivory.monitors.Monitored;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

@Path("instance")
public class InstanceManager extends AbstractInstanceManager {

    @Override
    public String getName() {
        return getClass().getName();
    }

    @GET
    @Path("running/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event="running")
    @Override
    public InstancesResult getRunningInstances(@PathParam("entityType") String type,
                                                      @PathParam("entityName") String entity,
                                                      @QueryParam("colo") String colo) {
        return super.getRunningInstances(type, entity, colo);
    }

    @GET
    @Path("status/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event="instance-status")
    @Override
    public InstancesResult getStatus(@Dimension("type") @PathParam("entityType") String type,
                                            @Dimension("entity") @PathParam("entityName") String entity,
                                            @Dimension("start-time") @QueryParam("start") String startStr,
                                            @Dimension("end-time") @QueryParam("end") String endStr,
                                            @Dimension("runid") @QueryParam("runid") String runId,
                                            @Dimension("colo") @QueryParam("colo") String colo) {
        return super.getStatus(type, entity, startStr, endStr, runId, colo);
    }

    @POST
    @Path("kill/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event="kill-instance")
    @Override
    public InstancesResult killInstance(@Context HttpServletRequest request,
                                        @Dimension("type") @PathParam("entityType") String type,
                                        @Dimension("entity") @PathParam("entityName") String entity,
                                        @Dimension("start-time") @QueryParam("start") String startStr,
                                        @Dimension("end-time") @QueryParam("end") String endStr,
                                        @Dimension("colo") @QueryParam("colo") String colo) {
        return super.killInstance(request, type, entity, startStr, endStr, colo);
    }

    @POST
    @Path("suspend/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
	@Monitored(event="suspend-instance")
    @Override
    public InstancesResult suspendInstance(@Context HttpServletRequest request,
                                           @Dimension("type") @PathParam("entityType") String type,
                                           @Dimension("entity") @PathParam("entityName") String entity,
                                           @Dimension("start-time") @QueryParam("start") String startStr,
                                           @Dimension("end-time") @QueryParam("end") String endStr,
                                           @Dimension("colo") @QueryParam("colo") String colo) {
        return super.suspendInstance(request, type, entity, startStr, endStr, colo);
    }

    @POST
    @Path("resume/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
	@Monitored(event="resume-instance")
    @Override
    public InstancesResult resumeInstance(@Context HttpServletRequest request,
                                          @Dimension("type") @PathParam("entityType") String type,
                                          @Dimension("entity") @PathParam("entityName") String entity,
                                          @Dimension("start-time") @QueryParam("start") String startStr,
                                          @Dimension("end-time") @QueryParam("end") String endStr,
                                          @Dimension("colo") @QueryParam("colo") String colo) {
        return super.resumeInstance(request, type, entity, startStr, endStr, colo);
    }

    @POST
    @Path("rerun/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
	@Monitored(event="re-run-instance")
    @Override
    public InstancesResult reRunInstance(@Dimension("type") @PathParam("entityType") String type,
                                                @Dimension("entity") @PathParam("entityName") String entity,
                                                @Dimension("start-time") @QueryParam("start") String startStr,
                                                @Dimension("end-time") @QueryParam("end") String endStr,
                                                @Context HttpServletRequest request,
                                                @Dimension("colo") @QueryParam("colo") String colo) {
        return super.reRunInstance(type, entity, startStr, endStr, request, colo);
    }
}
