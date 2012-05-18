package org.apache.ivory.resource;

import org.apache.ivory.monitors.Dimension;
import org.apache.ivory.monitors.Monitored;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

@Path("processinstance")
public class ProcessInstanceManager extends AbstractProcessInstanceManager {

    @Override
    public String getName() {
        return getClass().getName();
    }

    @GET
    @Path("running/{process}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event="running")
    @Override
    public ProcessInstancesResult getRunningInstances(@PathParam("process") String processName,
                                                      @QueryParam("colo") String colo) {
        return super.getRunningInstances(processName, colo);
    }

    @GET
    @Path("status/{process}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event="instance-status")
    @Override
    public ProcessInstancesResult getStatus(@Dimension("process") @PathParam("process") String processName,
                                            @Dimension("process") @QueryParam("start") String startStr,
                                            @Dimension("process") @QueryParam("end") String endStr,
                                            @Dimension("process") @QueryParam("type") String type,
                                            @Dimension("process") @QueryParam("runid") String runId,
                                            @Dimension("colo") @QueryParam("colo") String colo) {
        return super.getStatus(processName, startStr, endStr, type, runId, colo);
    }

    @POST
    @Path("kill/{process}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event="kill-instance")
    @Override
    public ProcessInstancesResult killProcessInstance(@Context HttpServletRequest request,
                                                      @Dimension("processName") @PathParam("process") String processName,
                                                      @Dimension("start-time") @QueryParam("start") String startStr,
                                                      @Dimension("end-time") @QueryParam("end") String endStr,
                                                      @Dimension("colo") @QueryParam("colo") String colo) {
        return super.killProcessInstance(request, processName, startStr, endStr, colo);
    }

    @POST
    @Path("suspend/{process}")
    @Produces(MediaType.APPLICATION_JSON)
	@Monitored(event="suspend-instance")
    @Override
    public ProcessInstancesResult suspendProcessInstance(@Context HttpServletRequest request,
                                                         @Dimension("processName") @PathParam("process") String processName,
                                                         @Dimension("start-time") @QueryParam("start") String startStr,
                                                         @Dimension("end-time") @QueryParam("end") String endStr,
                                                         @Dimension("colo") @QueryParam("colo") String colo) {
        return super.suspendProcessInstance(request, processName, startStr, endStr, colo);
    }

    @POST
    @Path("resume/{process}")
    @Produces(MediaType.APPLICATION_JSON)
	@Monitored(event="resume-instance")
    @Override
    public ProcessInstancesResult resumeProcessInstance(@Context HttpServletRequest request,
                                                        @Dimension("processName") @PathParam("process") String processName,
                                                        @Dimension("start-time") @QueryParam("start") String startStr,
                                                        @Dimension("end-time") @QueryParam("end") String endStr,
                                                        @Dimension("colo") @QueryParam("colo") String colo) {
        return super.resumeProcessInstance(request, processName, startStr, endStr, colo);
    }

    @POST
    @Path("rerun/{process}")
    @Produces(MediaType.APPLICATION_JSON)
	@Monitored(event="re-run-instance")
    @Override
    public ProcessInstancesResult reRunInstance(@Dimension("processName") @PathParam("process") String processName,
                                                @Dimension("start-time") @QueryParam("start") String startStr,
                                                @Dimension("end-time") @QueryParam("end") String endStr,
                                                @Context HttpServletRequest request,
                                                @Dimension("colo") @QueryParam("colo") String colo) {
        return super.reRunInstance(processName, startStr, endStr, request, colo);
    }
}
