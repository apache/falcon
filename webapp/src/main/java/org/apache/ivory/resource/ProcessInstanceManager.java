package org.apache.ivory.resource;

import org.apache.ivory.monitors.Dimension;
import org.apache.ivory.monitors.Monitored;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

@Path("processinstance")
public class ProcessInstanceManager extends AbstractProcessInstanceManager {

    @GET
    @Path("running/{process}")
    @Produces(MediaType.APPLICATION_JSON)
    @Override
    public ProcessInstancesResult getRunningInstances(@PathParam("process") String processName) {
        return super.getRunningInstances(processName);
    }

    @GET
    @Path("status/{process}")
    @Produces(MediaType.APPLICATION_JSON)
    @Override
    public ProcessInstancesResult getStatus(@PathParam("process") String processName,
                                            @QueryParam("start") String startStr,
                                            @QueryParam("end") String endStr,
                                            @QueryParam("type") String type,
                                            @QueryParam("runid") String runId) {
        return super.getStatus(processName, startStr, endStr, type, runId);
    }

    @POST
    @Path("kill/{process}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event="kill-instance")
    @Override
    public ProcessInstancesResult killProcessInstance(@Context HttpServletRequest request,
                                                      @Dimension("processName") @PathParam("process") String processName,
                                                      @Dimension("start-time") @QueryParam("start") String startStr,
                                                      @Dimension("end-time") @QueryParam("end") String endStr) {
        return super.killProcessInstance(request, processName, startStr, endStr);
    }

    @POST
    @Path("suspend/{process}")
    @Produces(MediaType.APPLICATION_JSON)
	@Monitored(event="suspend-instance")
    @Override
    public ProcessInstancesResult suspendProcessInstance(@Context HttpServletRequest request,
                                                         @Dimension("processName") @PathParam("process") String processName,
                                                         @Dimension("start-time") @QueryParam("start") String startStr,
                                                         @Dimension("end-time") @QueryParam("end") String endStr) {
        return super.suspendProcessInstance(request, processName, startStr, endStr);
    }

    @POST
    @Path("resume/{process}")
    @Produces(MediaType.APPLICATION_JSON)
	@Monitored(event="resume-instance")
    @Override
    public ProcessInstancesResult resumeProcessInstance(@Context HttpServletRequest request,
                                                        @Dimension("processName") @PathParam("process") String processName,
                                                        @Dimension("start-time") @QueryParam("start") String startStr,
                                                        @Dimension("end-time") @QueryParam("end") String endStr) {
        return super.resumeProcessInstance(request, processName, startStr, endStr);
    }

    @POST
    @Path("rerun/{process}")
    @Produces(MediaType.APPLICATION_JSON)
	@Monitored(event="re-run-instance")
    @Override
    public ProcessInstancesResult reRunInstance(@Dimension("processName") @PathParam("process") String processName,
                                                @Dimension("start-time") @QueryParam("start") String startStr,
                                                @Dimension("end-time") @QueryParam("end") String endStr,
                                                @Context HttpServletRequest request) {
        return super.reRunInstance(processName, startStr, endStr, request);
    }
}
