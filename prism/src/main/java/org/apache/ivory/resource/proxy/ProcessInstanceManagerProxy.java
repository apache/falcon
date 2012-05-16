package org.apache.ivory.resource.proxy;

import org.apache.ivory.IvoryException;
import org.apache.ivory.IvoryRuntimException;
import org.apache.ivory.IvoryWebException;
import org.apache.ivory.monitors.Dimension;
import org.apache.ivory.monitors.Monitored;
import org.apache.ivory.resource.AbstractProcessInstanceManager;
import org.apache.ivory.resource.ProcessInstancesResult;
import org.apache.ivory.resource.channel.Channel;
import org.apache.ivory.resource.channel.ChannelFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("processinstance")
public class ProcessInstanceManagerProxy extends AbstractProcessInstanceManager {

    private final Channel processInstanceManagerChannel;

    public ProcessInstanceManagerProxy() {
        try {
            processInstanceManagerChannel = ChannelFactory.get("ProcessInstanceManager");
        } catch (IvoryException e) {
            throw new IvoryRuntimException("Unable to initialize channel", e);
        }
    }

    @Override
    public String getName() {
        return getClass().getName();
    }

    @GET
    @Path("running/{process}")
    @Produces(MediaType.APPLICATION_JSON)
    @Override
    public ProcessInstancesResult getRunningInstances(@PathParam("process") String processName) {
        try {
            return processInstanceManagerChannel.invoke("getRunningInstances", processName);
        } catch (IvoryException e) {
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
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
        try {
            return processInstanceManagerChannel.invoke("getStatus", processName,
                    startStr, endStr, type, runId);
        } catch (IvoryException e) {
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
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
        try {
            return processInstanceManagerChannel.invoke("killProcessInstance",
                    request, processName, startStr, endStr);
        } catch (IvoryException e) {
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
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
        try {
            return processInstanceManagerChannel.invoke("suspendProcessInstance",
                    request, processName, startStr, endStr);
        } catch (IvoryException e) {
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
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
        try {
            return processInstanceManagerChannel.invoke("resumeProcessInstance",
                    request, processName, startStr, endStr);
        } catch (IvoryException e) {
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
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
        try {
            return processInstanceManagerChannel.invoke("reRunInstance",
                    processName, startStr, endStr, request);
        } catch (IvoryException e) {
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }
}
