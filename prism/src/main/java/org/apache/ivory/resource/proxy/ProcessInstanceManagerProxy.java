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
import java.util.HashMap;
import java.util.Map;

@Path("processinstance")
public class ProcessInstanceManagerProxy extends AbstractProcessInstanceManager {

    private final Map<String, Channel> processInstanceManagerChannels = new HashMap<String, Channel>();

    public ProcessInstanceManagerProxy() {
        try {
            String[] colos = getAllColos();

            for (String colo : colos) {
                initializeFor(colo);
            }
        } catch (IvoryException e) {
            throw new IvoryRuntimException("Unable to initialize channels", e);
        }
    }

    private void initializeFor(String colo) throws IvoryException {
        processInstanceManagerChannels.put(colo, ChannelFactory.get("ProcessInstanceManager", colo));
    }

    private Channel getInstanceManager(String colo) throws IvoryException {
        if (!processInstanceManagerChannels.containsKey(colo)) {
            initializeFor(colo);
        }
        return processInstanceManagerChannels.get(colo);
    }

    @Override
    public String getName() {
        return getClass().getName();
    }

    @GET
    @Path("running/{process}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event="running")
    @Override
    public ProcessInstancesResult getRunningInstances(@Dimension("process") @PathParam("process") String processName,
                                                      @Dimension("colo") @QueryParam("colo") String colo) {
        checkColo(colo);
        try {
            String[] colos = getColosToApply(colo);

            ProcessInstancesResult[] results = new ProcessInstancesResult[colos.length];
            for (int index = 0; index < colos.length; index++) {
                results[index] = getInstanceManager(colos[index]).
                        invoke("getRunningInstances", processName, colos[index]);
            }
            return consolidatedResult(results, colos);
        } catch (IvoryException e) {
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    @GET
    @Path("status/{process}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event="instance-status")
    @Override
    public ProcessInstancesResult getStatus(@Dimension("process") @PathParam("process") String processName,
                                            @Dimension("start") @QueryParam("start") String startStr,
                                            @Dimension("end") @QueryParam("end") String endStr,
                                            @Dimension("type") @QueryParam("type") String type,
                                            @Dimension("runid") @QueryParam("runid") String runId,
                                            @Dimension("colo") @QueryParam("colo") String colo) {
        try {
            String[] colos = getColosToApply(colo);

            ProcessInstancesResult[] results = new ProcessInstancesResult[colos.length];
            for (int index = 0; index < colos.length; index++) {
                results[index] = getInstanceManager(colos[index]).invoke("getStatus",
                        processName, startStr, endStr, type, runId, colos[index]);
            }
            return consolidatedResult(results, colos);
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
                                                      @Dimension("end-time") @QueryParam("end") String endStr,
                                                      @Dimension("colo") @QueryParam("colo") String colo) {

        try {
            String[] colos = getColosToApply(colo);

            ProcessInstancesResult[] results = new ProcessInstancesResult[colos.length];
            for (int index = 0; index < colos.length; index++) {
                results[index] = getInstanceManager(colos[index]).invoke("killProcessInstance",
                    request, processName, startStr, endStr, colos[index]);
            }
            return consolidatedResult(results, colos);
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
                                                         @Dimension("end-time") @QueryParam("end") String endStr,
                                                         @Dimension("colo") @QueryParam("colo") String colo) {
        try {
            String[] colos = getColosToApply(colo);

            ProcessInstancesResult[] results = new ProcessInstancesResult[colos.length];
            for (int index = 0; index < colos.length; index++) {
                results[index] = getInstanceManager(colos[index]).invoke("suspendProcessInstance",
                    request, processName, startStr, endStr, colos[index]);
            }
            return consolidatedResult(results, colos);
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
                                                        @Dimension("end-time") @QueryParam("end") String endStr,
                                                        @Dimension("colo") @QueryParam("colo") String colo) {
        try {
            String[] colos = getColosToApply(colo);

            ProcessInstancesResult[] results = new ProcessInstancesResult[colos.length];
            for (int index = 0; index < colos.length; index++) {
                results[index] = getInstanceManager(colos[index]).invoke("resumeProcessInstance",
                    request, processName, startStr, endStr, colos[index]);
            }
            return consolidatedResult(results, colos);
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
                                                @Context HttpServletRequest request,
                                                @Dimension("colo") @QueryParam("colo") String colo) {

        try {
            String[] colos = getColosToApply(colo);

            ProcessInstancesResult[] results = new ProcessInstancesResult[colos.length];
            for (int index = 0; index < colos.length; index++) {
                results[index] = getInstanceManager(colos[index]).invoke("reRunInstance",
                    processName, startStr, endStr, request, colos[index]);
            }
            return consolidatedResult(results, colos);
        } catch (IvoryException e) {
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }
}
