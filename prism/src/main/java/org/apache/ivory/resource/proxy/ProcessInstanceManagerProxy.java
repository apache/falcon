package org.apache.ivory.resource.proxy;

import org.apache.ivory.IvoryException;
import org.apache.ivory.IvoryRuntimException;
import org.apache.ivory.IvoryWebException;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.monitors.Dimension;
import org.apache.ivory.monitors.Monitored;
import org.apache.ivory.resource.APIResult;
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
    public ProcessInstancesResult getRunningInstances(@Dimension("process") @PathParam("process") final String processName,
                                                      @Dimension("colo") @QueryParam("colo") String colo) {
        return new InstanceProxy() {
            @Override
            protected ProcessInstancesResult doExecute(String colo) throws IvoryException {
                return getInstanceManager(colo).
                        invoke("getRunningInstances", processName, colo);
            }
        }.execute(colo, processName);
    }

    @GET
    @Path("status/{process}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event="instance-status")
    @Override
    public ProcessInstancesResult getStatus(@Dimension("process") @PathParam("process") final String processName,
                                            @Dimension("start") @QueryParam("start") final String startStr,
                                            @Dimension("end") @QueryParam("end") final String endStr,
                                            @Dimension("type") @QueryParam("type") final String type,
                                            @Dimension("runid") @QueryParam("runid") final String runId,
                                            @Dimension("colo") @QueryParam("colo") final String colo) {
        return new InstanceProxy() {
            @Override
            protected ProcessInstancesResult doExecute(String colo) throws IvoryException {
                return getInstanceManager(colo).invoke("getStatus",
                        processName, startStr, endStr, type, runId, colo);
            }
        }.execute(colo, processName);
    }

    @POST
    @Path("kill/{process}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event="kill-instance")
    @Override
    public ProcessInstancesResult killProcessInstance(@Context HttpServletRequest request,
                                                      @Dimension("processName") @PathParam("process") final String processName,
                                                      @Dimension("start-time") @QueryParam("start") final String startStr,
                                                      @Dimension("end-time") @QueryParam("end") final String endStr,
                                                      @Dimension("colo") @QueryParam("colo") final String colo) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        return new InstanceProxy() {
            @Override
            protected ProcessInstancesResult doExecute(String colo) throws IvoryException {
                return getInstanceManager(colo).invoke("killProcessInstance",
                    bufferedRequest, processName, startStr, endStr, colo);
            }
        }.execute(colo, processName);
    }

    @POST
    @Path("suspend/{process}")
    @Produces(MediaType.APPLICATION_JSON)
	@Monitored(event="suspend-instance")
    @Override
    public ProcessInstancesResult suspendProcessInstance(@Context HttpServletRequest request,
                                                         @Dimension("processName") @PathParam("process") final String processName,
                                                         @Dimension("start-time") @QueryParam("start") final String startStr,
                                                         @Dimension("end-time") @QueryParam("end") final String endStr,
                                                         @Dimension("colo") @QueryParam("colo") String colo) {
        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        return new InstanceProxy() {
            @Override
            protected ProcessInstancesResult doExecute(String colo) throws IvoryException {
                return getInstanceManager(colo).invoke("suspendProcessInstance",
                    bufferedRequest, processName, startStr, endStr, colo);
            }
        }.execute(colo, processName);
    }

    @POST
    @Path("resume/{process}")
    @Produces(MediaType.APPLICATION_JSON)
	@Monitored(event="resume-instance")
    @Override
    public ProcessInstancesResult resumeProcessInstance(@Context HttpServletRequest request,
                                                        @Dimension("processName") @PathParam("process") final String processName,
                                                        @Dimension("start-time") @QueryParam("start") final String startStr,
                                                        @Dimension("end-time") @QueryParam("end") final String endStr,
                                                        @Dimension("colo") @QueryParam("colo") String colo) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        return new InstanceProxy() {
            @Override
            protected ProcessInstancesResult doExecute(String colo) throws IvoryException {
                return getInstanceManager(colo).invoke("resumeProcessInstance",
                    bufferedRequest, processName, startStr, endStr, colo);
            }
        }.execute(colo, processName);
    }

    @POST
    @Path("rerun/{process}")
    @Produces(MediaType.APPLICATION_JSON)
	@Monitored(event="re-run-instance")
    @Override
    public ProcessInstancesResult reRunInstance(@Dimension("processName") @PathParam("process") final String processName,
                                                @Dimension("start-time") @QueryParam("start") final String startStr,
                                                @Dimension("end-time") @QueryParam("end") final String endStr,
                                                @Context HttpServletRequest request,
                                                @Dimension("colo") @QueryParam("colo") String colo) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        return new InstanceProxy() {
            @Override
            protected ProcessInstancesResult doExecute(String colo) throws IvoryException {
                return getInstanceManager(colo).invoke("reRunInstance",
                    processName, startStr, endStr, bufferedRequest, colo);
            }
        }.execute(colo, processName);
    }

    private abstract class InstanceProxy {

        public ProcessInstancesResult execute(String coloExpr, String name) {
            String[] colos = getColosFromExpression(coloExpr, EntityType.PROCESS.name(), name);

            ProcessInstancesResult[] results = new ProcessInstancesResult[colos.length];
            for (int index = 0; index < colos.length; index++) {
                try {
                    results[index] = doExecute(colos[index]);
                } catch (IvoryException e) {
                    results[index] = new ProcessInstancesResult(APIResult.Status.FAILED,
                            e.getClass().getName() + "::" + e.getMessage(),
                            new ProcessInstancesResult.ProcessInstance[0]);
                }
            }
            ProcessInstancesResult finalResult = consolidateInstanceResult(results, colos);
            if (finalResult.getStatus() != APIResult.Status.SUCCEEDED) {
                throw IvoryWebException.newException(finalResult, Response.Status.BAD_REQUEST);
            } else {
                return finalResult;
            }
        }

        protected abstract ProcessInstancesResult doExecute(String colo) throws IvoryException;
    }
}
