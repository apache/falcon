package org.apache.ivory.resource.proxy;

import org.apache.ivory.IvoryException;
import org.apache.ivory.IvoryRuntimException;
import org.apache.ivory.IvoryWebException;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.monitors.Dimension;
import org.apache.ivory.monitors.Monitored;
import org.apache.ivory.resource.APIResult;
import org.apache.ivory.resource.AbstractInstanceManager;
import org.apache.ivory.resource.InstancesResult;
import org.apache.ivory.resource.channel.Channel;
import org.apache.ivory.resource.channel.ChannelFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

@Path("instance")
public class InstanceManagerProxy extends AbstractInstanceManager {

    private final Map<String, Channel> processInstanceManagerChannels = new HashMap<String, Channel>();

    public InstanceManagerProxy() {
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
    @Path("running/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event="running")
    @Override
    public InstancesResult getRunningInstances(@Dimension("entityType") @PathParam("type") final String type,
                                                      @Dimension("entityName") @PathParam("entity") final String entity,
                                                      @Dimension("colo") @QueryParam("colo") String colo) {
        return new InstanceProxy() {
            @Override
            protected InstancesResult doExecute(String colo) throws IvoryException {
                return getInstanceManager(colo).
                        invoke("getRunningInstances", type, entity, colo);
            }
        }.execute(colo, entity);
    }

    @GET
    @Path("status/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event="instance-status")
    @Override
    public InstancesResult getStatus(@Dimension("entityType") @PathParam("type") final String type,
                                            @Dimension("entityName") @PathParam("entity") final String entity,
                                            @Dimension("start-time") @QueryParam("start") final String startStr,
                                            @Dimension("end-time") @QueryParam("end") final String endStr,
                                            @Dimension("runid") @QueryParam("runid") final String runId,
                                            @Dimension("colo") @QueryParam("colo") final String colo) {
        return new InstanceProxy() {
            @Override
            protected InstancesResult doExecute(String colo) throws IvoryException {
                return getInstanceManager(colo).invoke("getStatus",
                        type, entity, startStr, endStr, runId, colo);
            }
        }.execute(colo, entity);
    }

    @POST
    @Path("kill/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event="kill-instance")
    @Override
    public InstancesResult killInstance(@Context HttpServletRequest request,
                                        @Dimension("entityType") @PathParam("type") final String type,
                                        @Dimension("entityName") @PathParam("entity") final String entity,
                                        @Dimension("start-time") @QueryParam("start") final String startStr,
                                        @Dimension("end-time") @QueryParam("end") final String endStr,
                                        @Dimension("colo") @QueryParam("colo") final String colo) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        return new InstanceProxy() {
            @Override
            protected InstancesResult doExecute(String colo) throws IvoryException {
                return getInstanceManager(colo).invoke("killInstance",
                    bufferedRequest, type, entity, startStr, endStr, colo);
            }
        }.execute(colo, entity);
    }

    @POST
    @Path("suspend/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
	@Monitored(event="suspend-instance")
    @Override
    public InstancesResult suspendInstance(@Context HttpServletRequest request,
                                           @Dimension("entityType") @PathParam("type") final String type,
                                           @Dimension("entityName") @PathParam("entity") final String entity,
                                           @Dimension("start-time") @QueryParam("start") final String startStr,
                                           @Dimension("end-time") @QueryParam("end") final String endStr,
                                           @Dimension("colo") @QueryParam("colo") String colo) {
        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        return new InstanceProxy() {
            @Override
            protected InstancesResult doExecute(String colo) throws IvoryException {
                return getInstanceManager(colo).invoke("suspendInstance",
                    bufferedRequest, type, entity, startStr, endStr, colo);
            }
        }.execute(colo, entity);
    }

    @POST
    @Path("resume/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
	@Monitored(event="resume-instance")
    @Override
    public InstancesResult resumeInstance(@Context HttpServletRequest request,
                                          @Dimension("entityType") @PathParam("type") final String type,
                                          @Dimension("entityName") @PathParam("entity") final String entity,
                                          @Dimension("start-time") @QueryParam("start") final String startStr,
                                          @Dimension("end-time") @QueryParam("end") final String endStr,
                                          @Dimension("colo") @QueryParam("colo") String colo) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        return new InstanceProxy() {
            @Override
            protected InstancesResult doExecute(String colo) throws IvoryException {
                return getInstanceManager(colo).invoke("resumeInstance",
                    bufferedRequest, type, entity, startStr, endStr, colo);
            }
        }.execute(colo, entity);
    }

    @POST
    @Path("rerun/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
	@Monitored(event="re-run-instance")
    @Override
    public InstancesResult reRunInstance(@Dimension("entityType") @PathParam("type") final String type,
                                                @Dimension("entityName") @PathParam("entity") final String entity,
                                                @Dimension("start-time") @QueryParam("start") final String startStr,
                                                @Dimension("end-time") @QueryParam("end") final String endStr,
                                                @Context HttpServletRequest request,
                                                @Dimension("colo") @QueryParam("colo") String colo) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        return new InstanceProxy() {
            @Override
            protected InstancesResult doExecute(String colo) throws IvoryException {
                return getInstanceManager(colo).invoke("reRunInstance",
                    type, entity, startStr, endStr, bufferedRequest, colo);
            }
        }.execute(colo, entity);
    }

    private abstract class InstanceProxy {

        public InstancesResult execute(String coloExpr, String name) {
            String[] colos = getColosFromExpression(coloExpr, EntityType.PROCESS.name(), name);

            InstancesResult[] results = new InstancesResult[colos.length];
            for (int index = 0; index < colos.length; index++) {
                try {
                    results[index] = doExecute(colos[index]);
                } catch (IvoryException e) {
                    results[index] = new InstancesResult(APIResult.Status.FAILED,
                            e.getClass().getName() + "::" + e.getMessage(),
                            new InstancesResult.Instance[0]);
                }
            }
            InstancesResult finalResult = consolidateInstanceResult(results, colos);
            if (finalResult.getStatus() != APIResult.Status.SUCCEEDED) {
                throw IvoryWebException.newException(finalResult, Response.Status.BAD_REQUEST);
            } else {
                return finalResult;
            }
        }

        protected abstract InstancesResult doExecute(String colo) throws IvoryException;
    }
}
