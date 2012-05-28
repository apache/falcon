package org.apache.ivory.resource.proxy;

import org.apache.ivory.IvoryException;
import org.apache.ivory.IvoryRuntimException;
import org.apache.ivory.IvoryWebException;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.monitors.Dimension;
import org.apache.ivory.monitors.Monitored;
import org.apache.ivory.resource.APIResult;
import org.apache.ivory.resource.APIResult.Status;
import org.apache.ivory.resource.AbstractSchedulableEntityManager;
import org.apache.ivory.resource.EntityList;
import org.apache.ivory.resource.channel.Channel;
import org.apache.ivory.resource.channel.ChannelFactory;
import org.apache.ivory.util.DeploymentUtil;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;

@Path("entities")
public class SchedulableEntityManagerProxy extends AbstractSchedulableEntityManager {

    private final Map<String, Channel> entityManagerChannels = new HashMap<String, Channel>();
    private final Map<String, Channel> configSyncChannels = new HashMap<String, Channel>();
    private boolean embeddedMode = DeploymentUtil.isEmbeddedMode();
    private String currentColo = DeploymentUtil.getCurrentColo();

    public SchedulableEntityManagerProxy() {
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
        entityManagerChannels.put(colo, ChannelFactory.get("SchedulableEntityManager", colo));
        configSyncChannels.put(colo, ChannelFactory.get("ConfigSyncService", colo));
    }

    private Channel getConfigSyncChannel(String colo) throws IvoryException {
        if (!configSyncChannels.containsKey(colo)) {
            initializeFor(colo);
        }
        return configSyncChannels.get(colo);
    }

    private Channel getEntityManager(String colo) throws IvoryException {
        if (!entityManagerChannels.containsKey(colo)) {
            initializeFor(colo);
        }
        return entityManagerChannels.get(colo);
    }

    @Override
    public String getName() {
        return getClass().getName();
    }

    private BufferedRequest getBufferedRequest(HttpServletRequest request) {
        if (request instanceof BufferedRequest)
            return (BufferedRequest) request;
        return new BufferedRequest(request);
    }

    @POST
    @Path("submit/{type}")
    @Consumes({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Monitored(event = "submit")
    @Override
    public APIResult submit(@Context HttpServletRequest request, @Dimension("entityType") @PathParam("type") final String type,
            @Dimension("colo") @QueryParam("colo") final String ignore) {

        final HttpServletRequest bufferedRequest = getBufferedRequest(request);
        if (!embeddedMode) {
            super.submit(bufferedRequest, type, currentColo);
        }

        String entity = getEntityName(bufferedRequest, type);
        return new EntityProxy(type, entity) {
            @Override
            protected String[] getColosToApply() {
                return getAllColos();
            }

            @Override
            protected APIResult doExecute(String colo) throws IvoryException {
                return getConfigSyncChannel(colo).invoke("submit", bufferedRequest, type, colo);
            }
        }.execute();
    }

    private String getEntityName(HttpServletRequest request, String type) {
        try {
            request.getInputStream().reset();
            Entity entity = deserializeEntity(request, EntityType.valueOf(type.toUpperCase()));
            request.getInputStream().reset();
            return entity.getName();
        } catch (Exception e) {
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Path("validate/{type}")
    @Consumes({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Override
    public APIResult validate(@Context HttpServletRequest request, @PathParam("type") String type) {
        return super.validate(request, type);
    }

    @DELETE
    @Path("delete/{type}/{entity}")
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Monitored(event = "delete")
    @Override
    public APIResult delete(@Context HttpServletRequest request, @Dimension("entityType") @PathParam("type") final String type,
            @Dimension("entityName") @PathParam("entity") final String entity, @Dimension("colo") @QueryParam("colo") String ignore) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        final String[] applicableColos = getApplicableColos(type, entity);
        if (!embeddedMode) {
            super.delete(request, type, entity, currentColo);
        }

        return new EntityProxy(type, entity) {
            @Override
            protected String[] getColosToApply() {
                return getAllColos();
            }

            @Override
            protected String[] getColosToConsolidateResult() {
                return applicableColos;
            }

            @Override
            protected APIResult doExecute(String colo) throws IvoryException {
                return getConfigSyncChannel(colo).invoke("delete", bufferedRequest, type, entity, colo);
            }
        }.execute();
    }

    @POST
    @Path("update/{type}/{entity}")
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Monitored(event = "update")
    @Override
    public APIResult update(@Context HttpServletRequest request, @Dimension("entityType") @PathParam("type") final String type,
            @Dimension("entityName") @PathParam("entity") final String entityName,
            @Dimension("colo") @QueryParam("colo") String ignore) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        String[] oldColos = getApplicableColos(type, entityName);
        if (!embeddedMode) {
            super.update(request, type, entityName, currentColo);
        }
        String[] newColos = getApplicableColos(type, entityName);
        final String[] mergedColos = addColos(oldColos, newColos);

        return new EntityProxy(type, entityName) {
            @Override
            protected String[] getColosToApply() {
                return mergedColos;
            }

            @Override
            protected String[] getColosToConsolidateResult() {
                return mergedColos;
            }

            @Override
            protected APIResult doExecute(String colo) throws IvoryException {
                return getConfigSyncChannel(colo).invoke("update", bufferedRequest, type, entityName, colo);
            }
        }.execute();
    }

    private String[] addColos(String[] oldColos, String[] newColos) {
        Set<String> colos = new HashSet<String>();
        if (oldColos != null)
            Collections.addAll(colos, oldColos);
        if (newColos != null)
            Collections.addAll(colos, newColos);
        return colos.toArray(new String[colos.size()]);
    }

    @GET
    @Path("status/{type}/{entity}")
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Monitored(event = "status")
    @Override
    public APIResult getStatus(@Dimension("entityType") @PathParam("type") final String type,
            @Dimension("entityName") @PathParam("entity") final String entity,
            @Dimension("colo") @QueryParam("colo") final String coloExpr) throws IvoryWebException {
        return new EntityProxy(type, entity) {
            @Override
            protected String[] getColosToApply() {
                return getColosFromExpression(coloExpr, type, entity);
            }

            @Override
            protected APIResult doExecute(String colo) throws IvoryException {
                return getEntityManager(colo).invoke("getStatus", type, entity, colo);
            }
        }.execute();
    }

    @GET
    @Path("dependencies/{type}/{entity}")
    @Produces(MediaType.TEXT_XML)
    @Monitored(event = "dependencies")
    @Override
    public EntityList getDependencies(@Dimension("entityType") @PathParam("type") String type,
            @Dimension("entityName") @PathParam("entity") String entity) {
        return super.getDependencies(type, entity);
    }

    @GET
    @Path("list/{type}")
    @Produces(MediaType.TEXT_XML)
    @Override
    public EntityList getDependencies(@PathParam("type") String type) {
        return super.getDependencies(type);
    }

    @GET
    @Path("definition/{type}/{entity}")
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Override
    public String getEntityDefinition(@PathParam("type") String type, @PathParam("entity") String entityName) {
        return super.getEntityDefinition(type, entityName);
    }

    @POST
    @Path("schedule/{type}/{entity}")
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Monitored(event = "schedule")
    @Override
    public APIResult schedule(@Context final HttpServletRequest request,
            @Dimension("entityType") @PathParam("type") final String type,
            @Dimension("entityName") @PathParam("entity") final String entity,
            @Dimension("colo") @QueryParam("colo") final String coloExpr) {

        final HttpServletRequest bufferedRequest = getBufferedRequest(request);
        return new EntityProxy(type, entity) {
            @Override
            protected String[] getColosToApply() {
                return getColosFromExpression(coloExpr, type, entity);
            }

            @Override
            protected APIResult doExecute(String colo) throws IvoryException {
                return getEntityManager(colo).invoke("schedule", bufferedRequest, type, entity, colo);
            }
        }.execute();
    }

    @POST
    @Path("submitAndSchedule/{type}")
    @Consumes({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Monitored(event = "submitAndSchedule")
    @Override
    public APIResult submitAndSchedule(@Context HttpServletRequest request, @Dimension("entityType") @PathParam("type") String type,
            @Dimension("colo") @QueryParam("colo") String coloExpr) {
        BufferedRequest bufferedRequest = new BufferedRequest(request);
        String entity = getEntityName(bufferedRequest, type);

        APIResult submitResult = submit(bufferedRequest, type, coloExpr);
        APIResult schedResult = schedule(bufferedRequest, type, entity, coloExpr);
        return consolidateResult(submitResult, schedResult);
    }

    private APIResult consolidateResult(APIResult result1, APIResult result2) {
        int statusCnt = result1.getStatus().ordinal() + result2.getStatus().ordinal();
        APIResult result = new APIResult(statusCnt == 0 ? Status.SUCCEEDED : (statusCnt == 4 ? Status.FAILED : Status.PARTIAL),
                result1.getMessage() + result2.getMessage());
        result.setRequestId(result1.getRequestId() + result2.getRequestId());
        return result;
    }

    @POST
    @Path("suspend/{type}/{entity}")
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Monitored(event = "suspend")
    @Override
    public APIResult suspend(@Context final HttpServletRequest request,
            @Dimension("entityType") @PathParam("type") final String type,
            @Dimension("entityName") @PathParam("entity") final String entity,
            @Dimension("colo") @QueryParam("colo") final String coloExpr) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        return new EntityProxy(type, entity) {
            @Override
            protected String[] getColosToApply() {
                return getColosFromExpression(coloExpr, type, entity);
            }

            @Override
            protected APIResult doExecute(String colo) throws IvoryException {
                return getEntityManager(colo).invoke("suspend", bufferedRequest, type, entity, colo);
            }
        }.execute();
    }

    @POST
    @Path("resume/{type}/{entity}")
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Monitored(event = "resume")
    @Override
    public APIResult resume(@Context final HttpServletRequest request, @Dimension("entityType") @PathParam("type") final String type,
            @Dimension("entityName") @PathParam("entity") final String entity,
            @Dimension("colo") @QueryParam("colo") final String coloExpr) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        return new EntityProxy(type, entity) {
            @Override
            protected String[] getColosToApply() {
                return getColosFromExpression(coloExpr, type, entity);
            }

            @Override
            protected APIResult doExecute(String colo) throws IvoryException {
                return getEntityManager(colo).invoke("resume", bufferedRequest, type, entity, colo);
            }
        }.execute();
    }

    private abstract class EntityProxy {
        private String type;
        private String name;

        public EntityProxy(String type, String name) {
            this.type = type;
            this.name = name;
        }

        public APIResult execute() {
            String[] colos = getColosToApply();

            Map<String, APIResult> results = new HashMap<String, APIResult>();

            for (String colo : colos) {
                try {
                    results.put(colo, doExecute(colo));
                } catch (IvoryException e) {
                    results.put(colo, new APIResult(APIResult.Status.FAILED, e.getClass().getName() + "::" + e.getMessage()));
                }
            }
            APIResult finalResult = consolidateResult(results, getColosToConsolidateResult());
            if (finalResult.getStatus() != APIResult.Status.SUCCEEDED) {
                throw IvoryWebException.newException(finalResult, Response.Status.BAD_REQUEST);
            } else {
                return finalResult;
            }
        }

        protected String[] getColosToApply() {
            return getApplicableColos(type, name);
        }

        protected String[] getColosToConsolidateResult() {
            return getApplicableColos(type, name);
        }

        protected abstract APIResult doExecute(String colo) throws IvoryException;
    }
}