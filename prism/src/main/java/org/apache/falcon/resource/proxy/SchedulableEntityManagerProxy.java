package org.apache.falcon.resource.proxy;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.falcon.FalconException;
import org.apache.falcon.FalconRuntimException;
import org.apache.falcon.FalconWebException;
import org.apache.falcon.entity.EntityNotRegisteredException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.monitors.Dimension;
import org.apache.falcon.monitors.Monitored;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.AbstractSchedulableEntityManager;
import org.apache.falcon.resource.EntityList;
import org.apache.falcon.resource.channel.Channel;
import org.apache.falcon.resource.channel.ChannelFactory;
import org.apache.falcon.util.DeploymentUtil;

@Path("entities")
public class SchedulableEntityManagerProxy extends AbstractSchedulableEntityManager {
    private static final String PRISM_TAG = "prism";

    private final Map<String, Channel> entityManagerChannels = new HashMap<String, Channel>();
    private final Map<String, Channel> configSyncChannels = new HashMap<String, Channel>();
    private boolean embeddedMode = DeploymentUtil.isEmbeddedMode();
    private String currentColo = DeploymentUtil.getCurrentColo();

    public SchedulableEntityManagerProxy() {
        try {
            Set<String> colos = getAllColos();

            for (String colo : colos) {
                initializeFor(colo);
            }
            
            DeploymentUtil.setPrismMode();
        } catch (FalconException e) {
            throw new FalconRuntimException("Unable to initialize channels", e);
        }
    }

    private void initializeFor(String colo) throws FalconException {
        entityManagerChannels.put(colo, ChannelFactory.get("SchedulableEntityManager", colo));
        configSyncChannels.put(colo, ChannelFactory.get("ConfigSyncService", colo));
    }

    private Channel getConfigSyncChannel(String colo) throws FalconException {
        if (!configSyncChannels.containsKey(colo)) {
            initializeFor(colo);
        }
        return configSyncChannels.get(colo);
    }

    private Channel getEntityManager(String colo) throws FalconException {
        if (!entityManagerChannels.containsKey(colo)) {
            initializeFor(colo);
        }
        return entityManagerChannels.get(colo);
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

        final String entity = getEntity(bufferedRequest, type).getName();
        return new EntityProxy(type, entity) {
            @Override
            protected APIResult doExecute(String colo) throws FalconException {
                return getConfigSyncChannel(colo).invoke("submit", bufferedRequest, type, colo);
            }
        }.execute();
    }

    private Entity getEntity(HttpServletRequest request, String type) {
        try {
            request.getInputStream().reset();
            Entity entity = deserializeEntity(request, EntityType.valueOf(type.toUpperCase()));
            request.getInputStream().reset();
            return entity;
        } catch (Exception e) {
            throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);
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
        Map<String, APIResult> results = new HashMap<String, APIResult>();
        
        results.put("falcon", new EntityProxy(type, entity) {
            @Override
            public APIResult execute() {
                try {
                    EntityUtil.getEntity(type, entity);
                    return super.execute();
                } catch (EntityNotRegisteredException e) {
                    return new APIResult(APIResult.Status.SUCCEEDED, entity + "(" + type + ") removed successfully");
                } catch (FalconException e) {
                    throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);
                }
            }
            
            @Override
            protected APIResult doExecute(String colo) throws FalconException {
                return getConfigSyncChannel(colo).invoke("delete", bufferedRequest, type, entity, colo);
            }
        }.execute());

        if (!embeddedMode) {
            results.put(PRISM_TAG,  super.delete(bufferedRequest, type, entity, currentColo));
        }
        return consolidateResult(results);
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
        final Set<String> oldColos = getApplicableColos(type, entityName);
        final Set<String> newColos = getApplicableColos(type, getEntity(bufferedRequest, type));
        final Set<String> mergedColos = new HashSet<String>();
        mergedColos.addAll(oldColos);
        mergedColos.retainAll(newColos);    //Common colos where update should be called
        newColos.removeAll(oldColos);   //New colos where submit should be called
        oldColos.removeAll(mergedColos);   //Old colos where delete should be called

        Map<String, APIResult> results = new HashMap<String, APIResult>();
        if(!oldColos.isEmpty()) {
            results.put("delete", new EntityProxy(type, entityName) {
                @Override
                protected Set<String> getColosToApply() {
                    return oldColos;
                }
    
                @Override
                protected APIResult doExecute(String colo) throws FalconException {
                    return getConfigSyncChannel(colo).invoke("delete", bufferedRequest, type, entityName, colo);
                }
            }.execute());
        }

        if(!mergedColos.isEmpty()) {
            results.put("update", new EntityProxy(type, entityName) {
                @Override
                protected Set<String> getColosToApply() {
                    return mergedColos;
                }
    
                @Override
                protected APIResult doExecute(String colo) throws FalconException {
                    return getConfigSyncChannel(colo).invoke("update", bufferedRequest, type, entityName, colo);
                }
            }.execute());
        }

        if(!newColos.isEmpty()) {
            results.put("submit", new EntityProxy(type, entityName) {
                @Override
                protected Set<String> getColosToApply() {
                    return newColos;
                }
    
                @Override
                protected APIResult doExecute(String colo) throws FalconException {
                    return getConfigSyncChannel(colo).invoke("submit", bufferedRequest, type, colo);
                }
            }.execute());
        }
        
        if (!embeddedMode) {
            results.put(PRISM_TAG, super.update(bufferedRequest, type, entityName, currentColo));
        }
        
        return consolidateResult(results);
    }

    @GET
    @Path("status/{type}/{entity}")
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Monitored(event = "status")
    @Override
    public APIResult getStatus(@Dimension("entityType") @PathParam("type") final String type,
            @Dimension("entityName") @PathParam("entity") final String entity,
            @Dimension("colo") @QueryParam("colo") final String coloExpr) throws FalconWebException {
        return new EntityProxy(type, entity) {
            @Override
            protected Set<String> getColosToApply() {
                return getColosFromExpression(coloExpr, type, entity);
            }

            @Override
            protected APIResult doExecute(String colo) throws FalconException {
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
            protected Set<String> getColosToApply() {
                return getColosFromExpression(coloExpr, type, entity);
            }

            @Override
            protected APIResult doExecute(String colo) throws FalconException {
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
        String entity = getEntity(bufferedRequest, type).getName();
        Map<String, APIResult> results = new HashMap<String, APIResult>();
        results.put("submit", submit(bufferedRequest, type, coloExpr));
        results.put("schedule", schedule(bufferedRequest, type, entity, coloExpr));
        return consolidateResult(results);
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
            protected Set<String> getColosToApply() {
                return getColosFromExpression(coloExpr, type, entity);
            }

            @Override
            protected APIResult doExecute(String colo) throws FalconException {
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
            protected Set<String> getColosToApply() {
                return getColosFromExpression(coloExpr, type, entity);
            }

            @Override
            protected APIResult doExecute(String colo) throws FalconException {
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
            Set<String> colos = getColosToApply();

            Map<String, APIResult> results = new HashMap<String, APIResult>();

            for (String colo : colos) {
                try {
                    results.put(colo, doExecute(colo));
                } catch (FalconException e) {
                    results.put(colo, new APIResult(APIResult.Status.FAILED, e.getClass().getName() + "::" + e.getMessage()));
                }
            }
            APIResult finalResult = consolidateResult(results);
            if (finalResult.getStatus() != APIResult.Status.SUCCEEDED) {
                throw FalconWebException.newException(finalResult, Response.Status.BAD_REQUEST);
            } else {
                return finalResult;
            }
        }

        protected Set<String> getColosToApply() {
            return getApplicableColos(type, name);
        }

        protected abstract APIResult doExecute(String colo) throws FalconException;
    }
    
    private APIResult consolidateResult(Map<String, APIResult> results) {
        if (results == null || results.size() == 0)
            return null;

        StringBuilder buffer = new StringBuilder();
        StringBuilder requestIds = new StringBuilder();
        int statusCount = 0;
        for (Entry<String, APIResult> entry : results.entrySet()) {
            String colo = entry.getKey();
            APIResult result = entry.getValue();
            buffer.append(colo).append('/').append(result.getMessage()).append('\n');
            requestIds.append(colo).append('/').append(result.getRequestId()).append('\n');
            statusCount += result.getStatus().ordinal();
        }

        APIResult.Status status = (statusCount == 0) ? APIResult.Status.SUCCEEDED
                : ((statusCount == results.size() * 2) ? APIResult.Status.FAILED : APIResult.Status.PARTIAL);
        APIResult result = new APIResult(status, buffer.toString());
        result.setRequestId(requestIds.toString());
        return result;
    }
}
