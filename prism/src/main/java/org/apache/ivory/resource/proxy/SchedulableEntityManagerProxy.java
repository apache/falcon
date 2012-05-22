package org.apache.ivory.resource.proxy;

import org.apache.ivory.IvoryException;
import org.apache.ivory.IvoryRuntimException;
import org.apache.ivory.IvoryWebException;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.monitors.Dimension;
import org.apache.ivory.monitors.Monitored;
import org.apache.ivory.resource.APIResult;
import org.apache.ivory.resource.AbstractSchedulableEntityManager;
import org.apache.ivory.resource.EntityList;
import org.apache.ivory.resource.channel.Channel;
import org.apache.ivory.resource.channel.ChannelFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

@Path("entities")
public class SchedulableEntityManagerProxy extends AbstractSchedulableEntityManager {

    private final Map<String, Channel> entityManagerChannels = new HashMap<String, Channel>();
    private final Map<String, Channel> configSyncChannels = new HashMap<String, Channel>();

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

    @POST
    @Path("submit/{type}")
    @Consumes({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Monitored(event = "submit")
    @Override
    public APIResult submit(@Context HttpServletRequest request,
                            @Dimension("entityType") @PathParam("type") final String type,
                            @Dimension("colo") @QueryParam("colo") final String colo) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        if (!integratedMode) {
            super.submit(bufferedRequest, type, currentColo);
        }
        return new EntityProxy() {
            @Override
            protected APIResult doExecute(String colo) throws IvoryException {
                return getConfigSyncChannel(colo).
                        invoke("submit", bufferedRequest, type, colo);
            }
        }.execute();
    }

    @POST
    @Path("validate/{type}")
    @Consumes({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Override
    public APIResult validate(@Context HttpServletRequest request,
                              @PathParam("type") String type) {
        return super.validate(request, type);
    }

    @DELETE
    @Path("delete/{type}/{entity}")
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Monitored(event = "delete")
    @Override
    public APIResult delete(@Context HttpServletRequest request,
                            @Dimension("entityType") @PathParam("type") final String type,
                            @Dimension("entityName") @PathParam("entity") final String entity,
                            @Dimension("colo") @QueryParam("colo") String ignore) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        if (!integratedMode) {
            super.delete(request, type, entity, currentColo);
        }
        return new EntityProxy() {
            @Override
            protected APIResult doExecute(String colo) throws IvoryException {
                return getConfigSyncChannel(colo).
                    invoke("delete", bufferedRequest, type, entity, colo);
            }
        }.execute();
    }

    @POST
    @Path("update/{type}/{entity}")
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Monitored(event = "update")
    @Override
    public APIResult update(@Context HttpServletRequest request,
                            @Dimension("entityType") @PathParam("type") final String type,
                            @Dimension("entityName") @PathParam("entity") final String entityName,
                            @Dimension("colo") @QueryParam("colo") String ignore) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        if (!integratedMode) {
            super.update(request, type, entityName, currentColo);
        }
        return new EntityProxy() {
            @Override
            protected APIResult doExecute(String colo) throws IvoryException {
                return getConfigSyncChannel(colo).
                    invoke("update", bufferedRequest, type, entityName, colo);
            }
        }.execute();
    }

    @GET
    @Path("status/{type}/{entity}")
    @Produces(MediaType.TEXT_PLAIN)
    @Monitored(event = "status")
    @Override
    public String getStatus(@Dimension("entityType") @PathParam("type") String type,
                            @Dimension("entityName") @PathParam("entity") String entity)
            throws IvoryWebException {
        return super.getStatus(type, entity);
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
    public String getEntityDefinition(@PathParam("type") String type,
                                      @PathParam("entity") String entityName) {
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
                              @Dimension("colo") @QueryParam("colo") final String colo) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        return new EntityProxy() {
            @Override
            protected String[] getColos() {
                return getColosToApply(colo);
            }

            @Override
            protected APIResult doExecute(String colo) throws IvoryException {
                return getEntityManager(colo).
                        invoke("schedule", bufferedRequest, type, entity, colo);
            }
        }.execute();
    }

    @POST
     @Path("submitAndSchedule/{type}")
     @Consumes({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
     @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
     @Monitored(event = "submitAndSchedule")
    @Override
    public APIResult submitAndSchedule(@Context HttpServletRequest request,
                                       @Dimension("entityType") @PathParam("type") String type,
                                       @Dimension("colo") @QueryParam("colo") String colo) {
         try {
             if (!integratedMode) {
                 Entity entity = submitInternal(request, type);
                 return schedule(request, type, entity.getName(), colo);
             } else {
                 return getEntityManager(DEFAULT_COLO).
                         invoke("submitAndSchedule", request, type, DEFAULT_COLO);
             }
         } catch (Exception e) {
             throw IvoryWebException.newException(
                     new APIResult(APIResult.Status.FAILED, e.getMessage()),
                     Response.Status.BAD_REQUEST);
         }
    }

    @POST
    @Path("suspend/{type}/{entity}")
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Monitored(event = "suspend")
    @Override
    public APIResult suspend(@Context final HttpServletRequest request,
                             @Dimension("entityType") @PathParam("type") final String type,
                             @Dimension("entityName") @PathParam("entity") final String entity,
                             @Dimension("colo") @QueryParam("colo") final String colo) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        return new EntityProxy() {
            @Override
            protected String[] getColos() {
                return getColosToApply(colo);
            }

            @Override
            protected APIResult doExecute(String colo) throws IvoryException {
                return getEntityManager(colo).
                        invoke("suspend", bufferedRequest, type, entity, colo);
            }
        }.execute();
    }

    @POST
    @Path("resume/{type}/{entity}")
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Monitored(event = "resume")
    @Override
    public APIResult resume(@Context final HttpServletRequest request,
                            @Dimension("entityType") @PathParam("type") final String type,
                            @Dimension("entityName") @PathParam("entity") final String entity,
                            @Dimension("colo") @QueryParam("colo") final String colo) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        return new EntityProxy() {
            @Override
            protected String[] getColos() {
                return getColosToApply(colo);
            }

            @Override
            protected APIResult doExecute(String colo) throws IvoryException {
                return getEntityManager(colo).
                        invoke("resume", bufferedRequest, type, entity, colo);
            }
        }.execute();
    }

    private abstract class EntityProxy {

        public APIResult execute() {
            String[] colos = getColos();
            APIResult[] results = new APIResult[colos.length];

            for (int index = 0; index < colos.length; index++) {
                try {
                    results[index] = doExecute(colos[index]);
                } catch (IvoryException e) {
                    results[index] = new APIResult(APIResult.Status.FAILED,
                            e.getClass().getName() + "::" + e.getMessage());
                }
            }
            APIResult finalResult = consolidatedResult(results, colos);
            if (finalResult.getStatus() != APIResult.Status.SUCCEEDED) {
                throw IvoryWebException.newException(finalResult, Response.Status.BAD_REQUEST);
            } else {
                return finalResult;
            }
        }

        protected String[] getColos() {
            return getAllColos();
        }

        protected abstract APIResult doExecute(String colo) throws IvoryException;
    }
}
