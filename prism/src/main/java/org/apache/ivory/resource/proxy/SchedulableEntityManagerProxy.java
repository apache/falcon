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
                            @Dimension("entityType") @PathParam("type") String type,
                            @Dimension("colo") @QueryParam("colo") String colo) {
        try {
            if (!integratedMode) {
                super.submit(request, type, DEFAULT_COLO);
            }
            String[] colos = getAllColos();
            APIResult[] results = new APIResult[colos.length];

            for (int index = 0; index < colos.length; index++) {
                results[index] = getConfigSyncChannel(colos[index]).
                        invoke("submit", request, type, colos[index]);
            }
            return consolidatedResult(results, colos);
        } catch (IvoryException e) {
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
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
                            @Dimension("entityType") @PathParam("type") String type,
                            @Dimension("entityName") @PathParam("entity") String entity,
                            @Dimension("colo") @QueryParam("colo") String ignore) {
        try {
            if (!integratedMode) {
                super.delete(request, type, entity, DEFAULT_COLO);
            }
            String[] colos = getAllColos();
            APIResult[] results = new APIResult[colos.length];

            for (int index = 0; index < colos.length; index++) {
                results[index] = getConfigSyncChannel(colos[index]).
                        invoke("delete", request, type, entity, colos[index]);
            }
            return consolidatedResult(results, colos);
        } catch (IvoryException e) {
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Path("update/{type}/{entity}")
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Monitored(event = "update")
    @Override
    public APIResult update(@Context HttpServletRequest request,
                            @Dimension("entityType") @PathParam("type") String type,
                            @Dimension("entityName") @PathParam("entity") String entityName,
                            @Dimension("colo") @QueryParam("colo") String ignore) {
        try {
            if (!integratedMode) {
                super.update(request, type, entityName, DEFAULT_COLO);
            }
            String[] colos = getAllColos();
            APIResult[] results = new APIResult[colos.length];

            for (int index = 0; index < colos.length; index++) {
                results[index] = getConfigSyncChannel(colos[index]).
                        invoke("update", request, type, entityName, colos[index]);
            }
            return consolidatedResult(results, colos);
        } catch (IvoryException e) {
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
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
    public APIResult schedule(@Context HttpServletRequest request,
                              @Dimension("entityType") @PathParam("type") String type,
                              @Dimension("entityName") @PathParam("entity") String entity,
                              @Dimension("colo") @QueryParam("colo") String colo) {
        try {
            String[] colos = getColosToApply(colo);

            APIResult[] results = new APIResult[colos.length];
            for (int index = 0; index < colos.length; index++) {
                results[index] = getEntityManager(colos[index]).
                        invoke("schedule", request, type, entity, colos[index]);
            }
            return consolidatedResult(results, colos);
        } catch (IvoryException e) {
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
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
             throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
         }
    }

    @POST
    @Path("suspend/{type}/{entity}")
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Monitored(event = "suspend")
    @Override
    public APIResult suspend(@Context HttpServletRequest request,
                             @Dimension("entityType") @PathParam("type") String type,
                             @Dimension("entityName") @PathParam("entity") String entity,
                             @Dimension("colo") @QueryParam("colo") String colo) {

        try {
            String[] colos = getColosToApply(colo);

            APIResult[] results = new APIResult[colos.length];
            for (int index = 0; index < colos.length; index++) {
                results[index] = getEntityManager(colos[index]).
                        invoke("suspend", request, type, entity, colos[index]);
            }
            return consolidatedResult(results, colos);
        } catch (IvoryException e) {
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Path("resume/{type}/{entity}")
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Monitored(event = "resume")
    @Override
    public APIResult resume(@Context HttpServletRequest request,
                            @Dimension("entityType") @PathParam("type") String type,
                            @Dimension("entityName") @PathParam("entity") String entity,
                            @Dimension("colo") @QueryParam("colo") String colo) {
        try {
            String[] colos = getColosToApply(colo);

            APIResult[] results = new APIResult[colos.length];
            for (int index = 0; index < colos.length; index++) {
                results[index] = getEntityManager(colos[index]).
                        invoke("resume", request, type, entity, colos[index]);
            }
            return consolidatedResult(results, colos);
        } catch (IvoryException e) {
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }
}
