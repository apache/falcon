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
import org.apache.ivory.util.DeploymentProperties;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("entities")
public class SchedulableEntityManagerProxy extends AbstractSchedulableEntityManager {

    private static final String INTEGRATED = "integrated";
    private static final String DEPLOY_MODE = "deploy.mode";
    private final Channel entityManagerChannel;
    private final Channel configSyncChannel;

    private final boolean integratedMode;

    public SchedulableEntityManagerProxy() {
        try {
            entityManagerChannel = ChannelFactory.get("SchedulableEntityManager");
            configSyncChannel = ChannelFactory.get("ConfigSyncService");
            integratedMode = DeploymentProperties.get().
                    getProperty(DEPLOY_MODE, INTEGRATED).equals(INTEGRATED);
        } catch (IvoryException e) {
            throw new IvoryRuntimException("Unable to initialize channels", e);
        }
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
                            @Dimension("entityType") @PathParam("type") String type) {
        try {
            if (!integratedMode) {
                super.submit(request, type);
            }
            return configSyncChannel.invoke("submit", request, type);
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
                            @Dimension("entityName") @PathParam("entity") String entity) {
        try {
            if (!integratedMode) {
                super.delete(request, type, entity);;
            }
            return configSyncChannel.invoke("delete", request, type, entity);
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
                            @Dimension("entityName") @PathParam("entity") String entityName) {
        try {
            if (!integratedMode) {
                super.update(request, type, entityName);
            }
            return configSyncChannel.invoke("update", request, type, entityName);
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
                              @Dimension("entityName") @PathParam("entity") String entity) {
        try {
            return entityManagerChannel.invoke("schedule", request, type, entity);
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
                                       @Dimension("entityType") @PathParam("type") String type) {

         try {
             if (!integratedMode) {
                 Entity entity = submitInternal(request, type);
                 return entityManagerChannel.invoke("schedule", request, type, entity.getName());
             } else {
                 return entityManagerChannel.invoke("submitAndSchedule", request, type);
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
                             @Dimension("entityName") @PathParam("entity") String entity) {
        try {
            return entityManagerChannel.invoke("suspend", request, type, entity);
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
                            @Dimension("entityName") @PathParam("entity") String entity) {
        try {
            return entityManagerChannel.invoke("resume", request, type, entity);
        } catch (IvoryException e) {
            throw IvoryWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }
}
