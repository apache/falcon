package org.apache.ivory.resource.proxy;

import org.apache.ivory.IvoryWebException;
import org.apache.ivory.monitors.Dimension;
import org.apache.ivory.monitors.Monitored;
import org.apache.ivory.resource.APIResult;
import org.apache.ivory.resource.AbstractSchedulableEntityManager;
import org.apache.ivory.resource.EntityList;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

@Path("entities")
public class SchedulableEntityManagerProxy extends AbstractSchedulableEntityManager {

    @POST
    @Path("submit/{type}")
    @Consumes({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Monitored(event = "submit")
    @Override
    public APIResult submit(@Context HttpServletRequest request,
                            @Dimension("entityType") @PathParam("type") String type) {
        return super.submit(request, type);
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
        return super.delete(request, type, entity);
    }

    @POST
    @Path("update/{type}/{entity}")
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Monitored(event = "update")
    @Override
    public APIResult update(@Context HttpServletRequest request,
                            @Dimension("entityType") @PathParam("type") String type,
                            @Dimension("entityName") @PathParam("entity") String entityName) {
        return super.update(request, type, entityName);
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
        return super.schedule(request, type, entity);
    }

     @POST
     @Path("submitAndSchedule/{type}")
     @Consumes({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
     @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
     @Monitored(event = "submitAndSchedule")
    @Override
    public APIResult submitAndSchedule(@Context HttpServletRequest request,
                                       @Dimension("entityType") @PathParam("type") String type) {
        return super.submitAndSchedule(request, type);
    }

    @POST
    @Path("suspend/{type}/{entity}")
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Monitored(event = "suspend")
    @Override
    public APIResult suspend(@Context HttpServletRequest request,
                             @Dimension("entityType") @PathParam("type") String type,
                             @Dimension("entityName") @PathParam("entity") String entity) {
        return super.suspend(request, type, entity);
    }

    @POST
    @Path("resume/{type}/{entity}")
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Monitored(event = "resume")
    @Override
    public APIResult resume(@Context HttpServletRequest request,
                            @Dimension("entityType") @PathParam("type") String type,
                            @Dimension("entityName") @PathParam("entity") String entity) {
        return super.resume(request, type, entity);
    }
}
