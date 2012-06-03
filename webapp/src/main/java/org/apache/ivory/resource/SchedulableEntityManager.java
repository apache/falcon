package org.apache.ivory.resource;

import org.apache.ivory.IvoryWebException;
import org.apache.ivory.monitors.Dimension;
import org.apache.ivory.monitors.Monitored;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

@Path("entities")
public class SchedulableEntityManager extends AbstractSchedulableEntityManager {

    @Override
    public String getName() {
        return getClass().getName();
    }

    @GET
    @Path("status/{type}/{entity}")
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Monitored(event = "status")
    @Override
    public APIResult getStatus(@Dimension("entityType") @PathParam("type") String type,
                            @Dimension("entityName") @PathParam("entity") String entity,
                            @Dimension("colo") @QueryParam("colo") final String colo)
            throws IvoryWebException {
        return super.getStatus(type, entity, colo);
    }

    @GET
    @Path("missing/{type}/{entity}")
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Monitored(event = "isMissing")
    @Override
    public APIResult isMissing(@Dimension("entityType") @PathParam("type") String type,
                               @Dimension("entityName") @PathParam("entity") String entity,
                               @Dimension("colo") @QueryParam("colo") final String colo)
            throws IvoryWebException {
        return super.isMissing(type, entity, colo);
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
    @Monitored(event = "dependencies")
    @Override
    public EntityList getDependencies(@Dimension("type") @PathParam("type") String type) {
        return super.getDependencies(type);
    }

    @GET
    @Path("definition/{type}/{entity}")
    @Produces({ MediaType.TEXT_XML, MediaType.TEXT_PLAIN })
    @Monitored(event = "definition")
    @Override
    public String getEntityDefinition(@Dimension("type") @PathParam("type") String type,
                                      @Dimension("entity") @PathParam("entity") String entityName) {
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
        return super.schedule(request, type, entity, colo);
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
        return super.submitAndSchedule(request, type, colo);
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
        return super.suspend(request, type, entity, colo);
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
        return super.resume(request, type, entity, colo);
    }
}
