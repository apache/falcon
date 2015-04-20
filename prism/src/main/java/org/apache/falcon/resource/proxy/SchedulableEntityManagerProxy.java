/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.resource.proxy;

import org.apache.falcon.FalconException;
import org.apache.falcon.FalconRuntimException;
import org.apache.falcon.FalconWebException;
import org.apache.falcon.entity.EntityNotRegisteredException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.monitors.Dimension;
import org.apache.falcon.monitors.Monitored;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.AbstractSchedulableEntityManager;
import org.apache.falcon.resource.EntityList;
import org.apache.falcon.resource.EntitySummaryResult;
import org.apache.falcon.resource.FeedLookupResult;
import org.apache.falcon.resource.channel.Channel;
import org.apache.falcon.resource.channel.ChannelFactory;

import org.apache.falcon.util.DeploymentUtil;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A proxy implementation of the schedulable entity operations.
 */
@Path("entities")
public class SchedulableEntityManagerProxy extends AbstractSchedulableEntityManager {
    private static final String PRISM_TAG = "prism";
    public static final String FALCON_TAG = "falcon";

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
        if (request instanceof BufferedRequest) {
            return (BufferedRequest) request;
        }
        return new BufferedRequest(request);
    }

    @POST
    @Path("submit/{type}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "submit")
    @Override
    public APIResult submit(
            @Context HttpServletRequest request, @Dimension("entityType") @PathParam("type") final String type,
            @Dimension("colo") @QueryParam("colo") final String ignore) {

        final HttpServletRequest bufferedRequest = getBufferedRequest(request);

        final Entity entity = getEntity(bufferedRequest, type);
        Map<String, APIResult> results = new HashMap<String, APIResult>();
        final Set<String> colos = getApplicableColos(type, entity);

        validateEntity(entity, colos);

        results.put(FALCON_TAG, new EntityProxy(type, entity.getName()) {
            @Override
            protected Set<String> getColosToApply() {
                return colos;
            }

            @Override
            protected APIResult doExecute(String colo) throws FalconException {
                return getConfigSyncChannel(colo).invoke("submit", bufferedRequest, type, colo);
            }
        }.execute());

        if (!embeddedMode) {
            results.put(PRISM_TAG, super.submit(bufferedRequest, type, currentColo));
        }
        return consolidateResult(results, APIResult.class);
    }

    private void validateEntity(Entity entity, Set<String> applicableColos) {
        if (entity.getEntityType() != EntityType.CLUSTER || embeddedMode) {
            return;
        }
        // If the submitted entity is a cluster, ensure its spec. has one of the valid colos
        String colo = ((Cluster) entity).getColo();
        if (!applicableColos.contains(colo)) {
            throw FalconWebException.newException("The colo mentioned in the cluster specification, "
                    + colo + ", is not listed in Prism runtime.", Response.Status.BAD_REQUEST);
        }
    }

    private Entity getEntity(HttpServletRequest request, String type) {
        try {
            request.getInputStream().reset();
            Entity entity = deserializeEntity(request, EntityType.getEnum(type));
            request.getInputStream().reset();
            return entity;
        } catch (Exception e) {
            throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    @POST
    @Path("validate/{type}")
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Override
    public APIResult validate(@Context final HttpServletRequest request, @PathParam("type") final String type) {
        final HttpServletRequest bufferedRequest = getBufferedRequest(request);
        EntityType entityType = EntityType.getEnum(type);
        final Entity entity;
        try {
            entity = deserializeEntity(bufferedRequest, entityType);
            bufferedRequest.getInputStream().reset();
        } catch (Exception e) {
            throw FalconWebException.newException("Unable to parse the request", Response.Status.BAD_REQUEST);
        }
        return new EntityProxy(type, entity.getName()) {
            @Override
            protected Set<String> getColosToApply() {
                return getApplicableColos(type, entity);
            }

            @Override
            protected APIResult doExecute(String colo) throws FalconException {
                return getEntityManager(colo).invoke("validate", bufferedRequest, type);
            }
        }.execute();
    }

    @DELETE
    @Path("delete/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "delete")
    @Override
    public APIResult delete(
            @Context HttpServletRequest request, @Dimension("entityType") @PathParam("type") final String type,
            @Dimension("entityName") @PathParam("entity") final String entity,
            @Dimension("colo") @QueryParam("colo") String ignore) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        Map<String, APIResult> results = new HashMap<String, APIResult>();

        results.put(FALCON_TAG, new EntityProxy(type, entity) {
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
            results.put(PRISM_TAG, super.delete(bufferedRequest, type, entity, currentColo));
        }
        return consolidateResult(results, APIResult.class);
    }

    @POST
    @Path("update/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "update")
    @Override
    public APIResult update(
            @Context HttpServletRequest request, @Dimension("entityType") @PathParam("type") final String type,
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
        if (!oldColos.isEmpty()) {
            results.put(FALCON_TAG + "/delete", new EntityProxy(type, entityName) {
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

        if (!mergedColos.isEmpty()) {
            results.put(FALCON_TAG + "/update", new EntityProxy(type, entityName) {
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

        if (!newColos.isEmpty()) {
            results.put(FALCON_TAG + "/submit", new EntityProxy(type, entityName) {
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

        return consolidateResult(results, APIResult.class);
    }

    @POST
    @Path("touch/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "touch")
    @Override
    public APIResult touch(
            @Dimension("entityType") @PathParam("type") final String type,
            @Dimension("entityName") @PathParam("entity") final String entityName,
            @Dimension("colo") @QueryParam("colo") final String coloExpr) {
        final Set<String> colosFromExp = getColosFromExpression(coloExpr, type, entityName);
        return new EntityProxy(type, entityName) {
            @Override
            protected Set<String> getColosToApply() {
                return colosFromExp;
            }

            @Override
            protected APIResult doExecute(String colo) throws FalconException {
                return getEntityManager(colo).invoke("touch", type, entityName, colo);
            }
        }.execute();
    }

    @GET
    @Path("status/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "status")
    @Override
    public APIResult getStatus(@Dimension("entityType") @PathParam("type") final String type,
                               @Dimension("entityName") @PathParam("entity") final String entity,
                               @Dimension("colo") @QueryParam("colo") final String coloExpr) {
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
    @Produces({MediaType.TEXT_XML, MediaType.APPLICATION_JSON})
    @Monitored(event = "dependencies")
    @Override
    public EntityList getDependencies(@Dimension("entityType") @PathParam("type") String type,
                                      @Dimension("entityName") @PathParam("entity") String entity) {
        return super.getDependencies(type, entity);
    }

    @GET
    @Path("definition/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Override
    public String getEntityDefinition(@PathParam("type") String type, @PathParam("entity") String entityName) {
        return super.getEntityDefinition(type, entityName);
    }

    @POST
    @Path("schedule/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
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
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "submitAndSchedule")
    @Override
    public APIResult submitAndSchedule(
            @Context HttpServletRequest request, @Dimension("entityType") @PathParam("type") String type,
            @Dimension("colo") @QueryParam("colo") String coloExpr) {
        BufferedRequest bufferedRequest = new BufferedRequest(request);
        String entity = getEntity(bufferedRequest, type).getName();
        Map<String, APIResult> results = new HashMap<String, APIResult>();
        results.put("submit", submit(bufferedRequest, type, coloExpr));
        results.put("schedule", schedule(bufferedRequest, type, entity, coloExpr));
        return consolidateResult(results, APIResult.class);
    }

    @POST
    @Path("suspend/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
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
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "resume")
    @Override
    public APIResult resume(
            @Context final HttpServletRequest request, @Dimension("entityType") @PathParam("type") final String type,
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

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    @GET
    @Path("list/{type}")
    @Produces({MediaType.TEXT_XML, MediaType.APPLICATION_JSON})
    @Override
    public EntityList getEntityList(@PathParam("type") String type,
                                    @DefaultValue("") @QueryParam("fields") String fields,
                                    @DefaultValue("") @QueryParam("filterBy") String filterBy,
                                    @DefaultValue("") @QueryParam("tags") String tags,
                                    @DefaultValue("") @QueryParam("orderBy") String orderBy,
                                    @DefaultValue("asc") @QueryParam("sortOrder") String sortOrder,
                                    @DefaultValue("0") @QueryParam("offset") Integer offset,
                                    @DefaultValue(DEFAULT_NUM_RESULTS)
                                    @QueryParam("numResults") Integer resultsPerPage,
                                    @QueryParam("nameseq") String nameseq) {
        return super.getEntityList(type, fields, filterBy, tags, orderBy, sortOrder, offset, resultsPerPage, nameseq);
    }

    @GET
    @Path("summary/{type}")
    @Produces({MediaType.TEXT_XML, MediaType.APPLICATION_JSON})
    @Monitored(event = "summary")
    @Override
    public EntitySummaryResult getEntitySummary(
            @Dimension("type") @PathParam("type") final String type,
            @Dimension("cluster") @QueryParam("cluster") final String cluster,
            @DefaultValue("") @QueryParam("start") String startStr,
            @DefaultValue("") @QueryParam("end") String endStr,
            @DefaultValue("") @QueryParam("fields") final String entityFields,
            @DefaultValue("") @QueryParam("filterBy") final String entityFilter,
            @DefaultValue("") @QueryParam("tags") final String entityTags,
            @DefaultValue("") @QueryParam("orderBy") final String entityOrderBy,
            @DefaultValue("asc") @QueryParam("sortOrder") String entitySortOrder,
            @DefaultValue("0") @QueryParam("offset") final Integer entityOffset,
            @DefaultValue("10") @QueryParam("numResults") final Integer numEntities,
            @DefaultValue("7") @QueryParam("numInstances") final Integer numInstanceResults) {
        return super.getEntitySummary(type, cluster, startStr, endStr, entityFields, entityFilter,
                entityTags, entityOrderBy, entitySortOrder, entityOffset, numEntities, numInstanceResults);
    }

    @GET
    @Path("lookup/{type}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    @Monitored(event = "reverse-lookup")
    public FeedLookupResult reverseLookup(
            @Dimension("type") @PathParam("type") final String type,
            @Dimension("path") @QueryParam("path") final String path) {
        String entity = "DummyEntity"; // A dummy entity name to get around
        return new EntityProxy<FeedLookupResult>(type, entity, FeedLookupResult.class) {
            @Override
            protected Set<String> getColosToApply() {
                return getAllColos();
            }

            @Override
            protected FeedLookupResult doExecute(String colo) throws FalconException {
                return getEntityManager(colo).invoke("reverseLookup", type, path);
            }
        }.execute();

    }
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    private abstract class EntityProxy<T extends APIResult> {
        private final Class<T> clazz;
        private String type;
        private String name;

        public EntityProxy(String type, String name, Class<T> resultClazz) {
            this.clazz = resultClazz;
            this.type = type;
            this.name = name;
        }


        private T getResultInstance(APIResult.Status status, String message) {
            try {
                Constructor<T> constructor = clazz.getConstructor(APIResult.Status.class, String.class);
                return constructor.newInstance(status, message);
            } catch (Exception e) {
                throw new FalconRuntimException("Unable to consolidate result.", e);
            }
        }

        public EntityProxy(String type, String name) {
            this(type, name, (Class<T>) APIResult.class);
        }

        public T execute() {
            Set<String> colos = getColosToApply();

            Map<String, T> results = new HashMap();

            for (String colo : colos) {
                try {
                    results.put(colo, doExecute(colo));
                } catch (FalconException e) {
                    results.put(colo, getResultInstance(APIResult.Status.FAILED, e.getClass().getName() + "::"
                            + e.getMessage()));
                }
            }

            T finalResult = consolidateResult(results, clazz);
            if (finalResult.getStatus() != APIResult.Status.SUCCEEDED) {
                throw FalconWebException.newException(finalResult, Response.Status.BAD_REQUEST);
            } else {
                return finalResult;
            }
        }

        protected Set<String> getColosToApply() {
            return getApplicableColos(type, name);
        }

        protected abstract T doExecute(String colo) throws FalconException;
    }
}
