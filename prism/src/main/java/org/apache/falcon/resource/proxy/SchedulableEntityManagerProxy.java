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
import org.apache.falcon.monitors.Dimension;
import org.apache.falcon.monitors.Monitored;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.AbstractSchedulableEntityManager;
import org.apache.falcon.resource.EntityList;
import org.apache.falcon.resource.channel.Channel;
import org.apache.falcon.resource.channel.ChannelFactory;
import org.apache.falcon.util.DeploymentUtil;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
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

        final String entity = getEntity(bufferedRequest, type).getName();
        Map<String, APIResult> results = new HashMap<String, APIResult>();
        final Set<String> colos = getApplicableColos(type, getEntity(bufferedRequest, type));
        results.put(FALCON_TAG, new EntityProxy(type, entity) {
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
        return consolidateResult(results);
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
    @Consumes({MediaType.TEXT_XML, MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Override
    public APIResult validate(@Context HttpServletRequest request, @PathParam("type") String type) {
        return super.validate(request, type);
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
        return consolidateResult(results);
    }

    @POST
    @Path("update/{type}/{entity}")
    @Produces({MediaType.TEXT_XML, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
    @Monitored(event = "update")
    @Override
    public APIResult update(
            @Context HttpServletRequest request, @Dimension("entityType") @PathParam("type") final String type,
            @Dimension("entityName") @PathParam("entity") final String entityName,
            @Dimension("colo") @QueryParam("colo") String ignore,
            @Dimension("effective") @DefaultValue("") @QueryParam("effective") final String effectiveTime) {

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
                    return getConfigSyncChannel(colo).invoke("update", bufferedRequest, type, entityName, colo,
                            effectiveTime);
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
            results.put(PRISM_TAG, super.update(bufferedRequest, type, entityName, currentColo, effectiveTime));
        }

        return consolidateResult(results);
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
    @Path("list/{type}")
    @Produces({MediaType.TEXT_XML, MediaType.APPLICATION_JSON})
    @Override
    public EntityList getEntityList(@PathParam("type") String type,
                                    @DefaultValue("") @QueryParam("fields") String fields,
                                    @DefaultValue("") @QueryParam("statusFilter") String statusFilter,
                                    @DefaultValue("") @QueryParam("orderBy") String orderBy,
                                    @DefaultValue("0") @QueryParam("offset") Integer offset,
                                    @DefaultValue("-1") @QueryParam("numResults") Integer resultsPerPage) {
        return super.getEntityList(type, fields, statusFilter, orderBy, offset, resultsPerPage);
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
        return consolidateResult(results);
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
                    results.put(colo,
                            new APIResult(APIResult.Status.FAILED, e.getClass().getName() + "::" + e.getMessage()));
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
        if (results == null || results.size() == 0) {
            return null;
        }

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
