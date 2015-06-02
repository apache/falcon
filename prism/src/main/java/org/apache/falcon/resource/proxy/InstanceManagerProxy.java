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
import org.apache.falcon.LifeCycle;
import org.apache.falcon.monitors.Dimension;
import org.apache.falcon.monitors.Monitored;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.AbstractInstanceManager;
import org.apache.falcon.resource.FeedInstanceResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesSummaryResult;
import org.apache.falcon.resource.channel.Channel;
import org.apache.falcon.resource.channel.ChannelFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.lang.reflect.Constructor;
import java.util.*;

/**
 * A proxy implementation of the entity instance operations.
 */
@Path("instance")
public class InstanceManagerProxy extends AbstractInstanceManager {
    private final Map<String, Channel> processInstanceManagerChannels = new HashMap<String, Channel>();

    public InstanceManagerProxy() {
        try {
            Set<String> colos = getAllColos();

            for (String colo : colos) {
                initializeFor(colo);
            }
        } catch (FalconException e) {
            throw new FalconRuntimException("Unable to initialize channels", e);
        }
    }

    private void initializeFor(String colo) throws FalconException {
        processInstanceManagerChannels.put(colo, ChannelFactory.get("ProcessInstanceManager", colo));
    }

    private Channel getInstanceManager(String colo) throws FalconException {
        if (!processInstanceManagerChannels.containsKey(colo)) {
            initializeFor(colo);
        }
        return processInstanceManagerChannels.get(colo);
    }

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    @GET
    @Path("running/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "running")
    @Override
    public InstancesResult getRunningInstances(
            @Dimension("entityType") @PathParam("type") final String type,
            @Dimension("entityName") @PathParam("entity") final String entity,
            @Dimension("colo") @QueryParam("colo") String colo,
            @Dimension("lifecycle") @QueryParam("lifecycle") final List<LifeCycle> lifeCycles,
            @DefaultValue("") @QueryParam("filterBy") final String filterBy,
            @DefaultValue("") @QueryParam("orderBy") final String orderBy,
            @DefaultValue("") @QueryParam("sortOrder") final String sortOrder,
            @DefaultValue("0") @QueryParam("offset") final Integer offset,
            @DefaultValue(DEFAULT_NUM_RESULTS) @QueryParam("numResults") final Integer resultsPerPage) {
        return new InstanceProxy<InstancesResult>(InstancesResult.class) {
            @Override
            protected InstancesResult doExecute(String colo) throws FalconException {
                return getInstanceManager(colo).
                        invoke("getRunningInstances", type, entity, colo, lifeCycles,
                                filterBy, orderBy, sortOrder, offset, resultsPerPage);
            }
        }.execute(colo, type, entity);
    }

    /*
       getStatus(...) method actually gets all instances, filtered by a specific status. This is
       a better named API which achieves the same result
     */
    @GET
    @Path("list/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "instance-list")
    @Override
    public InstancesResult getInstances(
            @Dimension("entityType") @PathParam("type") final String type,
            @Dimension("entityName") @PathParam("entity") final String entity,
            @Dimension("start-time") @QueryParam("start") final String startStr,
            @Dimension("end-time") @QueryParam("end") final String endStr,
            @Dimension("colo") @QueryParam("colo") final String colo,
            @Dimension("lifecycle") @QueryParam("lifecycle") final List<LifeCycle> lifeCycles,
            @DefaultValue("") @QueryParam("filterBy") final String filterBy,
            @DefaultValue("") @QueryParam("orderBy") final String orderBy,
            @DefaultValue("") @QueryParam("sortOrder") final String sortOrder,
            @DefaultValue("0") @QueryParam("offset") final Integer offset,
            @DefaultValue(DEFAULT_NUM_RESULTS) @QueryParam("numResults") final Integer resultsPerPage) {
        return new InstanceProxy<InstancesResult>(InstancesResult.class) {
            @Override
            protected InstancesResult doExecute(String colo) throws FalconException {
                return getInstanceManager(colo).invoke("getInstances",
                        type, entity, startStr, endStr, colo, lifeCycles,
                        filterBy, orderBy, sortOrder, offset, resultsPerPage);
            }
        }.execute(colo, type, entity);
    }

    @GET
    @Path("status/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "instance-status")
    @Override
    public InstancesResult getStatus(
            @Dimension("entityType") @PathParam("type") final String type,
            @Dimension("entityName") @PathParam("entity") final String entity,
            @Dimension("start-time") @QueryParam("start") final String startStr,
            @Dimension("end-time") @QueryParam("end") final String endStr,
            @Dimension("colo") @QueryParam("colo") final String colo,
            @Dimension("lifecycle") @QueryParam("lifecycle") final List<LifeCycle> lifeCycles,
            @DefaultValue("") @QueryParam("filterBy") final String filterBy,
            @DefaultValue("") @QueryParam("orderBy") final String orderBy,
            @DefaultValue("") @QueryParam("sortOrder") final String sortOrder,
            @DefaultValue("0") @QueryParam("offset") final Integer offset,
            @DefaultValue(DEFAULT_NUM_RESULTS) @QueryParam("numResults") final Integer resultsPerPage) {
        return new InstanceProxy<InstancesResult>(InstancesResult.class) {
            @Override
            protected InstancesResult doExecute(String colo) throws FalconException {
                return getInstanceManager(colo).invoke("getStatus",
                        type, entity, startStr, endStr, colo, lifeCycles,
                        filterBy, orderBy, sortOrder, offset, resultsPerPage);
            }
        }.execute(colo, type, entity);
    }

    @GET
    @Path("summary/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "instance-summary")
    @Override
    public InstancesSummaryResult getSummary(
            @Dimension("entityType") @PathParam("type") final String type,
            @Dimension("entityName") @PathParam("entity") final String entity,
            @Dimension("start-time") @QueryParam("start") final String startStr,
            @Dimension("end-time") @QueryParam("end") final String endStr,
            @Dimension("colo") @QueryParam("colo") final String colo,
            @Dimension("lifecycle") @QueryParam("lifecycle") final List<LifeCycle> lifeCycles) {
        return new InstanceProxy<InstancesSummaryResult>(InstancesSummaryResult.class) {
            @Override
            protected InstancesSummaryResult doExecute(String colo) throws FalconException {
                return getInstanceManager(colo).invoke("getSummary",
                        type, entity, startStr, endStr, colo, lifeCycles);
            }
        }.execute(colo, type, entity);
    }

    @GET
    @Path("listing/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "instance-listing")
    @Override
    public FeedInstanceResult getListing(
            @Dimension("type") @PathParam("type") final String type,
            @Dimension("entity") @PathParam("entity") final String entity,
            @Dimension("start-time") @QueryParam("start") final String start,
            @Dimension("end-time") @QueryParam("end") final String end,
            @Dimension("colo") @QueryParam("colo") String colo) {
        return new InstanceProxy<FeedInstanceResult>(FeedInstanceResult.class) {
            @Override
            protected FeedInstanceResult doExecute(String colo) throws FalconException {
                return getInstanceManager(colo).invoke("getListing",
                        type, entity, start, end, colo);
            }
        }.execute(colo, type, entity);
    }

    @GET
    @Path("params/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "instance-params")
    @Override
    public InstancesResult getInstanceParams(
            @Dimension("type") @PathParam("type") final String type,
            @Dimension("entity") @PathParam("entity") final String entity,
            @Dimension("start-time") @QueryParam("start") final String start,
            @Dimension("colo") @QueryParam("colo") String colo,
            @Dimension("lifecycle") @QueryParam("lifecycle") final List<LifeCycle> lifeCycles) {
        return new InstanceProxy<InstancesResult>(InstancesResult.class) {
            @Override
            protected InstancesResult doExecute(String colo) throws FalconException {
                return getInstanceManager(colo).invoke("getInstanceParams",
                        type, entity, start, colo, lifeCycles);
            }
        }.execute(colo, type, entity);
    }

    @GET
    @Path("logs/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "instance-logs")
    @Override
    public InstancesResult getLogs(
            @Dimension("type") @PathParam("type") final String type,
            @Dimension("entity") @PathParam("entity") final String entity,
            @Dimension("start-time") @QueryParam("start") final String startStr,
            @Dimension("end-time") @QueryParam("end") final String endStr,
            @Dimension("colo") @QueryParam("colo") final String colo,
            @Dimension("run-id") @QueryParam("runid") final String runId,
            @Dimension("lifecycle") @QueryParam("lifecycle") final List<LifeCycle> lifeCycles,
            @DefaultValue("") @QueryParam("filterBy") final String filterBy,
            @DefaultValue("") @QueryParam("orderBy") final String orderBy,
            @DefaultValue("") @QueryParam("sortOrder") final String sortOrder,
            @DefaultValue("0") @QueryParam("offset") final Integer offset,
            @DefaultValue(DEFAULT_NUM_RESULTS) @QueryParam("numResults") final Integer resultsPerPage) {
        return new InstanceProxy<InstancesResult>(InstancesResult.class) {
            @Override
            protected InstancesResult doExecute(String colo) throws FalconException {
                return getInstanceManager(colo).invoke("getLogs",
                        type, entity, startStr, endStr, colo, runId, lifeCycles,
                        filterBy, orderBy, sortOrder, offset, resultsPerPage);
            }
        }.execute(colo, type, entity);
    }

    @POST
    @Path("kill/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "kill-instance")
    @Override
    public InstancesResult killInstance(
            @Context HttpServletRequest request,
            @Dimension("entityType") @PathParam("type") final String type,
            @Dimension("entityName") @PathParam("entity") final String entity,
            @Dimension("start-time") @QueryParam("start") final String startStr,
            @Dimension("end-time") @QueryParam("end") final String endStr,
            @Dimension("colo") @QueryParam("colo") final String colo,
            @Dimension("lifecycle") @QueryParam("lifecycle") final List<LifeCycle> lifeCycles) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        return new InstanceProxy<InstancesResult>(InstancesResult.class) {
            @Override
            protected InstancesResult doExecute(String colo) throws FalconException {
                return getInstanceManager(colo).invoke("killInstance",
                        bufferedRequest, type, entity, startStr, endStr, colo, lifeCycles);
            }
        }.execute(colo, type, entity);
    }

    @POST
    @Path("suspend/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "suspend-instance")
    @Override
    public InstancesResult suspendInstance(
            @Context HttpServletRequest request,
            @Dimension("entityType") @PathParam("type") final String type,
            @Dimension("entityName") @PathParam("entity") final String entity,
            @Dimension("start-time") @QueryParam("start") final String startStr,
            @Dimension("end-time") @QueryParam("end") final String endStr,
            @Dimension("colo") @QueryParam("colo") String colo,
            @Dimension("lifecycle") @QueryParam("lifecycle") final List<LifeCycle> lifeCycles) {
        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        return new InstanceProxy<InstancesResult>(InstancesResult.class) {
            @Override
            protected InstancesResult doExecute(String colo) throws FalconException {
                return getInstanceManager(colo).invoke("suspendInstance",
                        bufferedRequest, type, entity, startStr, endStr, colo, lifeCycles);
            }
        }.execute(colo, type, entity);
    }

    @POST
    @Path("resume/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "resume-instance")
    @Override
    public InstancesResult resumeInstance(
            @Context HttpServletRequest request,
            @Dimension("entityType") @PathParam("type") final String type,
            @Dimension("entityName") @PathParam("entity") final String entity,
            @Dimension("start-time") @QueryParam("start") final String startStr,
            @Dimension("end-time") @QueryParam("end") final String endStr,
            @Dimension("colo") @QueryParam("colo") String colo,
            @Dimension("lifecycle") @QueryParam("lifecycle") final List<LifeCycle> lifeCycles) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        return new InstanceProxy<InstancesResult>(InstancesResult.class) {
            @Override
            protected InstancesResult doExecute(String colo) throws FalconException {
                return getInstanceManager(colo).invoke("resumeInstance",
                        bufferedRequest, type, entity, startStr, endStr, colo, lifeCycles);
            }
        }.execute(colo, type, entity);
    }

    @POST
    @Path("rerun/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "re-run-instance")
    @Override
    public InstancesResult reRunInstance(
            @Dimension("entityType") @PathParam("type") final String type,
            @Dimension("entityName") @PathParam("entity") final String entity,
            @Dimension("start-time") @QueryParam("start") final String startStr,
            @Dimension("end-time") @QueryParam("end") final String endStr,
            @Context HttpServletRequest request,
            @Dimension("colo") @QueryParam("colo") String colo,
            @Dimension("lifecycle") @QueryParam("lifecycle") final List<LifeCycle> lifeCycles,
            @Dimension("force") @QueryParam("force") final Boolean isForced) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        return new InstanceProxy<InstancesResult>(InstancesResult.class) {
            @Override
            protected InstancesResult doExecute(String colo) throws FalconException {
                return getInstanceManager(colo).invoke("reRunInstance",
                        type, entity, startStr, endStr, bufferedRequest, colo, lifeCycles, isForced);
            }
        }.execute(colo, type, entity);
    }
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    private abstract class InstanceProxy<T extends APIResult> {

        private final Class<T> clazz;

        public InstanceProxy(Class<T> resultClazz) {
            this.clazz = resultClazz;
        }

        public T execute(String coloExpr, String type, String name) {
            Set<String> colos = getColosFromExpression(coloExpr, type, name);

            Map<String, T> results = new HashMap<String, T>();
            for (String colo : colos) {
                try {
                    T resultHolder = doExecute(colo);
                    results.put(colo, resultHolder);
                } catch (FalconException e) {
                    results.put(colo, getResultInstance(APIResult.Status.FAILED,
                            e.getClass().getName() + "::" + e.getMessage()));
                }
            }
            T finalResult = consolidateResult(results, clazz);
            if (finalResult.getStatus() != APIResult.Status.SUCCEEDED) {
                throw FalconWebException.newException(finalResult, Response.Status.BAD_REQUEST);
            } else {
                return finalResult;
            }
        }

        protected abstract T doExecute(String colo) throws FalconException;

        private T getResultInstance(APIResult.Status status, String message) {
            try {
                Constructor<T> constructor = clazz.getConstructor(APIResult.Status.class, String.class);
                return constructor.newInstance(status, message);
            } catch (Exception e) {
                throw new FalconRuntimException("Unable to consolidate result.", e);
            }
        }
    }
}
