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
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesResult.Instance;
import org.apache.falcon.resource.InstancesSummaryResult;
import org.apache.falcon.resource.InstancesSummaryResult.InstanceSummary;
import org.apache.falcon.resource.channel.Channel;
import org.apache.falcon.resource.channel.ChannelFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;

/**
 * A proxy implementation of the entity instance operations.
 */
@Path("instance")
public class InstanceManagerProxy extends AbstractInstanceManager {
    private static final String DEFAULT_NUM_RESULTS = "10";
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
            @DefaultValue("0") @QueryParam("offset") final Integer offset,
            @DefaultValue(DEFAULT_NUM_RESULTS) @QueryParam("numResults") final Integer resultsPerPage) {
        return new InstanceProxy() {
            @Override
            protected InstancesResult doExecute(String colo) throws FalconException {
                return getInstanceManager(colo).
                        invoke("getRunningInstances", type, entity, colo, lifeCycles,
                                filterBy, orderBy, offset, resultsPerPage);
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
            @DefaultValue("0") @QueryParam("offset") final Integer offset,
            @DefaultValue(DEFAULT_NUM_RESULTS) @QueryParam("numResults") final Integer resultsPerPage) {
        return new InstanceProxy() {
            @Override
            protected InstancesResult doExecute(String colo) throws FalconException {
                return getInstanceManager(colo).invoke("getInstances",
                        type, entity, startStr, endStr, colo, lifeCycles,
                        filterBy, orderBy, offset, resultsPerPage);
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
            @DefaultValue("0") @QueryParam("offset") final Integer offset,
            @DefaultValue(DEFAULT_NUM_RESULTS) @QueryParam("numResults") final Integer resultsPerPage) {
        return new InstanceProxy() {
            @Override
            protected InstancesResult doExecute(String colo) throws FalconException {
                return getInstanceManager(colo).invoke("getStatus",
                        type, entity, startStr, endStr, colo, lifeCycles,
                        filterBy, orderBy, offset, resultsPerPage);
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
        return new InstanceSummaryProxy() {
            @Override
            protected InstancesSummaryResult doExecute(String colo) throws FalconException {
                return getInstanceManager(colo).invoke("getSummary",
                        type, entity, startStr, endStr, colo, lifeCycles);
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
        return new InstanceProxy() {
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
            @DefaultValue("0") @QueryParam("offset") final Integer offset,
            @DefaultValue(DEFAULT_NUM_RESULTS) @QueryParam("numResults") final Integer resultsPerPage) {
        return new InstanceProxy() {
            @Override
            protected InstancesResult doExecute(String colo) throws FalconException {
                return getInstanceManager(colo).invoke("getLogs",
                        type, entity, startStr, endStr, colo, runId, lifeCycles,
                        filterBy, orderBy, offset, resultsPerPage);
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
        return new InstanceProxy() {
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
        return new InstanceProxy() {
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
        return new InstanceProxy() {
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
            @Dimension("lifecycle") @QueryParam("lifecycle") final List<LifeCycle> lifeCycles) {

        final HttpServletRequest bufferedRequest = new BufferedRequest(request);
        return new InstanceProxy() {
            @Override
            protected InstancesResult doExecute(String colo) throws FalconException {
                return getInstanceManager(colo).invoke("reRunInstance",
                        type, entity, startStr, endStr, bufferedRequest, colo, lifeCycles);
            }
        }.execute(colo, type, entity);
    }
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    private abstract class InstanceProxy {

        public InstancesResult execute(String coloExpr, String type, String name) {
            Set<String> colos = getColosFromExpression(coloExpr, type, name);

            Map<String, InstancesResult> results = new HashMap<String, InstancesResult>();
            for (String colo : colos) {
                try {
                    InstancesResult resultHolder = doExecute(colo);
                    results.put(colo, resultHolder);
                } catch (FalconException e) {
                    results.put(colo, new InstancesResult(APIResult.Status.FAILED,
                            e.getClass().getName() + "::" + e.getMessage(),
                            new InstancesResult.Instance[0]));
                }
            }
            InstancesResult finalResult = consolidateInstanceResult(results);
            if (finalResult.getStatus() != APIResult.Status.SUCCEEDED) {
                throw FalconWebException.newException(finalResult, Response.Status.BAD_REQUEST);
            } else {
                return finalResult;
            }
        }

        protected abstract InstancesResult doExecute(String colo) throws FalconException;
    }

    private abstract class InstanceSummaryProxy {

        public InstancesSummaryResult execute(String coloExpr, String type, String name) {
            Set<String> colos = getColosFromExpression(coloExpr, type, name);

            Map<String, InstancesSummaryResult> results = new HashMap<String, InstancesSummaryResult>();
            for (String colo : colos) {
                try {
                    InstancesSummaryResult resultHolder = doExecute(colo);
                    results.put(colo, resultHolder);
                } catch (FalconException e) {
                    results.put(colo, new InstancesSummaryResult(APIResult.Status.FAILED,
                            e.getClass().getName() + "::" + e.getMessage(),
                            new InstancesSummaryResult.InstanceSummary[0]));
                }
            }
            InstancesSummaryResult finalResult = consolidateInstanceSummaryResult(results);
            if (finalResult.getStatus() != APIResult.Status.SUCCEEDED) {
                throw FalconWebException.newException(finalResult, Response.Status.BAD_REQUEST);
            } else {
                return finalResult;
            }
        }

        protected abstract InstancesSummaryResult doExecute(String colo) throws FalconException;
    }

    private InstancesResult consolidateInstanceResult(Map<String, InstancesResult> results) {
        if (results == null || results.isEmpty()) {
            return null;
        }

        StringBuilder message = new StringBuilder();
        StringBuilder requestIds = new StringBuilder();
        List<Instance> instances = new ArrayList<Instance>();
        int statusCount = 0;
        for (Map.Entry<String, InstancesResult> entry : results.entrySet()) {
            String colo = entry.getKey();
            InstancesResult result = results.get(colo);
            message.append(colo).append('/').append(result.getMessage()).append('\n');
            requestIds.append(colo).append('/').append(result.getRequestId()).append('\n');
            statusCount += result.getStatus().ordinal();

            if (result.getInstances() == null) {
                continue;
            }

            for (Instance instance : result.getInstances()) {
                instance.instance = instance.getInstance();
                instances.add(instance);
            }
        }
        Instance[] arrInstances = new Instance[instances.size()];
        APIResult.Status status = (statusCount == 0) ? APIResult.Status.SUCCEEDED
                : ((statusCount == results.size() * 2) ? APIResult.Status.FAILED : APIResult.Status.PARTIAL);
        InstancesResult result = new InstancesResult(status, message.toString(), instances.toArray(arrInstances));
        result.setRequestId(requestIds.toString());
        return result;
    }

    private InstancesSummaryResult consolidateInstanceSummaryResult(Map<String, InstancesSummaryResult> results) {
        if (results == null || results.isEmpty()) {
            return null;
        }

        StringBuilder message = new StringBuilder();
        StringBuilder requestIds = new StringBuilder();
        List<InstanceSummary> instances = new ArrayList<InstanceSummary>();
        int statusCount = 0;
        for (Map.Entry<String, InstancesSummaryResult> entry : results.entrySet()) {
            String colo = entry.getKey();
            InstancesSummaryResult result = results.get(colo);
            message.append(colo).append('/').append(result.getMessage()).append('\n');
            requestIds.append(colo).append('/').append(result.getRequestId()).append('\n');
            statusCount += result.getStatus().ordinal();

            if (result.getInstancesSummary() == null) {
                continue;
            }

            for (InstanceSummary instance : result.getInstancesSummary()) {
                instance.summaryMap = instance.getSummaryMap();
                instances.add(instance);
            }
        }
        InstanceSummary[] arrInstances = new InstanceSummary[instances.size()];
        APIResult.Status status = (statusCount == 0) ? APIResult.Status.SUCCEEDED
                : ((statusCount == results.size() * 2) ? APIResult.Status.FAILED : APIResult.Status.PARTIAL);
        InstancesSummaryResult result = new InstancesSummaryResult(status, message.toString(),
                instances.toArray(arrInstances));
        result.setRequestId(requestIds.toString());
        return result;
    }
}
