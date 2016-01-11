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

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.falcon.FalconException;
import org.apache.falcon.FalconRuntimException;
import org.apache.falcon.FalconWebException;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.monitors.Dimension;
import org.apache.falcon.monitors.Monitored;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.AbstractInstanceManager;
import org.apache.falcon.resource.FeedInstanceResult;
import org.apache.falcon.resource.InstanceDependencyResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesSummaryResult;
import org.apache.falcon.resource.TriageResult;
import org.apache.falcon.resource.channel.Channel;
import org.apache.falcon.resource.channel.ChannelFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A proxy implementation of the entity instance operations.
 */
@Path("instance")
public class InstanceManagerProxy extends AbstractInstanceManager {
    private static final Logger LOG = LoggerFactory.getLogger(InstanceManagerProxy.class);

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
    /**
     * Get a list of instances currently running for a given entity.
     * @param type Valid options are cluster, feed or process.
     * @param entity Name of the entity.
     * @param colo <optional param> Colo on which the query should be run.
     * @param lifeCycles <optional param> Valid lifecycles for feed are Eviction/Replication(default) and for process
     *                   is Execution(default).
     * @param filterBy <optional param> Filter results by list of field:value pairs. Example:
     *                 filterBy=CLUSTER:primary-cluster
     *                 Supported filter fields are CLUSTER, SOURCECLUSTER, STARTEDAFTER.
     *                 Query will do an AND among filterBy fields.
     * @param orderBy <optional param> Field by which results should be ordered
     *                Supports ordering by "status","startTime","endTime","cluster".
     * @param sortOrder <optional param> Valid options are "asc" and "desc"
     * @param offset <optional param> Show results from the offset, used for pagination. Defaults to 0.
     * @param numResults <optional param> Number of results to show per request, used for pagination.
     *                   Only integers > 0 are valid, Default is 10.
     * @return List of instances currently running.
     */
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
            @QueryParam("numResults") final Integer numResults) {
        final Integer resultsPerPage = numResults == null ? getDefaultResultsPerPage() : numResults;
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
    /**
     * Get list of all instances of a given entity.
     * @param type Valid options are cluster, feed or process.
     * @param entity Name of the entity.
     * @param startStr <optional param> Show instances from this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
     *                 By default, it is set to (end - (10 * entityFrequency)).
     * @param endStr <optional param> Show instances up to this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
     *               Default is set to now.
     * @param colo <optional param> Colo on which the query should be run.
     * @param lifeCycles <optional param> Valid lifecycles for feed are Eviction/Replication(default) and for process
     *                   is Execution(default).
     * @param filterBy <optional param> Filter results by list of field:value pairs. Example:
     *                 filterBy=STATUS:RUNNING,CLUSTER:primary-cluster
     *                 Supported filter fields are STATUS, CLUSTER, SOURCECLUSTER, STARTEDAFTER.
     *                 Query will do an AND among filterBy fields.
     * @param orderBy <optional param> Field by which results should be ordered.
     *                Supports ordering by "status","startTime","endTime","cluster".
     * @param sortOrder <optional param> Valid options are "asc" and "desc"
     * @param offset <optional param> Show results from the offset, used for pagination. Defaults to 0.
     * @param numResults <optional param> Number of results to show per request, used for pagination.
     *                   Only integers > 0 are valid, Default is 10.
     * @return List of instances of given entity
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
            @QueryParam("numResults") Integer numResults,
            @Dimension("allAttempts") @QueryParam("allAttempts") final Boolean allAttempts) {
        final Integer resultsPerPage = numResults == null ? getDefaultResultsPerPage() : numResults;
        return new InstanceProxy<InstancesResult>(InstancesResult.class) {
            @Override
            protected InstancesResult doExecute(String colo) throws FalconException {
                return getInstanceManager(colo).invoke("getInstances",
                        type, entity, startStr, endStr, colo, lifeCycles,
                        filterBy, orderBy, sortOrder, offset, resultsPerPage, allAttempts);
            }
        }.execute(colo, type, entity);
    }

    /**
     * Get status of a specific instance of an entity.
     * @param type Valid options are cluster, feed or process.
     * @param entity Name of the entity.
     * @param startStr <optional param> Show instances from this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
     *                 By default, it is set to (end - (10 * entityFrequency)).
     * @param endStr <optional param> Show instances up to this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
     *               Default is set to now.
     * @param colo <optional param> Colo on which the query should be run.
     * @param lifeCycles <optional param> Valid lifecycles for feed are Eviction/Replication(default) and for process
     *                   is Execution(default).
     * @param filterBy <optional param> Filter results by list of field:value pairs. Example:
     *                 filterBy=STATUS:RUNNING,CLUSTER:primary-cluster
     *                 Supported filter fields are STATUS, CLUSTER, SOURCECLUSTER, STARTEDAFTER.
     *                 Query will do an AND among filterBy fields.
     * @param orderBy <optional param> Field by which results should be ordered.
     *                Supports ordering by "status","startTime","endTime","cluster".
     * @param sortOrder <optional param> Valid options are "asc" and "desc"
     * @param offset <optional param> Show results from the offset, used for pagination. Defaults to 0.
     * @param numResults <optional param> Number of results to show per request, used for pagination.
     *                   Only integers > 0 are valid, Default is 10.
     * @return Status of the specified instance along with job urls for all actions of user workflow and non-succeeded
     *         actions of the main-workflow.
     */
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
            @QueryParam("numResults") final Integer numResults,
            @Dimension("allAttempts") @QueryParam("allAttempts") final Boolean allAttempts) {
        final Integer resultsPerPage = numResults == null ? getDefaultResultsPerPage() : numResults;
        return new InstanceProxy<InstancesResult>(InstancesResult.class) {
            @Override
            protected InstancesResult doExecute(String colo) throws FalconException {
                return getInstanceManager(colo).invoke("getStatus",
                        type, entity, startStr, endStr, colo, lifeCycles,
                        filterBy, orderBy, sortOrder, offset, resultsPerPage, allAttempts);
            }
        }.execute(colo, type, entity);
    }

    /**
     * Get summary of instance/instances of an entity.
     * @param type Valid options are cluster, feed or process.
     * @param entity Name of the entity.
     * @param startStr <optional param> Show instances from this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
     *                 By default, it is set to (end - (10 * entityFrequency)).
     * @param endStr <optional param> Show instances up to this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
     *               Default is set to now.
     * @param colo <optional param> Colo on which the query should be run.
     * @param lifeCycles <optional param> Valid lifecycles for feed are Eviction/Replication(default) and for process
     *                   is Execution(default).
     * @param filterBy <optional param> Filter results by list of field:value pairs.
     *                 Example1: filterBy=STATUS:RUNNING,CLUSTER:primary-cluster
     *                 Example2: filterBy=Status:RUNNING,Status:KILLED
     *                 Supported filter fields are STATUS, CLUSTER.
     *                 Query will do an AND among filterBy fields.
     * @param orderBy <optional param> Field by which results should be ordered.
     *                Supports ordering by "cluster". Example: orderBy=cluster
     * @param sortOrder <optional param> Valid options are "asc" and "desc". Example: sortOrder=asc
     * @return Summary of the instances over the specified time range
     */
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
            @Dimension("lifecycle") @QueryParam("lifecycle") final List<LifeCycle> lifeCycles,
            @DefaultValue("") @QueryParam("filterBy") final String filterBy,
            @DefaultValue("") @QueryParam("orderBy") final String orderBy,
            @DefaultValue("") @QueryParam("sortOrder") final String sortOrder) {
        return new InstanceProxy<InstancesSummaryResult>(InstancesSummaryResult.class) {
            @Override
            protected InstancesSummaryResult doExecute(String colo) throws FalconException {
                return getInstanceManager(colo).invoke("getSummary",
                        type, entity, startStr, endStr, colo, lifeCycles,
                        filterBy, orderBy, sortOrder);
            }
        }.execute(colo, type, entity);
    }

    /**
     * Get falcon feed instance availability.
     * @param type Valid options is feed.
     * @param entity Name of the entity.
     * @param start <optional param> Show instances from this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
     *              By default, it is set to (end - (10 * entityFrequency)).
     * @param end <optional param> Show instances up to this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
     *            Default is set to now.
     * @param colo Colo on which the query should be run.
     * @return Feed instance availability status
     */
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

    /**
     * Get the params passed to the workflow for an instance of feed/process.
     * @param type Valid options are cluster, feed or process.
     * @param entity Name of the entity.
     * @param start should be the nominal time of the instance for which you want the params to be returned
     * @param colo <optional param> Colo on which the query should be run.
     * @param lifeCycles <optional param> Valid lifecycles for feed are Eviction/Replication(default) and for process is
     *                   Execution(default).
     * @return List of instances currently running.
     */
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

    /**
     * Get log of a specific instance of an entity.
     * @param type Valid options are cluster, feed or process.
     * @param entity Name of the entity.
     * @param startStr <optional param> Show instances from this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
     *                 By default, it is set to (end - (10 * entityFrequency)).
     * @param endStr <optional param> Show instances up to this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
     *               Default is set to now.
     * @param colo <optional param> Colo on which the query should be run.
     * @param runId <optional param> Run Id.
     * @param lifeCycles <optional param> Valid lifecycles for feed are Eviction/Replication(default) and for process is
     *                   Execution(default).
     * @param filterBy <optional param> Filter results by list of field:value pairs.
     *                 Example: filterBy=STATUS:RUNNING,CLUSTER:primary-cluster
     *                 Supported filter fields are STATUS, CLUSTER, SOURCECLUSTER, STARTEDAFTER.
     *                 Query will do an AND among filterBy fields.
     * @param orderBy <optional param> Field by which results should be ordered.
     *                Supports ordering by "status","startTime","endTime","cluster".
     * @param sortOrder <optional param> Valid options are "asc" and "desc"
     * @param offset <optional param> Show results from the offset, used for pagination. Defaults to 0.
     * @param numResults <optional param> Number of results to show per request, used for pagination. Only integers > 0
     *                   are valid, Default is 10.
     * @return Log of specified instance.
     */
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
            @QueryParam("numResults") final Integer numResults) {
        final Integer resultsPerPage = numResults == null ? getDefaultResultsPerPage() : numResults;
        return new InstanceProxy<InstancesResult>(InstancesResult.class) {
            @Override
            protected InstancesResult doExecute(String colo) throws FalconException {
                return getInstanceManager(colo).invoke("getLogs",
                        type, entity, startStr, endStr, colo, runId, lifeCycles,
                        filterBy, orderBy, sortOrder, offset, resultsPerPage);
            }
        }.execute(colo, type, entity);
    }

    /**
     * Kill currently running instance(s) of an entity.
     * @param request Servlet Request
     * @param type Valid options are feed or process.
     * @param entity name of the entity.
     * @param startStr start time of the instance(s) that you want to refer to
     * @param endStr end time of the instance(s) that you want to refer to
     * @param colo Colo on which the query should be run.
     * @param lifeCycles <optional param> can be Eviction/Replication(default) for feed and Execution(default) for
     *                   process.
     * @return Result of the kill operation.
     */
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

    /**
     * Suspend instances of an entity.
     * @param request Servlet Request
     * @param type Valid options are feed or process.
     * @param entity name of the entity.
     * @param startStr the start time of the instance(s) that you want to refer to
     * @param endStr the end time of the instance(s) that you want to refer to
     * @param colo Colo on which the query should be run.
     * @param lifeCycles <optional param> can be Eviction/Replication(default) for feed and Execution(default) for
     *                   process.
     * @return Results of the suspend command.
     */
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

    /**
     * Resume suspended instances of an entity.
     * @param request Servlet Request
     * @param type Valid options are feed or process.
     * @param entity name of the entity.
     * @param startStr start time of the instance(s) that you want to refer to
     * @param endStr the end time of the instance(s) that you want to refer to
     * @param colo Colo on which the query should be run.
     * @param lifeCycles <optional param> can be Eviction/Replication(default) for feed and Execution(default) for
     *                   process.
     * @return Results of the resume command.
     */
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

    /**
     * Rerun instances of an entity. On issuing a rerun, by default the execution resumes from the last failed node in
     * the workflow.
     * @param type Valid options are feed or process.
     * @param entity name of the entity.
     * @param startStr start is the start time of the instance that you want to refer to
     * @param endStr end is the end time of the instance that you want to refer to
     * @param request Servlet Request
     * @param colo Colo on which the query should be run.
     * @param lifeCycles <optional param> can be Eviction/Replication(default) for feed and Execution(default) for
     *                   process.
     * @param isForced <optional param> can be used to forcefully rerun the entire instance.
     * @return Results of the rerun command.
     */
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


    /**
     * Get dependent instances for a particular instance.
     * @param entityType Valid options are feed or process.
     * @param entityName Name of the entity
     * @param instanceTimeStr <mandatory param> time of the given instance
     * @param colo Colo on which the query should be run.
     * @return Dependent instances for the specified instance
     */
    @GET
    @Path("dependencies/{type}/{entity}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "instance-dependency")
    public InstanceDependencyResult instanceDependencies(
            @Dimension("type") @PathParam("type") final String entityType,
            @Dimension("entityName") @PathParam("entity") final String entityName,
            @Dimension("instanceTime") @QueryParam("instanceTime") final String instanceTimeStr,
            @Dimension("colo") @QueryParam("colo") String colo) {

        return new InstanceProxy<InstanceDependencyResult>(InstanceDependencyResult.class) {

            @Override
            protected InstanceDependencyResult doExecute(String colo) throws FalconException {
                return getInstanceManager(colo).invoke("instanceDependencies",
                        entityType, entityName, instanceTimeStr, colo);
            }

        }.execute(colo, entityType, entityName);
    }

    /**
     *
     * @param entityType type of the entity. Only feed and process are valid entity types for triage.
     * @param entityName name of the entity.
     * @param instanceTime time of the instance which should be used to triage.
     * @param colo Colo on which the query should be run.
     * @return It returns a json graph
     */
    @GET
    @Path("triage/{type}/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    @Monitored(event = "triage-instance")
    @Override
    public TriageResult triageInstance(
            @Dimension("type") @PathParam("type") final String entityType,
            @Dimension("name") @PathParam("name") final String entityName,
            @Dimension("instanceTime") @QueryParam("start") final String instanceTime,
            @Dimension("colo") @QueryParam("colo") String colo) {
        return new InstanceProxy<TriageResult>(TriageResult.class) {
            @Override
            protected TriageResult doExecute(String colo) throws FalconException {
                return getInstanceManager(colo).invoke("triageInstance", entityType, entityName, instanceTime, colo);
            }
        }.execute(colo, entityType, entityName);
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
                } catch (FalconWebException e){
                    APIResult result = (APIResult)e.getResponse().getEntity();
                    results.put(colo, getResultInstance(APIResult.Status.FAILED, result.getMessage()));
                } catch (Throwable e) {
                    LOG.error("Failed to fetch results for colo:{}", colo, e);
                    results.put(colo, getResultInstance(APIResult.Status.FAILED,
                            e.getClass().getName() + "::" + e.getMessage()));
                }
            }
            T finalResult = consolidateResult(results, clazz);
            if (finalResult.getStatus() != APIResult.Status.SUCCEEDED) {
                throw FalconWebException.newAPIException(finalResult.getMessage());
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
