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
package org.apache.falcon.client;

import org.apache.falcon.LifeCycle;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.EntityList;
import org.apache.falcon.resource.EntitySummaryResult;
import org.apache.falcon.resource.ExtensionJobList;
import org.apache.falcon.resource.FeedInstanceResult;
import org.apache.falcon.resource.FeedLookupResult;
import org.apache.falcon.resource.InstanceDependencyResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesSummaryResult;
import org.apache.falcon.resource.LineageGraphResult;
import org.apache.falcon.resource.SchedulableEntityInstanceResult;
import org.apache.falcon.resource.TriageResult;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Abstract Client API to submit and manage Falcon Entities (Cluster, Feed, Process) jobs
 * against an Falcon instance.
 */
public abstract class AbstractFalconClient {

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck

    protected static final String FALCON_INSTANCE_ACTION_CLUSTERS = "falcon.instance.action.clusters";
    protected static final String FALCON_INSTANCE_SOURCE_CLUSTERS = "falcon.instance.source.clusters";

    /**
     * Submit a new entity. Entities can be of type feed, process or data end
     * points. Entity definitions are validated structurally against schema and
     * subsequently for other rules before they are admitted into the system.
     * @param entityType Entity type. Valid options are cluster, feed or process.
     * @param filePath Path for the entity definition
     * @return
     * @throws IOException
     */
    public abstract APIResult submit(String entityType, String filePath, String doAsUser);

    /**
     * Schedules an submitted process entity immediately.
     * @param entityType Entity type. Valid options are cluster, feed or process.
     * @param entityName Name of the entity.
     * @param colo Cluster name.
     * @return
     */
    public abstract APIResult schedule(EntityType entityType, String entityName, String colo, Boolean skipDryRun,
                                       String doAsuser, String properties);

    /**
     * Delete the specified entity.
     * @param entityType Entity type. Valid options are cluster, feed or process.
     * @param entityName Name of the entity.
     * @param doAsUser Proxy User.
     * @return
     */
    public abstract APIResult delete(EntityType entityType, String entityName,
                                     String doAsUser);

    /**
     * Validates the submitted entity.
     * @param entityType Entity type. Valid options are cluster, feed or process.
     * @param filePath Path for the entity definition to validate.
     * @param skipDryRun Dry run.
     * @param doAsUser Proxy User.
     * @return
    \     */
    public abstract APIResult validate(String entityType, String filePath, Boolean skipDryRun,
                                       String doAsUser);

    /**
     * Updates the submitted entity.
     * @param entityType Entity type. Valid options are cluster, feed or process.
     * @param entityName Name of the entity.
     * @param filePath Path for the entity definition to update.
     * @param skipDryRun Dry run.
     * @param doAsUser Proxy User.
     * @return
     */
    public abstract APIResult update(String entityType, String entityName, String filePath,
                                     Boolean skipDryRun, String doAsUser);

    /**
     * Get definition of the entity.
     * @param entityType Entity type. Valid options are cluster, feed or process.
     * @param entityName Name of the entity.
     * @param doAsUser Proxy user.
     * @return
     */
    public abstract Entity getDefinition(String entityType, String entityName,
                                         String doAsUser);


    /**
     *
     * @param type entity type
     * @param entity entity name
     * @param start start time
     * @param end end time
     * @param colo colo name
     * @param lifeCycles lifecycle of an entity (for ex : feed has replication,eviction).
     * @param filterBy filter operation can be applied to results
     * @param orderBy
     * @param sortOrder sort order can be asc or desc
     * @param offset offset while displaying results
     * @param numResults num of Results to output
     * @param doAsUser proxy user
     * @param allAttempts To get the instances corresponding to each run-id
     * @return
     */
    public abstract InstancesResult getStatusOfInstances(String type, String entity, String start, String end, String
        colo, List<LifeCycle> lifeCycles, String filterBy, String orderBy, String sortOrder, Integer offset, Integer
                                                             numResults, String doAsUser, Boolean allAttempts);

    /**
     * Suspend an entity.
     * @param entityType Valid options are feed or process.
     * @param entityName Name of the entity.
     * @param colo Colo on which the query should be run.
     * @param doAsUser proxy user
     * @return Status of the entity.
     */
    public abstract APIResult suspend(EntityType entityType, String entityName, String colo, String doAsUser);

    /**
     * Resume a supended entity.
     * @param entityType Valid options are feed or process.
     * @param entityName Name of the entity.
     * @param colo Colo on which the query should be run.
     * @param doAsUser proxy user
     * @return Result of the resume command.
     */
    public abstract APIResult resume(EntityType entityType, String entityName, String colo, String doAsUser);

    /**
     * Get status of the entity.
     * @param entityType Valid options are feed or process.
     * @param entityName Name of the entity.
     * @param colo Colo on which the query should be run.
     * @param doAsUser proxy user
     * @param showScheduler whether the call should return the scheduler on which the entity is scheduled.
     * @return Status of the entity.
     */
    public abstract APIResult getStatus(EntityType entityType, String entityName, String colo, String doAsUser,
                                        boolean showScheduler);

    /**
     * Submits and schedules an entity.
     * @param entityType Valid options are feed or process.
     * @param filePath Path for the entity definition
     * @param skipDryRun Optional query param, Falcon skips oozie dryrun when value is set to true.
     * @param doAsUser proxy user
     * @return Result of the submit and schedule command.
     */
    public abstract APIResult submitAndSchedule(String entityType, String filePath, Boolean skipDryRun, String doAsUser,
                                                String properties);

    /**
     * Registers an extension.
     * @param extensionName extensionName of the extension.
     * @param packagePath Package location for the extension.
     * @param description description of the extension.
     * @return Result of the registerExtension command.
     */
    public abstract APIResult registerExtension(String extensionName, String packagePath, String description);

    /**
     *
     * @param extensionName extensionName that needs to be unregistered
     * @return Result of the unregisterExtension operation
     */
    public abstract APIResult unregisterExtension(String extensionName);

    /**
     *
     * @param extensionName extensionName that needs to be enabled
     * @return Result of the enableExtension operation
     */
    public abstract APIResult enableExtension(String extensionName);

    /**
     *
     * @param extensionName extensionName that needs to be disabled
     * @return Result of the disableExtension operation
     */
    public abstract APIResult disableExtension(String extensionName);

    /**
     * Prepares set of entities the extension has implemented and stage them to a local directory and submit them too.
     * @param extensionName extension which is available in the store.
     * @param jobName name to be used in all the extension entities' tagging that are built as part of
     *                           loadAndPrepare.
     * @param configPath path to extension parameters.
     * @return
     */
    public abstract APIResult submitExtensionJob(String extensionName, String jobName, String configPath,
                                                String doAsUser);

    /**
     * Schedules the set of entities that are part of the extension.
     * @param jobName extensionJob that needs to be scheduled.
     * @return APIResult stating status of scheduling the extension.
     */
    public abstract APIResult scheduleExtensionJob(String jobName, String coloExpr, String doAsUser);

    /**
     * Prepares set of entities the extension has implemented and stage them to a local directory and submits and
     * schedules them.
     * @param extensionName extension which is available in the store.
     * @param jobName name to be used in all the extension entities' tagging that are built as part of
     *                           loadAndPrepare.
     * @param configPath path to extension parameters.
     * @return
     */
    public abstract APIResult submitAndScheduleExtensionJob(String extensionName, String jobName, String configPath,
                                                 String doAsUser);

    /**
     * Prepares set of entities the extension has implemented and stage them to a local directory and updates them.
     * @param jobName name to be used in all the extension entities' tagging that are built as part of
     *                           loadAndPrepare.
     * @param configPath path to extension parameters.
     * @return
     */
    public abstract APIResult updateExtensionJob(String jobName, String configPath, String doAsUser);

    /**
     * Deletes the entities that are part of the extension job and then deleted the job from the DB.
     * @param jobName name of the extension job that needs to be deleted.
     * @return APIResult status of the deletion query.
     */
    public abstract APIResult deleteExtensionJob(final String jobName, final String doAsUser);

    /**
     *
     * @param jobName name of the extension that has to be suspended.
     * @param coloExpr comma separated list of colos where the operation has to be performed.
     * @param doAsUser proxy user
     * @return result status of the suspend operation.
     */
    public abstract APIResult suspendExtensionJob(final String jobName, final String coloExpr, final String doAsUser);

    /**
     *
     * @param jobName name of the extension that has to be resumed.
     * @param coloExpr comma separated list of colos where the operation has to be performed.
     * @param doAsUser proxy user.
     * @return result status of the resume operation.
     */
    public abstract APIResult resumeExtensionJob(final String jobName, final String coloExpr, final String doAsUser);

    /**
     *  Prepares set of entities the extension has implemented to validate the extension job.
     * @param jobName job name of the extension job.
     * @return
     */
    public abstract APIResult getExtensionJobDetails(final String jobName);

    /**
     * Returns details of an extension.
     * @param extensionName name of the extension.
     * @return
     */
    public abstract APIResult getExtensionDetail(final String extensionName);

    /**
     * Returns all registered extensions.
     * @return
     */
    public abstract APIResult enumerateExtensions();

    /**
     * Returns all registered jobs for an extension.
     * @return
     */
    public abstract ExtensionJobList getExtensionJobs(String extensionName, String sortOrder, String doAsUser);

    /**
     *
     * Get list of the entities.
     * We have two filtering parameters for entity tags: "tags" and "tagkeys".
     * "tags" does the exact match in key=value fashion, while "tagkeys" finds all the entities with the given key as a
     * substring in the tags. This "tagkeys" filter is introduced for the user who doesn't remember the exact tag but
     * some keywords in the tag. It also helps users to save the time of typing long tags.
     * The returned entities will match all the filtering criteria.
     * @param entityType Comma-separated entity types. Can be empty. Valid entity types are cluster, feed or process.
     * @param fields <optional param> Fields of entity that the user wants to view, separated by commas.
     *               Valid options are STATUS, TAGS, PIPELINES, CLUSTERS.
     * @param nameSubsequence <optional param> Subsequence of entity name. Not case sensitive.
     *                        The entity name needs to contain all the characters in the subsequence in the same order.
     *                        Example 1: "sample1" will match the entity named "SampleFeed1-2".
     *                        Example 2: "mhs" will match the entity named "New-My-Hourly-Summary".
     * @param tagKeywords <optional param> Keywords in tags, separated by comma. Not case sensitive.
     *                    The returned entities will have tags that match all the tag keywords.
     * @param filterTags <optional param> Return list of entities that have specified tags, separated by a comma.
     *             Query will do AND on tag values.
     *             Example: tags=consumer=consumer@xyz.com,owner=producer@xyz.com
     * @param filterBy <optional param> Filter results by list of field:value pairs.
     *                 Example: filterBy=STATUS:RUNNING,PIPELINES:clickLogs
     *                 Supported filter fields are NAME, STATUS, PIPELINES, CLUSTER.
     *                 Query will do an AND among filterBy fields.
     * @param orderBy <optional param> Field by which results should be ordered.
     *                Supports ordering by "name".
     * @param sortOrder <optional param> Valid options are "asc" and "desc"
     * @param offset <optional param> Show results from the offset, used for pagination. Defaults to 0.
     * @param numResults <optional param> Number of results to show per request, used for pagination. Only
     *                       integers > 0 are valid, Default is 10.
     * @param doAsUser proxy user
     * @return Total number of results and a list of entities.
     */
    public abstract EntityList getEntityList(String entityType, String fields, String nameSubsequence, String
        tagKeywords, String filterBy, String filterTags, String orderBy, String sortOrder, Integer offset, Integer
                                                 numResults, String doAsUser);

    /**
     * Given an EntityType and cluster, get list of entities along with summary of N recent instances of each entity.
     * @param entityType Valid options are feed or process.
     * @param cluster Show entities that belong to this cluster.
     * @param start <optional param> Show entity summaries from this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
     *                 By default, it is set to (end - 2 days).
     * @param end <optional param> Show entity summary up to this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
     *               Default is set to now.
     * @param fields <optional param> Fields of entity that the user wants to view, separated by commas.
     *                     Valid options are STATUS, TAGS, PIPELINES.
     * @param filterBy <optional param> Filter results by list of field:value pairs.
     *                     Example: filterBy=STATUS:RUNNING,PIPELINES:clickLogs
     *                     Supported filter fields are NAME, STATUS, PIPELINES, CLUSTER.
     *                     Query will do an AND among filterBy fields.
     * @param filterTags <optional param> Return list of entities that have specified tags, separated by a comma.
     *                   Query will do AND on tag values.
     *                   Example: tags=consumer=consumer@xyz.com,owner=producer@xyz.com
     * @param orderBy <optional param> Field by which results should be ordered.
     *                      Supports ordering by "name".
     * @param sortOrder <optional param> Valid options are "asc" and "desc"
     * @param offset <optional param> Show results from the offset, used for pagination. Defaults to 0.
     * @param numInstances <optional param> Number of results to show per request, used for pagination. Only
     *                    integers > 0 are valid, Default is 10.
     * @param numResults <optional param> Number of recent instances to show per entity. Only integers > 0 are
     *                           valid, Default is 7.
     * @param doAsUser proxy user
     * @return Show entities along with summary of N instances for each entity.
     */
    public abstract EntitySummaryResult getEntitySummary(String entityType, String cluster, String start, String end,
                                                         String fields, String filterBy, String filterTags, String
                                                             orderBy, String sortOrder, Integer offset, Integer
                                                             numResults, Integer numInstances, String doAsUser);

    /**
     * Force updates the entity.
     * @param entityType Valid options are feed or process.
     * @param entityName Name of the entity.
     * @param colo Colo on which the query should be run.
     * @param skipDryRun Optional query param, Falcon skips oozie dryrun when value is set to true.
     * @param doAsUser proxy user
     * @return Result of the validation.
     */
    public abstract APIResult touch(String entityType, String entityName, String colo, Boolean skipDryRun,
                                    String doAsUser);

    /**
     * Kill currently running instance(s) of an entity.
     * @param type Valid options are feed or process.
     * @param entity name of the entity.
     * @param start start time of the instance(s) that you want to refer to
     * @param end end time of the instance(s) that you want to refer to
     * @param colo Colo on which the query should be run.
     * @param lifeCycles <optional param> can be Eviction/Replication(default) for feed and Execution(default) for
     *                   process.
     * @param doAsUser proxy user
     * @return Result of the kill operation.
     * @throws UnsupportedEncodingException
     */
    public abstract InstancesResult killInstances(String type, String entity, String start, String end, String colo,
                                                  String clusters, String sourceClusters, List<LifeCycle> lifeCycles,
                                                  String doAsUser) throws UnsupportedEncodingException;

    /**
     * Suspend instances of an entity.
     * @param type Valid options are feed or process.
     * @param entity name of the entity.
     * @param start the start time of the instance(s) that you want to refer to
     * @param end the end time of the instance(s) that you want to refer to
     * @param colo Colo on which the query should be run.
     * @param lifeCycles <optional param> can be Eviction/Replication(default) for feed and Execution(default) for
     *                   process.
     * @param doAsUser proxy user
     * @return Results of the suspend command.
     * @throws UnsupportedEncodingException
     */
    public abstract InstancesResult suspendInstances(String type, String entity, String start, String end, String colo,
                                                     String clusters, String sourceClusters, List<LifeCycle> lifeCycles,
                                                     String doAsUser) throws UnsupportedEncodingException;

    /**
     * Resume suspended instances of an entity.
     * @param type Valid options are feed or process.
     * @param entity name of the entity.
     * @param start start time of the instance(s) that you want to refer to
     * @param end the end time of the instance(s) that you want to refer to
     * @param colo Colo on which the query should be run.
     * @param lifeCycles <optional param> can be Eviction/Replication(default) for feed and Execution(default) for
     *                   process.
     * @param doAsUser proxy user
     * @return Results of the resume command.
     * @throws UnsupportedEncodingException
     */
    public abstract InstancesResult resumeInstances(String type, String entity, String start, String end, String colo,
                                                    String clusters, String sourceClusters, List<LifeCycle> lifeCycles,
                                                    String doAsUser) throws UnsupportedEncodingException;

    /**
     * Rerun instances of an entity. On issuing a rerun, by default the execution resumes from the last failed node in
     * the workflow.
     * @param type Valid options are feed or process.
     * @param entity name of the entity.
     * @param start start is the start time of the instance that you want to refer to
     * @param end end is the end time of the instance that you want to refer to
     * @param colo Colo on which the query should be run.
     * @param lifeCycles <optional param> can be Eviction/Replication(default) for feed and Execution(default) for
     *                   process.
     * @param isForced <optional param> can be used to forcefully rerun the entire instance.
     * @param doAsUser proxy user
     * @return Results of the rerun command.
     * @throws IOException
     */
    public abstract InstancesResult rerunInstances(String type, String entity, String start, String end,
                                                   String filePath, String colo, String clusters,
                                                   String sourceClusters, List<LifeCycle> lifeCycles, Boolean isForced,
                                                   String doAsUser) throws IOException;

    /**
     * Get summary of instance/instances of an entity.
     * @param type Valid options are cluster, feed or process.
     * @param entity Name of the entity.
     * @param start <optional param> Show instances from this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
     *                 By default, it is set to (end - (10 * entityFrequency)).
     * @param end <optional param> Show instances up to this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
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
     * @param doAsUser proxy user
     * @return Summary of the instances over the specified time range
     */
    public abstract InstancesSummaryResult getSummaryOfInstances(String type, String entity, String start, String end,
                                                                 String colo, List<LifeCycle> lifeCycles,
                                                                 String filterBy, String orderBy, String sortOrder,
                                                                 String doAsUser);

    /**
     * Get falcon feed instance availability.
     * @param type Valid options is feed.
     * @param entity Name of the entity.
     * @param start <optional param> Show instances from this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
     *              By default, it is set to (end - (10 * entityFrequency)).
     * @param end <optional param> Show instances up to this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
     *            Default is set to now.
     * @param colo Colo on which the query should be run.
     * @param doAsUser proxy user
     * @return Feed instance availability status
     */
    public abstract FeedInstanceResult getFeedListing(String type, String entity, String start, String end, String colo,
                                                      String doAsUser);

    /**
     * Get log of a specific instance of an entity.
     * @param type Valid options are cluster, feed or process.
     * @param entity Name of the entity.
     * @param start <optional param> Show instances from this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
     *                 By default, it is set to (end - (10 * entityFrequency)).
     * @param end <optional param> Show instances up to this date. Date format is yyyy-MM-dd'T'HH:mm'Z'.
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
     * @param doAsUser proxy user
     * @return Log of specified instance.
     */
    public abstract InstancesResult getLogsOfInstances(String type, String entity, String start, String end,
                                                       String colo, String runId, List<LifeCycle> lifeCycles,
                                                       String filterBy, String orderBy, String sortOrder,
                                                       Integer offset, Integer numResults, String doAsUser);

    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    /**
     * Get the params passed to the workflow for an instance of feed/process.
     * @param type Valid options are cluster, feed or process.
     * @param entity Name of the entity.
     * @param start should be the nominal time of the instance for which you want the params to be returned
     * @param colo <optional param> Colo on which the query should be run.
     * @param lifeCycles <optional param> Valid lifecycles for feed are Eviction/Replication(default) and for process is
     *                   Execution(default).
     * @param doAsUser proxy user
     * @return List of instances currently running.
     * @throws UnsupportedEncodingException
     */
    public abstract InstancesResult getParamsOfInstance(String type, String entity, String start, String colo,
                                                        List<LifeCycle> lifeCycles, String doAsUser) throws
        UnsupportedEncodingException;

    /**
     * Get dependent instances for a particular instance.
     * @param entityType Valid options are feed or process.
     * @param entityName Name of the entity
     * @param instanceTime <mandatory param> time of the given instance
     * @param colo Colo on which the query should be run.
     * @return Dependent instances for the specified instance
     */
    public abstract InstanceDependencyResult getInstanceDependencies(String entityType, String entityName,
                                                                     String instanceTime, String colo);

    /**
     * Get version of the falcon server.
     * @return Version of the server.
     */
    public abstract String getVersion(String doAsUser);

    protected InputStream getServletInputStream(String clusters, String sourceClusters, String properties) throws
        UnsupportedEncodingException {

        InputStream stream;
        StringBuilder buffer = new StringBuilder();
        if (clusters != null) {
            buffer.append(FALCON_INSTANCE_ACTION_CLUSTERS).append('=').append(clusters).append('\n');
        }
        if (sourceClusters != null) {
            buffer.append(FALCON_INSTANCE_SOURCE_CLUSTERS).append('=').append(sourceClusters).append('\n');
        }
        if (properties != null) {
            buffer.append(properties);
        }
        stream = new ByteArrayInputStream(buffer.toString().getBytes());
        return (buffer.length() == 0) ? null : stream;
    }

    /**
     * Converts a InputStream into ServletInputStream.
     *
     * @param filePath - Path of file to stream
     * @return ServletInputStream
     */
    protected InputStream getServletInputStream(String filePath) {

        if (filePath == null) {
            return null;
        }
        InputStream stream;
        try {
            stream = new FileInputStream(filePath);
        } catch (FileNotFoundException e) {
            throw new FalconCLIException("File not found:" + filePath, e);
        }
        return stream;
    }

    public abstract SchedulableEntityInstanceResult getFeedSlaMissPendingAlerts(String entityType, String entityName,
                                                                                String start, String end, String colo);

    public abstract FeedLookupResult reverseLookUp(String entityType, String path, String doAs);

    public abstract EntityList getDependency(String entityType, String entityName, String doAs);

    public abstract TriageResult triage(String name, String entityName, String start, String colo);
    // SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    public abstract InstancesResult getRunningInstances(String type, String entity, String colo,
                                                        List<LifeCycle> lifeCycles,
                                                        String filterBy, String orderBy, String sortOrder,
                                                        Integer offset, Integer numResults, String doAsUser);
    // RESUME CHECKSTYLE CHECK ParameterNumberCheck
    public abstract FeedInstanceResult getFeedInstanceListing(String type, String entity, String start, String end,
                                                              String colo, String doAsUser);
    public abstract int getStatus(String doAsUser);

    public abstract String getThreadDump(String doAs);

    public abstract LineageGraphResult getEntityLineageGraph(String pipeline, String doAs);

    public abstract String getDimensionList(String dimensionType, String cluster, String doAs);

    public abstract String getReplicationMetricsDimensionList(String schedEntityType, String schedEntityName,
                                                              Integer numResults, String doAs);

    public abstract String getDimensionRelations(String dimensionType, String dimensionName, String doAs);

    public abstract String getVertex(String id, String doAs);

    public abstract String getVertices(String key, String value, String doAs);

    public abstract String getVertexEdges(String id, String direction, String doAs);

    public abstract String getEdge(String id, String doAs);
}
