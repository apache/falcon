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

/**
* FalconCLI Constants.
*/
public final class FalconCLIConstants {
    private FalconCLIConstants(){

    }
    public static final String ENV_FALCON_DEBUG = "FALCON_DEBUG";
    public static final String DEFINITION_OPT = "definition";
    public static final String LOOKUP_OPT = "lookup";
    public static final String SLA_MISS_ALERT_OPT = "slaAlert";
    public static final String TOUCH_OPT = "touch";
    public static final String ADMIN_CMD = "admin";
    public static final String HELP_CMD = "help";
    public static final String METADATA_CMD = "metadata";
    public static final String ENTITY_CMD = "entity";
    public static final String INSTANCE_CMD = "instance";
    public static final String EXTENSION_CMD = "extension";
    public static final String SAFE_MODE_OPT = "setsafemode";
    public static final String VERSION_OPT = "version";
    public static final String SUBMIT_OPT = "submit";
    public static final String SUBMIT_ONLY_OPT = "submitOnly";
    public static final String UPDATE_OPT = "update";
    public static final String UPDATE_CLUSTER_DEPENDENTS_OPT = "updateClusterDependents";
    public static final String DELETE_OPT = "delete";
    public static final String ENABLE_OPT = "enable";
    public static final String DISABLE_OPT = "disable";
    public static final String SCHEDULE_OPT = "schedule";
    public static final String CURRENT_COLO = "current.colo";
    public static final String CLIENT_PROPERTIES = "/client.properties";
    public static final String RELATIONS_OPT = "relations";
    public static final String PIPELINE_OPT = "pipeline";
    public static final String NAME_OPT = "name";
    public static final String VERSION_OPT_DESCRIPTION = "show Falcon server build version";
    public static final String STACK_OPTION_DESCRIPTION = "show the thread stack dump";
    public static final String FALCON_URL = "FALCON_URL";
    public static final String STACK_OPTION = "stack";
    public static final String SUBMIT_OPT_DESCRIPTION = "Submits an entity xml to Falcon";
    public static final String UPDATE_OPT_DESCRIPTION = "Updates an existing entity";
    public static final String DELETE_OPT_DESCRIPTION = "Deletes an entity in Falcon, and kills its instance from "
            + "workflow engine";
    public static final String SUBMIT_AND_SCHEDULE_OPT = "submitAndSchedule";
    public static final String SUBMIT_AND_SCHEDULE_OPT_DESCRIPTION = "Submits an entity to Falcon and "
            + "schedules it immediately";
    public static final String VALIDATE_OPT = "validate";
    public static final String VALIDATE_OPT_DESCRIPTION = "Validates an entity based on the entity type";
    public static final String DEFINITION_OPT_DESCRIPTION = "Gets the Definition of entity";
    public static final String SLA_MISS_ALERT_OPT_DESCRIPTION = "Get missing feed instances which missed SLA";


    public static final String LOOKUP_OPT_DESCRIPTION = "Lookup a feed given its instance's path";
    public static final String PATH_OPT = "path";
    public static final String PATH_OPT_DESCRIPTION = "Path for a feed's instance";
    public static final String TOUCH_OPT_DESCRIPTION = "Force update the entity in workflow engine"
            + "(even without any changes to entity)";
    public static final String PROPS_OPT = "properties";
    public static final String PROPS_OPT_DESCRIPTION = "User supplied comma separated key value properties";
    public static final String FIELDS_OPT = "fields";
    public static final String FIELDS_OPT_DESCRIPTION = "Entity fields to show for a request";
    public static final String TAGS_OPT = "tags";
    public static final String TAGS_OPT_DESCRIPTION = "Filter returned entities by the specified tags";
    public static final String NUM_INSTANCES_OPT = "numInstances";
    public static final String NUM_INSTANCES_OPT_DESCRIPTION = "Number of instances to return per entity "
            + "summary request";
    public static final String NAMESEQ_OPT = "nameseq";
    public static final String NAMESEQ_OPT_DESCRIPTION = "Subsequence of entity name";
    public static final String TAGKEYS_OPT = "tagkeys";
    public static final String TAGKEYS_OPT_DESCRIPTION = "Keywords in tags";
    public static final String SHOWSCHEDULER_OPT = "showScheduler";
    public static final String SHOWSCHEDULER_OPT_DESCRIPTION = "To return the scheduler "
            + "on which the entity is scheduled.";

    public static final String DEBUG_OPTION = "debug";
    public static final String URL_OPTION = "url";
    public static final String TYPE_OPT = "type";
    public static final String COLO_OPT = "colo";
    public static final String CLUSTER_OPT = "cluster";
    public static final String FEED_OPT = "feed";
    public static final String PROCESS_OPT = "process";
    public static final String ENTITY_NAME_OPT = "name";
    public static final String FILE_PATH_OPT = "file";
    public static final String SUSPEND_OPT = "suspend";
    public static final String RESUME_OPT = "resume";
    public static final String STATUS_OPT = "status";
    public static final String SUMMARY_OPT = "summary";
    public static final String DEPENDENCY_OPT = "dependency";
    public static final String SKIPDRYRUN_OPT = "skipDryRun";
    public static final String FILTER_BY_OPT = "filterBy";
    public static final String ORDER_BY_OPT = "orderBy";
    public static final String SORT_ORDER_OPT = "sortOrder";
    public static final String OFFSET_OPT = "offset";
    public static final String NUM_RESULTS_OPT = "numResults";
    public static final String START_OPT = "start";
    public static final String END_OPT = "end";
    public static final String DO_AS_OPT = "doAs";
    public static final String RUNNING_OPT_DESCRIPTION = "Gets running process instances for a given process";
    public static final String LIST_OPT_DESCRIPTION = "Gets all instances for a given entity in the range start "
            + "time and optional end time";
    public static final String STATUS_OPT_DESCRIPTION = "Gets status of process instances for a given process in"
            + " the range start time and optional end time";
    public static final String SUMMARY_OPT_DESCRIPTION = "Gets summary of instances for a given process in the"
            + " range start time and optional end time";
    public static final String KILL_OPT_DESCRIPTION = "Kills active process instances for a given process in the"
            + " range start time and optional end time";
    public static final String SUSPEND_OPT_DESCRIPTION = "Suspends active process instances for a given process in"
            + " the range start time and optional end time";
    public static final String RESUME_OPT_DESCRIPTION = "Resumes suspended process instances for a given"
            + " process in the range start time and optional end time";
    public static final String RERUN_OPT_DESCRIPTION = "Reruns process instances for a given process in the"
            + " range start time and optional end time and overrides properties present in job.properties file";
    public static final String LOG_OPT_DESCRIPTION = "Logs print the logs for process instances for a given"
            + " process in the range start time and optional end time";
    public static final String PARARMS_OPT_DESCRIPTION = "Displays the workflow parameters for a given instance"
            + " of specified nominal time start time represents nominal time and end time is not considered";
    public static final String LISTING_OPT_DESCRIPTION = "Displays feed listing and their status between a"
            + " start and end time range.";
    public static final String DEPENDENCY_OPT_DESCRIPTION = "Displays dependent instances for a specified"
            + " instance.";
    public static final String TRIAGE_OPT_DESCRIPTION = "Triage a feed or process instance and find the failures"
            + " in it's lineage.";
    public static final String URL_OPTION_DESCRIPTION = "Falcon URL";
    public static final String START_OPT_DESCRIPTION = "Start time is required for commands, status, kill, "
            + "suspend, resume and re-runand it is nominal time while displaying workflow params";
    public static final String END_OPT_DESCRIPTION = "End time is optional for commands, status, kill, suspend, "
            + "resume and re-run; if not specified then current time is considered as end time";
    public static final String RUNID_OPT_DESCRIPTION = "Instance runid  is optional and user can specify the "
            + "runid, defaults to 0";
    public static final String CLUSTERS_OPT_DESCRIPTION = "clusters is optional for commands kill, suspend and "
            + "resume, should not be specified for other commands";
    public static final String SOURCECLUSTER_OPT_DESCRIPTION = " source cluster is optional for commands kill, "
            + "suspend and resume, should not be specified for other commands (required for only feed)";
    public static final String FILE_PATH_OPT_DESCRIPTION = "Path to job.properties file is required for rerun "
            + "command, it should contain name=value pair for properties to override for rerun";
    public static final String TYPE_OPT_DESCRIPTION = "Entity type, can be feed or process xml";
    public static final String ENTITY_NAME_OPT_DESCRIPTION = "Entity name, can be feed or process name";
    public static final String COLO_OPT_DESCRIPTION = "Colo on which the cmd has to be executed";
    public static final String LIFECYCLE_OPT_DESCRIPTION = "describes life cycle of entity , for feed it can be "
            + "replication/retention and for process it can be execution";
    public static final String FILTER_BY_OPT_DESCRIPTION = "Filter returned instances by the specified fields";
    public static final String ORDER_BY_OPT_DESCRIPTION = "Order returned instances by this field";
    public static final String SORT_ORDER_OPT_DESCRIPTION = "asc or desc order for results";
    public static final String OFFSET_OPT_DESCRIPTION = "Start returning instances from this offset";
    public static final String FORCE_RERUN_FLAG_DESCRIPTION = "Flag to forcefully rerun entire workflow "
            + "of an instance";
    public static final String DO_AS_OPT_DESCRIPTION = "doAs user";
    public static final String INSTANCE_TIME_OPT_DESCRIPTION = "Time for an instance";
    public static final String ALL_ATTEMPTS_DESCRIPTION = "To get all attempts of corresponding instances";
    public static final String FORCE_RERUN_FLAG = "force";
    public static final String INSTANCE_TIME_OPT = "instanceTime";
    public static final String RUNNING_OPT = "running";
    public static final String KILL_OPT = "kill";
    public static final String RERUN_OPT = "rerun";
    public static final String LOG_OPT = "logs";
    public static final String CLUSTERS_OPT = "clusters";
    public static final String SOURCECLUSTER_OPT = "sourceClusters";
    public static final String LIFECYCLE_OPT = "lifecycle";
    public static final String PARARMS_OPT = "params";
    public static final String LISTING_OPT = "listing";
    public static final String TRIAGE_OPT = "triage";
    public static final String SKIPDRYRUN_OPT_DESCRIPTION = "skip dry run in workflow engine";
    public static final String SCHEDULE_OPT_DESCRIPTION = "Schedules a submited entity in Falcon";
    public static final String ALL_ATTEMPTS = "allAttempts";
    public static final String RUNID_OPT = "runid";
    public static final String INSTANCE_STATUS_OPT = "instanceStatus";
    public static final String SEARCH_OPT = "search";


    // Discovery Commands
    public static final String DISCOVERY_OPT = "discovery";
    public static final String LIST_OPT = "list";

    // Lineage Commands
    public static final String LINEAGE_OPT = "lineage";
    public static final String VERTEX_CMD = "vertex";
    public static final String VERTICES_CMD = "vertices";
    public static final String VERTEX_EDGES_CMD = "edges";
    public static final String EDGE_CMD = "edge";
    public static final String ID_OPT = "id";
    public static final String KEY_OPT = "key";
    public static final String VALUE_OPT = "value";
    public static final String DIRECTION_OPT = "direction";

    public static final String DISCOVERY_OPT_DESCRIPTION = "Discover falcon metadata relations";
    public static final String LINEAGE_OPT_DESCRIPTION = "Get falcon metadata lineage information";
    public static final String PIPELINE_OPT_DESCRIPTION = "Get lineage graph for the entities in a pipeline";
    public static final String RELATIONS_OPT_DESCRIPTION = "List all relations for a dimension";
    public static final String NAME_OPT_DESCRIPTION = "Dimension name";
    public static final String CLUSTER_OPT_DESCRIPTION = "Cluster name";
    public static final String FEED_OPT_DESCRIPTION = "Feed Entity name";
    public static final String PROCESS_OPT_DESCRIPTION = "Process Entity name";
    public static final String NUM_RESULTS_OPT_DESCRIPTION = "Number of results to return per request";
    public static final String VERTEX_CMD_DESCRIPTION = "show the vertices";
    public static final String VERTICES_CMD_DESCRIPTION = "show the vertices";
    public static final String VERTEX_EDGES_CMD_DESCRIPTION = "show the edges for a given vertex";
    public static final String EDGE_CMD_DESCRIPTION = "show the edges";
    public static final String ID_OPT_DESCRIPTION = "vertex or edge id";
    public static final String KEY_OPT_DESCRIPTION = "key property";
    public static final String VALUE_OPT_DESCRIPTION = "value property";
    public static final String DIRECTION_OPT_DESCRIPTION = "edge direction property";
    public static final String DEBUG_OPTION_DESCRIPTION = "Use debug mode to see debugging statements on stdout";
    public static final String DO_AS_DESCRIPTION = "doAs user";
    public static final String UREGISTER = "unregister";
    public static final String DETAIL = "detail";
    public static final String REGISTER = "register";
    public static final String PATH = "path";
    public static final String DESCRIPTION = "description";
}
