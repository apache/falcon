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

package org.apache.falcon;

/**
* FalconCLI Constants.
*/
public final class FalconCLIConstants {
    private FalconCLIConstants(){

    }
    public static final String ENV_FALCON_DEBUG = "FALCON_DEBUG";
    public static final String DEBUG_OPTION = "debug";
    public static final String URL_OPTION = "url";

    public static final String ADMIN_CMD = "admin";
    public static final String HELP_CMD = "help";
    public static final String METADATA_CMD = "metadata";
    public static final String ENTITY_CMD = "entity";
    public static final String INSTANCE_CMD = "instance";
    public static final String EXTENSION_CMD = "extension";

    public static final String TYPE_OPT = "type";
    public static final String COLO_OPT = "colo";
    public static final String CLUSTER_OPT = "cluster";
    public static final String FEED_OPT = "feed";
    public static final String PROCESS_OPT = "process";
    public static final String ENTITY_NAME_OPT = "name";
    public static final String FILE_PATH_OPT = "file";
    public static final String VERSION_OPT = "version";
    public static final String SCHEDULE_OPT = "schedule";
    public static final String SUSPEND_OPT = "suspend";
    public static final String RESUME_OPT = "resume";
    public static final String STATUS_OPT = "status";
    public static final String SUMMARY_OPT = "summary";
    public static final String DEPENDENCY_OPT = "dependency";
    public static final String LIST_OPT = "list";
    public static final String SKIPDRYRUN_OPT = "skipDryRun";
    public static final String FILTER_BY_OPT = "filterBy";
    public static final String ORDER_BY_OPT = "orderBy";
    public static final String SORT_ORDER_OPT = "sortOrder";
    public static final String OFFSET_OPT = "offset";
    public static final String NUM_RESULTS_OPT = "numResults";
    public static final String START_OPT = "start";
    public static final String END_OPT = "end";
    public static final String CURRENT_COLO = "current.colo";
    public static final String CLIENT_PROPERTIES = "/client.properties";
    public static final String DO_AS_OPT = "doAs";
    public static final String RELATIONS_OPT = "relations";
    public static final String PIPELINE_OPT = "pipeline";
    public static final String NAME_OPT = "name";
}
