/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.cli.commands;

import static org.apache.falcon.cli.FalconCLI.*;
import static org.apache.falcon.cli.FalconInstanceCLI.*;

import org.apache.falcon.ResponseHelper;
import org.apache.falcon.entity.v0.EntityType;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

/**
 * Instance commands.
 */
@Component
public class FalconInstanceCommands extends BaseFalconCommands {
    public static final String INSTANCE_PREFIX = "instance";
    public static final String INSTANCE_COMMAND_PREFIX = INSTANCE_PREFIX + " ";

    @CliCommand(value = INSTANCE_COMMAND_PREFIX + TRIAGE_OPT, help = TRIAGE_OPT_DESCRIPTION)
    public String triage(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION)
            final String entityName,
            @CliOption(key = {COLO_OPT}, mandatory = true, help = COLO_OPT_DESCRIPTION)
            final String colo,
            @CliOption(key = {START_OPT}, mandatory = false, help = START_OPT_DESCRIPTION) final String start
    ) {
        return getFalconClient().triage(entityType.name(), entityName, start, getColo(colo)).toString();
    }

    @CliCommand(value = INSTANCE_COMMAND_PREFIX + DEPENDENCY_OPT, help = DEPENDENCY_OPT_DESCRIPTION)
    public String dependency(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION)
            final String entityName,
            @CliOption(key = {COLO_OPT}, mandatory = true, help = COLO_OPT_DESCRIPTION)
            final String colo,
            @CliOption(key = {START_OPT}, mandatory = false, help = START_OPT_DESCRIPTION) final String start
    ) {
        return getFalconClient().getInstanceDependencies(entityType.name(), entityName, start, getColo(colo))
                .toString();
    }

    @CliCommand(value = INSTANCE_COMMAND_PREFIX + RUNNING_OPT, help = RUNNING_OPT_DESCRIPTION)
    public String running(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION)
            final String entityName,
            @CliOption(key = {COLO_OPT}, mandatory = true, help = COLO_OPT_DESCRIPTION)
            final String colo,
            @CliOption(key = {LIFECYCLE_OPT}, mandatory = false, help = LIFECYCLE_OPT_DESCRIPTION)
            final String lifeCycle,
            @CliOption(key = {ORDER_BY_OPT}, mandatory = false, help = ORDER_BY_OPT_DESCRIPTION) final String orderBy,
            @CliOption(key = {SORT_ORDER_OPT}, mandatory = false,
                    help = SORT_ORDER_OPT_DESCRIPTION) final String sortOrder,
            @CliOption(key = {FILTER_BY_OPT}, mandatory = false,
                    help = FILTER_BY_OPT_DESCRIPTION) final String filterBy,
            @CliOption(key = {OFFSET_OPT}, mandatory = false, help = OFFSET_OPT_DESCRIPTION) final Integer offset,
            @CliOption(key = {NUM_RESULTS_OPT}, mandatory = false,
                    help = NUM_RESULTS_OPT_DESCRIPTION) final Integer numResults
    ) {
        validateOrderBy(orderBy, INSTANCE_PREFIX);
        validateFilterBy(filterBy, INSTANCE_PREFIX);
        return ResponseHelper.getString(getFalconClient().getRunningInstances(entityType.name(),
                entityName, colo, getLifeCycle(lifeCycle), filterBy, orderBy, sortOrder, offset, numResults,
                getDoAs()));
    }

    @CliCommand(value = {INSTANCE_COMMAND_PREFIX + STATUS_OPT, INSTANCE_COMMAND_PREFIX + LIST_OPT},
            help = STATUS_OPT_DESCRIPTION)
    public String status(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION)
            final String entityName,
            @CliOption(key = {COLO_OPT}, mandatory = true, help = COLO_OPT_DESCRIPTION)
            final String colo,
            @CliOption(key = {START_OPT}, mandatory = false, help = START_OPT_DESCRIPTION) final String start,
            @CliOption(key = {END_OPT}, mandatory = false, help = END_OPT_DESCRIPTION) final String end,
            @CliOption(key = {LIFECYCLE_OPT}, mandatory = false, help = LIFECYCLE_OPT_DESCRIPTION)
            final String lifeCycle,
            @CliOption(key = {ORDER_BY_OPT}, mandatory = false, help = ORDER_BY_OPT_DESCRIPTION) final String orderBy,
            @CliOption(key = {SORT_ORDER_OPT}, mandatory = false,
                    help = SORT_ORDER_OPT_DESCRIPTION) final String sortOrder,
            @CliOption(key = {FILTER_BY_OPT}, mandatory = false,
                    help = FILTER_BY_OPT_DESCRIPTION) final String filterBy,
            @CliOption(key = {OFFSET_OPT}, mandatory = false, help = OFFSET_OPT_DESCRIPTION) final Integer offset,
            @CliOption(key = {NUM_RESULTS_OPT}, mandatory = false,
                    help = NUM_RESULTS_OPT_DESCRIPTION) final Integer numResults,
            @CliOption(key = {ALL_ATTEMPTS}, mandatory = false, specifiedDefaultValue = "true",
                    help = ALL_ATTEMPTS_DESCRIPTION) final Boolean allAttempts
    ) {
        validateOrderBy(orderBy, INSTANCE_PREFIX);
        validateFilterBy(filterBy, INSTANCE_PREFIX);
        return ResponseHelper.getString(getFalconClient().getStatusOfInstances(entityType.name(), entityName, start,
                end, getColo(colo), getLifeCycle(lifeCycle), filterBy, orderBy, sortOrder, offset, numResults,
                getDoAs(), allAttempts));
    }

    @CliCommand(value = {INSTANCE_COMMAND_PREFIX + SUMMARY_OPT},
            help = SUMMARY_OPT_DESCRIPTION)
    public String summary(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION)
            final String entityName,
            @CliOption(key = {COLO_OPT}, mandatory = true, help = COLO_OPT_DESCRIPTION)
            final String colo,
            @CliOption(key = {START_OPT}, mandatory = false, help = START_OPT_DESCRIPTION) final String start,
            @CliOption(key = {END_OPT}, mandatory = false, help = END_OPT_DESCRIPTION) final String end,
            @CliOption(key = {LIFECYCLE_OPT}, mandatory = false, help = LIFECYCLE_OPT_DESCRIPTION)
            final String lifeCycle,
            @CliOption(key = {ORDER_BY_OPT}, mandatory = false, help = ORDER_BY_OPT_DESCRIPTION) final String orderBy,
            @CliOption(key = {SORT_ORDER_OPT}, mandatory = false,
                    help = SORT_ORDER_OPT_DESCRIPTION) final String sortOrder,
            @CliOption(key = {FILTER_BY_OPT}, mandatory = false,
                    help = FILTER_BY_OPT_DESCRIPTION) final String filterBy
    ) {
        validateOrderBy(orderBy, INSTANCE_PREFIX);
        validateFilterBy(filterBy, INSTANCE_PREFIX);
        return ResponseHelper.getString(getFalconClient().getSummaryOfInstances(entityType.name(), entityName, start,
                end, getColo(colo), getLifeCycle(lifeCycle), filterBy, orderBy, sortOrder, getDoAs()));
    }

    @CliCommand(value = {INSTANCE_COMMAND_PREFIX + KILL_OPT},
            help = KILL_OPT_DESCRIPTION)
    public String kill(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION)
            final String entityName,
            @CliOption(key = {COLO_OPT}, mandatory = true, help = COLO_OPT_DESCRIPTION)
            final String colo,
            @CliOption(key = {START_OPT}, mandatory = true, help = START_OPT_DESCRIPTION) final String start,
            @CliOption(key = {END_OPT}, mandatory = true, help = END_OPT_DESCRIPTION) final String end,
            @CliOption(key = {CLUSTERS_OPT}, mandatory = false, help = CLUSTERS_OPT_DESCRIPTION) final String clusters,
            @CliOption(key = {SOURCECLUSTER_OPT}, mandatory = false, help = SOURCECLUSTER_OPT_DESCRIPTION)
            final String sourceClusters,
            @CliOption(key = {LIFECYCLE_OPT}, mandatory = false, help = LIFECYCLE_OPT_DESCRIPTION)
            final String lifeCycle
    ) throws UnsupportedEncodingException {
        return ResponseHelper.getString(getFalconClient().killInstances(entityType.name(), entityName, start,
                end, getColo(colo), clusters, sourceClusters, getLifeCycle(lifeCycle), getDoAs()));
    }

    @CliCommand(value = {INSTANCE_COMMAND_PREFIX + SUSPEND_OPT},
            help = SUSPEND_OPT_DESCRIPTION)
    public String suspend(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION)
            final String entityName,
            @CliOption(key = {COLO_OPT}, mandatory = true, help = COLO_OPT_DESCRIPTION)
            final String colo,
            @CliOption(key = {START_OPT}, mandatory = true, help = START_OPT_DESCRIPTION) final String start,
            @CliOption(key = {END_OPT}, mandatory = true, help = END_OPT_DESCRIPTION) final String end,
            @CliOption(key = {CLUSTERS_OPT}, mandatory = false, help = CLUSTERS_OPT_DESCRIPTION) final String clusters,
            @CliOption(key = {SOURCECLUSTER_OPT}, mandatory = false, help = SOURCECLUSTER_OPT_DESCRIPTION)
            final String sourceClusters,
            @CliOption(key = {LIFECYCLE_OPT}, mandatory = false, help = LIFECYCLE_OPT_DESCRIPTION)
            final String lifeCycle
    ) throws UnsupportedEncodingException {
        return ResponseHelper.getString(getFalconClient().suspendInstances(entityType.name(), entityName, start,
                end, getColo(colo), clusters, sourceClusters, getLifeCycle(lifeCycle), getDoAs()));
    }

    @CliCommand(value = {INSTANCE_COMMAND_PREFIX + RESUME_OPT},
            help = RESUME_OPT_DESCRIPTION)
    public String resume(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION)
            final String entityName,
            @CliOption(key = {COLO_OPT}, mandatory = true, help = COLO_OPT_DESCRIPTION)
            final String colo,
            @CliOption(key = {START_OPT}, mandatory = true, help = START_OPT_DESCRIPTION) final String start,
            @CliOption(key = {END_OPT}, mandatory = true, help = END_OPT_DESCRIPTION) final String end,
            @CliOption(key = {CLUSTERS_OPT}, mandatory = false, help = CLUSTERS_OPT_DESCRIPTION) final String clusters,
            @CliOption(key = {SOURCECLUSTER_OPT}, mandatory = false, help = SOURCECLUSTER_OPT_DESCRIPTION)
            final String sourceClusters,
            @CliOption(key = {LIFECYCLE_OPT}, mandatory = false, help = LIFECYCLE_OPT_DESCRIPTION)
            final String lifeCycle
    ) throws UnsupportedEncodingException {
        return ResponseHelper.getString(getFalconClient().resumeInstances(entityType.name(), entityName, start,
                end, getColo(colo), clusters, sourceClusters, getLifeCycle(lifeCycle), getDoAs()));
    }

    @CliCommand(value = {INSTANCE_COMMAND_PREFIX + RERUN_OPT},
            help = RERUN_OPT_DESCRIPTION)
    public String rerun(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION)
            final String entityName,
            @CliOption(key = {COLO_OPT}, mandatory = true, help = COLO_OPT_DESCRIPTION)
            final String colo,
            @CliOption(key = {START_OPT}, mandatory = true, help = START_OPT_DESCRIPTION) final String start,
            @CliOption(key = {END_OPT}, mandatory = true, help = END_OPT_DESCRIPTION) final String end,
            @CliOption(key = {FILE_PATH_OPT}, mandatory = false, help = FILE_PATH_OPT_DESCRIPTION)
            final String filePath,
            @CliOption(key = {CLUSTERS_OPT}, mandatory = false, help = CLUSTERS_OPT_DESCRIPTION) final String clusters,
            @CliOption(key = {SOURCECLUSTER_OPT}, mandatory = false, help = SOURCECLUSTER_OPT_DESCRIPTION)
            final String sourceClusters,
            @CliOption(key = {LIFECYCLE_OPT}, mandatory = false, help = LIFECYCLE_OPT_DESCRIPTION)
            final String lifeCycle,
            @CliOption(key = {FORCE_RERUN_FLAG}, mandatory = false, specifiedDefaultValue = "true",
                    help = FORCE_RERUN_FLAG_DESCRIPTION) final Boolean forceRerun
    ) throws IOException {
        return ResponseHelper.getString(getFalconClient().rerunInstances(entityType.name(), entityName, start,
                end, filePath, getColo(colo), clusters, sourceClusters, getLifeCycle(lifeCycle), forceRerun,
                getDoAs()));
    }

    @CliCommand(value = {INSTANCE_COMMAND_PREFIX + LOG_OPT},
            help = LOG_OPT_DESCRIPTION)
    public String log(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION)
            final String entityName,
            @CliOption(key = {COLO_OPT}, mandatory = true, help = COLO_OPT_DESCRIPTION)
            final String colo,
            @CliOption(key = {RUNID_OPT}, mandatory = true, help = RUNID_OPT_DESCRIPTION)
            final String runId,
            @CliOption(key = {START_OPT}, mandatory = false, help = START_OPT_DESCRIPTION) final String start,
            @CliOption(key = {END_OPT}, mandatory = false, help = END_OPT_DESCRIPTION) final String end,
            @CliOption(key = {CLUSTERS_OPT}, mandatory = false, help = CLUSTERS_OPT_DESCRIPTION) final String clusters,
            @CliOption(key = {LIFECYCLE_OPT}, mandatory = false, help = LIFECYCLE_OPT_DESCRIPTION)
            final String lifeCycle,
            @CliOption(key = {ORDER_BY_OPT}, mandatory = false, help = ORDER_BY_OPT_DESCRIPTION) final String orderBy,
            @CliOption(key = {SORT_ORDER_OPT}, mandatory = false,
                    help = SORT_ORDER_OPT_DESCRIPTION) final String sortOrder,
            @CliOption(key = {FILTER_BY_OPT}, mandatory = false,
                    help = FILTER_BY_OPT_DESCRIPTION) final String filterBy,
            @CliOption(key = {OFFSET_OPT}, mandatory = false, help = OFFSET_OPT_DESCRIPTION) final Integer offset,
            @CliOption(key = {NUM_RESULTS_OPT}, mandatory = false,
                    help = NUM_RESULTS_OPT_DESCRIPTION) final Integer numResults
    ) {
        validateOrderBy(orderBy, INSTANCE_PREFIX);
        validateFilterBy(filterBy, INSTANCE_PREFIX);
        return ResponseHelper.getString(getFalconClient().getLogsOfInstances(entityType.name(), entityName, start,
                end, getColo(colo), runId, getLifeCycle(lifeCycle), filterBy, orderBy, sortOrder, offset, numResults,
                getDoAs()));
    }

    @CliCommand(value = {INSTANCE_COMMAND_PREFIX + PARARMS_OPT},
            help = PARARMS_OPT_DESCRIPTION)
    public String params(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION)
            final String entityName,
            @CliOption(key = {COLO_OPT}, mandatory = true, help = COLO_OPT_DESCRIPTION)
            final String colo,
            @CliOption(key = {START_OPT}, mandatory = false, help = START_OPT_DESCRIPTION) final String start,
            @CliOption(key = {LIFECYCLE_OPT}, mandatory = false, help = LIFECYCLE_OPT_DESCRIPTION)
            final String lifeCycle
    ) throws IOException {
        return ResponseHelper.getString(getFalconClient().getParamsOfInstance(entityType.name(), entityName, start,
                getColo(colo), getLifeCycle(lifeCycle),
                getDoAs()));
    }

    @CliCommand(value = {INSTANCE_COMMAND_PREFIX + LISTING_OPT},
            help = LISTING_OPT_DESCRIPTION)
    public String listing(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION)
            final String entityName,
            @CliOption(key = {COLO_OPT}, mandatory = true, help = COLO_OPT_DESCRIPTION)
            final String colo,
            @CliOption(key = {START_OPT}, mandatory = false, help = START_OPT_DESCRIPTION) final String start,
            @CliOption(key = {END_OPT}, mandatory = false, help = END_OPT_DESCRIPTION) final String end
    ) {
        return ResponseHelper.getString(getFalconClient().getFeedInstanceListing(entityType.name(), entityName, start,
                end, getColo(colo), getDoAs()));
    }
}
