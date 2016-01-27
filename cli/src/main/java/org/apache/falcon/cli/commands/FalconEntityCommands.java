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

package org.apache.falcon.cli.commands;

import org.apache.falcon.ResponseHelper;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.resource.EntityList;
import org.apache.falcon.resource.FeedLookupResult;
import org.apache.falcon.resource.SchedulableEntityInstanceResult;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.File;

import static org.apache.falcon.cli.FalconCLI.CLUSTER_OPT;
import static org.apache.falcon.cli.FalconCLI.CLUSTER_OPT_DESCRIPTION;
import static org.apache.falcon.cli.FalconCLI.COLO_OPT;
import static org.apache.falcon.cli.FalconCLI.END_OPT;
import static org.apache.falcon.cli.FalconCLI.ENTITY_NAME_OPT;
import static org.apache.falcon.cli.FalconCLI.FILE_PATH_OPT;
import static org.apache.falcon.cli.FalconCLI.START_OPT;
import static org.apache.falcon.cli.FalconCLI.SUMMARY_OPT;
import static org.apache.falcon.cli.FalconCLI.SUMMARY_OPT_DESCRIPTION;
import static org.apache.falcon.cli.FalconCLI.TYPE_OPT;
import static org.apache.falcon.cli.FalconCLI.validateEntityTypeForSummary;
import static org.apache.falcon.cli.FalconCLI.validateFilterBy;
import static org.apache.falcon.cli.FalconCLI.validateOrderBy;
import static org.apache.falcon.cli.FalconEntityCLI.*;
import static org.apache.falcon.cli.FalconEntityCLI.DEPENDENCY_OPT;
import static org.apache.falcon.cli.FalconEntityCLI.DEPENDENCY_OPT_DESCRIPTION;
import static org.apache.falcon.cli.FalconEntityCLI.END_OPT_DESCRIPTION;
import static org.apache.falcon.cli.FalconEntityCLI.ENTITY_NAME_OPT_DESCRIPTION;
import static org.apache.falcon.cli.FalconEntityCLI.FILE_PATH_OPT_DESCRIPTION;
import static org.apache.falcon.cli.FalconEntityCLI.FILTER_BY_OPT;
import static org.apache.falcon.cli.FalconEntityCLI.FILTER_BY_OPT_DESCRIPTION;
import static org.apache.falcon.cli.FalconEntityCLI.LIST_OPT;
import static org.apache.falcon.cli.FalconEntityCLI.LIST_OPT_DESCRIPTION;
import static org.apache.falcon.cli.FalconEntityCLI.NUM_RESULTS_OPT;
import static org.apache.falcon.cli.FalconEntityCLI.NUM_RESULTS_OPT_DESCRIPTION;
import static org.apache.falcon.cli.FalconEntityCLI.OFFSET_OPT;
import static org.apache.falcon.cli.FalconEntityCLI.ORDER_BY_OPT;
import static org.apache.falcon.cli.FalconEntityCLI.ORDER_BY_OPT_DESCRIPTION;
import static org.apache.falcon.cli.FalconEntityCLI.RESUME_OPT;
import static org.apache.falcon.cli.FalconEntityCLI.RESUME_OPT_DESCRIPTION;
import static org.apache.falcon.cli.FalconEntityCLI.SCHEDULE_OPT;
import static org.apache.falcon.cli.FalconEntityCLI.SCHEDULE_OPT_DESCRIPTION;
import static org.apache.falcon.cli.FalconEntityCLI.SKIPDRYRUN_OPT;
import static org.apache.falcon.cli.FalconEntityCLI.SKIPDRYRUN_OPT_DESCRIPTION;
import static org.apache.falcon.cli.FalconEntityCLI.SORT_ORDER_OPT;
import static org.apache.falcon.cli.FalconEntityCLI.SORT_ORDER_OPT_DESCRIPTION;
import static org.apache.falcon.cli.FalconEntityCLI.START_OPT_DESCRIPTION;
import static org.apache.falcon.cli.FalconEntityCLI.STATUS_OPT;
import static org.apache.falcon.cli.FalconEntityCLI.STATUS_OPT_DESCRIPTION;
import static org.apache.falcon.cli.FalconEntityCLI.SUSPEND_OPT;
import static org.apache.falcon.cli.FalconEntityCLI.SUSPEND_OPT_DESCRIPTION;
import static org.apache.falcon.cli.FalconEntityCLI.TYPE_OPT_DESCRIPTION;

/**
 * Entity Commands.
 */
@Component
public class FalconEntityCommands extends BaseFalconCommands {
    public static final String ENTITY_PREFIX = "entity";
    public static final String ENTITY_PREFIX_SPACE = "entity ";

    @CliCommand(value = ENTITY_PREFIX_SPACE + SLA_MISS_ALERT_OPT, help = SLA_MISS_ALERT_OPT_DESCRIPTION)
    public String slaAlert(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final String entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION)
            final String entityName,
            @CliOption(key = {START_OPT}, mandatory = true, help = START_OPT_DESCRIPTION) final String start,
            @CliOption(key = {END_OPT}, mandatory = true, help = END_OPT_DESCRIPTION) final String end,
            @CliOption(key = {COLO_OPT}, mandatory = true, help = COLO_OPT_DESCRIPTION) final String colo
    ) {
        SchedulableEntityInstanceResult response = getFalconClient().getFeedSlaMissPendingAlerts(entityType,
                entityName, start, end, getColo(colo));
        return ResponseHelper.getString(response);
    }

    @CliCommand(value = ENTITY_PREFIX_SPACE + SUBMIT_OPT, help = SUBMIT_OPT_DESCRIPTION)
    public String submit(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final String entityType,
            @CliOption(key = {FILE_PATH_OPT}, mandatory = true, help = FILE_PATH_OPT_DESCRIPTION) final File filePath
    ) {

        return getFalconClient().submit(entityType, filePath.getPath(), getDoAs()).getMessage();
    }

    @CliCommand(value = ENTITY_PREFIX_SPACE + LOOKUP_OPT, help = LOOKUP_OPT_DESCRIPTION)
    public String lookup(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final String entityType,
            @CliOption(key = {PATH_OPT}, mandatory = true, help = PATH_OPT_DESCRIPTION) final File feedInstancePath
    ) {

        FeedLookupResult resp = getFalconClient().reverseLookUp(entityType, feedInstancePath.getPath(), getDoAs());
        return ResponseHelper.getString(resp);
    }

    @CliCommand(value = ENTITY_PREFIX_SPACE + UPDATE_OPT, help = UPDATE_OPT_DESCRIPTION)
    public String update(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final String entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION)
            final String entityName,
            @CliOption(key = {FILE_PATH_OPT}, mandatory = true, help = FILE_PATH_OPT_DESCRIPTION)
            final File filePath,
            @CliOption(key = {SKIPDRYRUN_OPT}, mandatory = false, help = SKIPDRYRUN_OPT_DESCRIPTION,
                    unspecifiedDefaultValue = "false", specifiedDefaultValue = "true") boolean skipDryRun
    ) {

        return getFalconClient()
                .update(entityType, entityName, filePath.getPath(), skipDryRun, getDoAs()).getMessage();
    }

    @CliCommand(value = ENTITY_PREFIX_SPACE + SUBMIT_AND_SCHEDULE_OPT, help = SUBMIT_AND_SCHEDULE_OPT_DESCRIPTION)
    public String submitAndSchedule(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final String entityType,
            @CliOption(key = {FILE_PATH_OPT}, mandatory = true, help = FILE_PATH_OPT_DESCRIPTION) final File filePath,
            @CliOption(key = {SKIPDRYRUN_OPT}, mandatory = false, help = SKIPDRYRUN_OPT_DESCRIPTION,
                    unspecifiedDefaultValue = "false", specifiedDefaultValue = "true") boolean skipDryRun,
            @CliOption(key = {PROPS_OPT}, mandatory = true, help = PROPS_OPT_DESCRIPTION) final String properties
    ) {

        return getFalconClient().submitAndSchedule(entityType, filePath.getPath(), skipDryRun, getDoAs(), properties).getMessage();
    }

    @CliCommand(value = ENTITY_PREFIX_SPACE + VALIDATE_OPT, help = VALIDATE_OPT_DESCRIPTION)
    public String validate(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final String entityType,
            @CliOption(key = {FILE_PATH_OPT}, mandatory = true, help = FILE_PATH_OPT_DESCRIPTION) final File filePath,
            @CliOption(key = {SKIPDRYRUN_OPT}, mandatory = false, help = SKIPDRYRUN_OPT_DESCRIPTION,
                    unspecifiedDefaultValue = "false", specifiedDefaultValue = "true") boolean skipDryRun
    ) {

        return getFalconClient().validate(entityType, filePath.getPath(), skipDryRun, getDoAs()).getMessage();
    }

    @CliCommand(value = ENTITY_PREFIX_SPACE + SCHEDULE_OPT, help = SCHEDULE_OPT_DESCRIPTION)
    public String schedule(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final String entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION) String entityName,
            @CliOption(key = {COLO_OPT}, mandatory = true, help = COLO_OPT_DESCRIPTION) final String colo,
            @CliOption(key = {SKIPDRYRUN_OPT}, mandatory = false, help = SKIPDRYRUN_OPT_DESCRIPTION,
                    unspecifiedDefaultValue = "false", specifiedDefaultValue = "true") boolean skipDryRun,
            @CliOption(key = {PROPS_OPT}, mandatory = true, help = PROPS_OPT_DESCRIPTION) final String properties
    ) {

        return getFalconClient().schedule(EntityType.valueOf(entityType.toUpperCase()), entityName, colo, skipDryRun, getDoAs(), properties).getMessage();
    }

    @CliCommand(value = ENTITY_PREFIX_SPACE + SUSPEND_OPT, help = SUSPEND_OPT_DESCRIPTION)
    public String suspend(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final String entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION) String entityName,
            @CliOption(key = {COLO_OPT}, mandatory = true, help = COLO_OPT_DESCRIPTION) final String colo
    ) {

        return getFalconClient().suspend(EntityType.valueOf(entityType.toUpperCase()), entityName, colo, getDoAs()).getMessage();
    }

    @CliCommand(value = ENTITY_PREFIX_SPACE + RESUME_OPT, help = RESUME_OPT_DESCRIPTION)
    public String resume(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final String entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION) String entityName,
            @CliOption(key = {COLO_OPT}, mandatory = true, help = COLO_OPT_DESCRIPTION) final String colo
    ) {

        return getFalconClient().resume(EntityType.valueOf(entityType.toUpperCase()), entityName, colo, getDoAs()).getMessage();
    }

    @CliCommand(value = ENTITY_PREFIX_SPACE + DELETE_OPT, help = DELETE_OPT_DESCRIPTION)
    public String delete(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final String entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION) String entityName
    ) {

        return getFalconClient().delete(EntityType.valueOf(entityType.toUpperCase()), entityName, getDoAs()).getMessage();
    }

    @CliCommand(value = ENTITY_PREFIX_SPACE + STATUS_OPT, help = STATUS_OPT_DESCRIPTION)
    public String getStatus(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final String entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION) String entityName,
            @CliOption(key = {COLO_OPT}, mandatory = true, help = COLO_OPT_DESCRIPTION) final String colo
    ) {

        return getFalconClient().getStatus(EntityType.valueOf(entityType.toUpperCase()), entityName, colo, getDoAs()).getMessage();
    }

    @CliCommand(value = ENTITY_PREFIX_SPACE + DEFINITION_OPT, help = DEFINITION_OPT_DESCRIPTION)
    public String getDefinition(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final String entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION) String entityName
    ) {

        return getFalconClient().getDefinition(entityType, entityName, getDoAs()).toString();
    }

    @CliCommand(value = ENTITY_PREFIX_SPACE + DEPENDENCY_OPT, help = DEPENDENCY_OPT_DESCRIPTION)
    public String getDependency(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final String entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION) String entityName
    ) {

        return getFalconClient().getDependency(entityType, entityName, getDoAs()).toString();
    }

    @CliCommand(value = ENTITY_PREFIX_SPACE + LIST_OPT, help = LIST_OPT_DESCRIPTION)
    public String list(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final String entityType,
            @CliOption(key = {FIELDS_OPT}, mandatory = true, help = FIELDS_OPT_DESCRIPTION) final String fields,
            @CliOption(key = {ORDER_BY_OPT}, mandatory = true, help = ORDER_BY_OPT_DESCRIPTION) final String orderBy,
            @CliOption(key = {SORT_ORDER_OPT}, mandatory = true, help = SORT_ORDER_OPT_DESCRIPTION) final String sortOrder,
            @CliOption(key = {FILTER_BY_OPT}, mandatory = true, help = FILTER_BY_OPT_DESCRIPTION) final String filterBy,
            @CliOption(key = {TAGS_OPT}, mandatory = true, help = TAGS_OPT_DESCRIPTION) final String filterTags,
            @CliOption(key = {NAMESEQ_OPT}, mandatory = true, help = NAMESEQ_OPT_DESCRIPTION) final String nameSubsequence,
            @CliOption(key = {TAGKEYS_OPT}, mandatory = true, help = TAGKEYS_OPT_DESCRIPTION) final String tagKeywords,
            @CliOption(key = {OFFSET_OPT}, mandatory = true, help = OFFSET_OPT_DESCRIPTION) final int offset,
            @CliOption(key = {NUM_RESULTS_OPT}, mandatory = true, help = NUM_RESULTS_OPT_DESCRIPTION) final int numResults

    ) {
        validateEntityFields(fields);
        validateOrderBy(orderBy, ENTITY_PREFIX);
        validateFilterBy(filterBy, ENTITY_PREFIX);
        EntityList entityList = getFalconClient().getEntityList(entityType, fields, nameSubsequence, tagKeywords,
                filterBy, filterTags, orderBy, sortOrder, offset, numResults, getDoAs());
        return entityList != null ? entityList.toString() : "No entity of type (" + entityType + ") found.";
    }

    @CliCommand(value = ENTITY_PREFIX_SPACE + SUMMARY_OPT, help = SUMMARY_OPT_DESCRIPTION)
    public String summary(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final String entityType,
            @CliOption(key = {CLUSTER_OPT}, mandatory = true, help = CLUSTER_OPT_DESCRIPTION) final String cluster,
            @CliOption(key = {START_OPT}, mandatory = true, help = START_OPT_DESCRIPTION) final String start,
            @CliOption(key = {END_OPT}, mandatory = true, help = END_OPT_DESCRIPTION) final String end,
            @CliOption(key = {FIELDS_OPT}, mandatory = true, help = FIELDS_OPT_DESCRIPTION) final String fields,
            @CliOption(key = {ORDER_BY_OPT}, mandatory = true, help = ORDER_BY_OPT_DESCRIPTION) final String orderBy,
            @CliOption(key = {SORT_ORDER_OPT}, mandatory = true, help = SORT_ORDER_OPT_DESCRIPTION) final String sortOrder,
            @CliOption(key = {FILTER_BY_OPT}, mandatory = true, help = FILTER_BY_OPT_DESCRIPTION) final String filterBy,
            @CliOption(key = {TAGS_OPT}, mandatory = true, help = TAGS_OPT_DESCRIPTION) final String filterTags,
            @CliOption(key = {OFFSET_OPT}, mandatory = true, help = OFFSET_OPT_DESCRIPTION) final int offset,
            @CliOption(key = {NUM_RESULTS_OPT}, mandatory = true, help = NUM_RESULTS_OPT_DESCRIPTION) final int numResults,
            @CliOption(key = {NUM_INSTANCES_OPT}, mandatory = true, help = NUM_INSTANCES_OPT_DESCRIPTION) final int numInstances

    ) {
        validateEntityTypeForSummary(entityType);
        validateEntityFields(fields);
        validateFilterBy(filterBy, ENTITY_PREFIX);
        validateOrderBy(orderBy, ENTITY_PREFIX);
        return ResponseHelper.getString(getFalconClient().getEntitySummary(
                entityType, cluster, start, end, fields, filterBy, filterTags,
                orderBy, sortOrder, offset, numResults, numInstances, getDoAs()));
    }
}
