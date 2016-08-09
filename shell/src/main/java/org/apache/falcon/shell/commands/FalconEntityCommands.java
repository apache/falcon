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

package org.apache.falcon.shell.commands;

import org.apache.falcon.ValidationUtil;
import org.apache.falcon.ResponseHelper;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.resource.EntityList;
import org.apache.falcon.resource.FeedLookupResult;
import org.apache.falcon.resource.SchedulableEntityInstanceResult;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.File;

import static org.apache.falcon.client.FalconCLIConstants.DEFINITION_OPT;
import static org.apache.falcon.client.FalconCLIConstants.DELETE_OPT;
import static org.apache.falcon.client.FalconCLIConstants.LOOKUP_OPT;
import static org.apache.falcon.client.FalconCLIConstants.SLA_MISS_ALERT_OPT;
import static org.apache.falcon.client.FalconCLIConstants.SUBMIT_ONLY_OPT;
import static org.apache.falcon.client.FalconCLIConstants.UPDATE_OPT;
import static org.apache.falcon.client.FalconCLIConstants.CLUSTER_OPT;
import static org.apache.falcon.client.FalconCLIConstants.CLUSTER_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.COLO_OPT;
import static org.apache.falcon.client.FalconCLIConstants.COLO_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.DEFINITION_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.DELETE_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.DEPENDENCY_OPT;
import static org.apache.falcon.client.FalconCLIConstants.DEPENDENCY_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.END_OPT;
import static org.apache.falcon.client.FalconCLIConstants.END_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.ENTITY_NAME_OPT;
import static org.apache.falcon.client.FalconCLIConstants.ENTITY_NAME_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.FIELDS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.FIELDS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.FILE_PATH_OPT;
import static org.apache.falcon.client.FalconCLIConstants.FILE_PATH_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.FILTER_BY_OPT;
import static org.apache.falcon.client.FalconCLIConstants.FILTER_BY_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.LIST_OPT;
import static org.apache.falcon.client.FalconCLIConstants.LIST_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.LOOKUP_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.NAMESEQ_OPT;
import static org.apache.falcon.client.FalconCLIConstants.NAMESEQ_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.NUM_INSTANCES_OPT;
import static org.apache.falcon.client.FalconCLIConstants.NUM_INSTANCES_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.NUM_RESULTS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.NUM_RESULTS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.OFFSET_OPT;
import static org.apache.falcon.client.FalconCLIConstants.OFFSET_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.ORDER_BY_OPT;
import static org.apache.falcon.client.FalconCLIConstants.ORDER_BY_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.PATH_OPT;
import static org.apache.falcon.client.FalconCLIConstants.PATH_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.PROPS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.PROPS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.RESUME_OPT;
import static org.apache.falcon.client.FalconCLIConstants.RESUME_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.SCHEDULE_OPT;
import static org.apache.falcon.client.FalconCLIConstants.SCHEDULE_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.SHOWSCHEDULER_OPT;
import static org.apache.falcon.client.FalconCLIConstants.SHOWSCHEDULER_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.SKIPDRYRUN_OPT;
import static org.apache.falcon.client.FalconCLIConstants.SKIPDRYRUN_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.SLA_MISS_ALERT_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.SORT_ORDER_OPT;
import static org.apache.falcon.client.FalconCLIConstants.SORT_ORDER_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.START_OPT;
import static org.apache.falcon.client.FalconCLIConstants.START_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.STATUS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.STATUS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.SUBMIT_AND_SCHEDULE_OPT;
import static org.apache.falcon.client.FalconCLIConstants.SUBMIT_AND_SCHEDULE_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.SUBMIT_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.SUMMARY_OPT;
import static org.apache.falcon.client.FalconCLIConstants.SUMMARY_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.SUSPEND_OPT;
import static org.apache.falcon.client.FalconCLIConstants.SUSPEND_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.TAGKEYS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.TAGKEYS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.TAGS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.TAGS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.TYPE_OPT;
import static org.apache.falcon.client.FalconCLIConstants.TYPE_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.UPDATE_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.VALIDATE_OPT;
import static org.apache.falcon.client.FalconCLIConstants.VALIDATE_OPT_DESCRIPTION;

/**
 * Entity Commands.
 */
@Component
public class FalconEntityCommands extends BaseFalconCommands {
    public static final String ENTITY_PREFIX = "entity";
    public static final String ENTITY_COMMAND_PREFIX = ENTITY_PREFIX + " ";

    @CliCommand(value = ENTITY_COMMAND_PREFIX + SLA_MISS_ALERT_OPT, help = SLA_MISS_ALERT_OPT_DESCRIPTION)
    public String slaAlert(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = false, help = ENTITY_NAME_OPT_DESCRIPTION)
            final String entityName,
            @CliOption(key = {COLO_OPT}, mandatory = false, help = COLO_OPT_DESCRIPTION,
                    unspecifiedDefaultValue = "*") final String colo,
            @CliOption(key = {START_OPT}, mandatory = true, help = START_OPT_DESCRIPTION) final String start,
            @CliOption(key = {END_OPT}, mandatory = false, help = END_OPT_DESCRIPTION) final String end
    ) {
        SchedulableEntityInstanceResult response = getFalconClient()
                .getFeedSlaMissPendingAlerts(entityType.name().toLowerCase(), entityName, start, end, getColo(colo));
        return ResponseHelper.getString(response);
    }
    //The command here is submitOnly in place of submit as it conflicts with submitAndSchedule and tab feature will not
    //work of shell
    @CliCommand(value = ENTITY_COMMAND_PREFIX + SUBMIT_ONLY_OPT, help = SUBMIT_OPT_DESCRIPTION)
    public String submit(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {FILE_PATH_OPT}, mandatory = true, help = FILE_PATH_OPT_DESCRIPTION) final File filePath
    ) {

        return getFalconClient().submit(entityType.name().toLowerCase(), filePath.getPath(),
                getDoAs()).getMessage();
    }

    @CliCommand(value = ENTITY_COMMAND_PREFIX + LOOKUP_OPT, help = LOOKUP_OPT_DESCRIPTION)
    public String lookup(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {PATH_OPT}, mandatory = true, help = PATH_OPT_DESCRIPTION) final String feedInstancePath
    ) {

        FeedLookupResult resp = getFalconClient().reverseLookUp(entityType.name().toLowerCase(),
                feedInstancePath, getDoAs());
        return ResponseHelper.getString(resp);
    }

    @CliCommand(value = ENTITY_COMMAND_PREFIX + UPDATE_OPT, help = UPDATE_OPT_DESCRIPTION)
    public String update(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION)
            final String entityName,
            @CliOption(key = {FILE_PATH_OPT}, mandatory = true, help = FILE_PATH_OPT_DESCRIPTION)
            final File filePath,
            @CliOption(key = {SKIPDRYRUN_OPT}, mandatory = false, help = SKIPDRYRUN_OPT_DESCRIPTION,
                    unspecifiedDefaultValue = "false", specifiedDefaultValue = "true") boolean skipDryRun
    ) {
        return getFalconClient()
                .update(entityType.name().toLowerCase(), entityName, filePath.getPath(), skipDryRun, getDoAs())
                .getMessage();
    }

    @CliCommand(value = ENTITY_COMMAND_PREFIX + SUBMIT_AND_SCHEDULE_OPT, help = SUBMIT_AND_SCHEDULE_OPT_DESCRIPTION)
    public String submitAndSchedule(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {FILE_PATH_OPT}, mandatory = true, help = FILE_PATH_OPT_DESCRIPTION) final File filePath,
            @CliOption(key = {SKIPDRYRUN_OPT}, mandatory = false, help = SKIPDRYRUN_OPT_DESCRIPTION,
                    unspecifiedDefaultValue = "false", specifiedDefaultValue = "true") boolean skipDryRun,
            @CliOption(key = {PROPS_OPT}, mandatory = false, help = PROPS_OPT_DESCRIPTION) final String properties
    ) {

        return getFalconClient()
                .submitAndSchedule(entityType.name().toLowerCase(), filePath.getPath(), skipDryRun, getDoAs(),
                        properties)
                .getMessage();
    }

    @CliCommand(value = ENTITY_COMMAND_PREFIX + VALIDATE_OPT, help = VALIDATE_OPT_DESCRIPTION)
    public String validate(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {FILE_PATH_OPT}, mandatory = true, help = FILE_PATH_OPT_DESCRIPTION) final File filePath,
            @CliOption(key = {SKIPDRYRUN_OPT}, mandatory = false, help = SKIPDRYRUN_OPT_DESCRIPTION,
                    unspecifiedDefaultValue = "false", specifiedDefaultValue = "true") boolean skipDryRun
    ) {

        return getFalconClient()
                .validate(entityType.name().toLowerCase(), filePath.getPath(), skipDryRun, getDoAs())
                .getMessage();
    }

    @CliCommand(value = ENTITY_COMMAND_PREFIX + SCHEDULE_OPT, help = SCHEDULE_OPT_DESCRIPTION)
    public String schedule(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION) String entityName,
            @CliOption(key = {COLO_OPT}, mandatory = false, help = COLO_OPT_DESCRIPTION,
                    unspecifiedDefaultValue = "*") final String colo,
            @CliOption(key = {SKIPDRYRUN_OPT}, mandatory = false, help = SKIPDRYRUN_OPT_DESCRIPTION,
                    unspecifiedDefaultValue = "false", specifiedDefaultValue = "true") boolean skipDryRun,
            @CliOption(key = {PROPS_OPT}, mandatory = false, help = PROPS_OPT_DESCRIPTION) final String properties
    ) {

        return getFalconClient().schedule(entityType, entityName, colo, skipDryRun, getDoAs(),
                properties).getMessage();
    }

    @CliCommand(value = ENTITY_COMMAND_PREFIX + SUSPEND_OPT, help = SUSPEND_OPT_DESCRIPTION)
    public String suspend(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION) String entityName,
            @CliOption(key = {COLO_OPT}, mandatory = false, help = COLO_OPT_DESCRIPTION,
                    unspecifiedDefaultValue = "*") final String colo
    ) {

        return getFalconClient().suspend(entityType, entityName, colo, getDoAs()).getMessage();
    }

    @CliCommand(value = ENTITY_COMMAND_PREFIX + RESUME_OPT, help = RESUME_OPT_DESCRIPTION)
    public String resume(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION) String entityName,
            @CliOption(key = {COLO_OPT}, mandatory = false, help = COLO_OPT_DESCRIPTION,
                    unspecifiedDefaultValue = "*") final String colo
    ) {

        return getFalconClient().resume(entityType, entityName, colo, getDoAs()).getMessage();
    }

    @CliCommand(value = ENTITY_COMMAND_PREFIX + DELETE_OPT, help = DELETE_OPT_DESCRIPTION)
    public String delete(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION) String entityName
    ) {

        return getFalconClient().delete(entityType, entityName, getDoAs()).getMessage();
    }

    @CliCommand(value = ENTITY_COMMAND_PREFIX + STATUS_OPT, help = STATUS_OPT_DESCRIPTION)
    public String getStatus(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION) String entityName,
            @CliOption(key = {COLO_OPT}, mandatory = false, help = COLO_OPT_DESCRIPTION,
                    unspecifiedDefaultValue = "*") final String colo,
            @CliOption(key = {SHOWSCHEDULER_OPT}, mandatory = true,
                    help = SHOWSCHEDULER_OPT_DESCRIPTION) final boolean showScheduler
    ) {

        return getFalconClient().getStatus(entityType, entityName, colo, getDoAs(),
                showScheduler).getMessage();
    }

    @CliCommand(value = ENTITY_COMMAND_PREFIX + DEFINITION_OPT, help = DEFINITION_OPT_DESCRIPTION)
    public String getDefinition(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION) String entityName
    ) {

        return getFalconClient().getDefinition(entityType.name().toLowerCase(),
                entityName, getDoAs()).toString();
    }

    @CliCommand(value = ENTITY_COMMAND_PREFIX + DEPENDENCY_OPT, help = DEPENDENCY_OPT_DESCRIPTION)
    public String getDependency(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {ENTITY_NAME_OPT}, mandatory = true, help = ENTITY_NAME_OPT_DESCRIPTION) String entityName
    ) {

        return getFalconClient().getDependency(entityType.name().toLowerCase(), entityName,
                getDoAs()).toString();
    }

    // SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    @CliCommand(value = ENTITY_COMMAND_PREFIX + LIST_OPT, help = LIST_OPT_DESCRIPTION)
    public String list(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {FIELDS_OPT}, mandatory = false, help = FIELDS_OPT_DESCRIPTION) final String fields,
            @CliOption(key = {ORDER_BY_OPT}, mandatory = false, help = ORDER_BY_OPT_DESCRIPTION) final String orderBy,
            @CliOption(key = {SORT_ORDER_OPT}, mandatory = false,
                    help = SORT_ORDER_OPT_DESCRIPTION) final String sortOrder,
            @CliOption(key = {FILTER_BY_OPT}, mandatory = false,
                    help = FILTER_BY_OPT_DESCRIPTION) final String filterBy,
            @CliOption(key = {TAGS_OPT}, mandatory = false, help = TAGS_OPT_DESCRIPTION) final String filterTags,
            @CliOption(key = {NAMESEQ_OPT}, mandatory = false,
                    help = NAMESEQ_OPT_DESCRIPTION) final String nameSubsequence,
            @CliOption(key = {TAGKEYS_OPT}, mandatory = false, help = TAGKEYS_OPT_DESCRIPTION) final String tagKeywords,
            @CliOption(key = {OFFSET_OPT}, mandatory = false, help = OFFSET_OPT_DESCRIPTION) final Integer offset,
            @CliOption(key = {NUM_RESULTS_OPT}, mandatory = false,
                    help = NUM_RESULTS_OPT_DESCRIPTION) final Integer numResults

    ) {
        ValidationUtil.validateEntityFields(fields);
        ValidationUtil.validateOrderBy(orderBy, ENTITY_PREFIX);
        ValidationUtil.validateFilterBy(filterBy, ENTITY_PREFIX);
        EntityList entityList = getFalconClient().getEntityList(entityType.name().toLowerCase(),
                fields, nameSubsequence, tagKeywords, filterBy, filterTags, orderBy, sortOrder, offset,
                numResults, getDoAs());
        return entityList != null ? entityList.toString() : "No entity of type (" + entityType + ") found.";
    }

    @CliCommand(value = ENTITY_COMMAND_PREFIX + SUMMARY_OPT, help = SUMMARY_OPT_DESCRIPTION)
    public String summary(
            @CliOption(key = {TYPE_OPT}, mandatory = true, help = TYPE_OPT_DESCRIPTION) final EntityType entityType,
            @CliOption(key = {CLUSTER_OPT}, mandatory = true, help = CLUSTER_OPT_DESCRIPTION) final String cluster,
            @CliOption(key = {START_OPT}, mandatory = false, help = START_OPT_DESCRIPTION) final String start,
            @CliOption(key = {END_OPT}, mandatory = false, help = END_OPT_DESCRIPTION) final String end,
            @CliOption(key = {FIELDS_OPT}, mandatory = false, help = FIELDS_OPT_DESCRIPTION) final String fields,
            @CliOption(key = {ORDER_BY_OPT}, mandatory = false, help = ORDER_BY_OPT_DESCRIPTION) final String orderBy,
            @CliOption(key = {SORT_ORDER_OPT}, mandatory = false,
                    help = SORT_ORDER_OPT_DESCRIPTION) final String sortOrder,
            @CliOption(key = {FILTER_BY_OPT}, mandatory = false, help = FILTER_BY_OPT_DESCRIPTION) final String filterBy,
            @CliOption(key = {TAGS_OPT}, mandatory = false, help = TAGS_OPT_DESCRIPTION) final String filterTags,
            @CliOption(key = {OFFSET_OPT}, mandatory = false, help = OFFSET_OPT_DESCRIPTION) final Integer offset,
            @CliOption(key = {NUM_RESULTS_OPT}, mandatory = false,
                    help = NUM_RESULTS_OPT_DESCRIPTION) final Integer numResults,
            @CliOption(key = {NUM_INSTANCES_OPT}, mandatory = false,
                    help = NUM_INSTANCES_OPT_DESCRIPTION) final Integer numInstances

    ) {
        ValidationUtil.validateEntityTypeForSummary(entityType.name().toLowerCase());
        ValidationUtil.validateEntityFields(fields);
        ValidationUtil.validateFilterBy(filterBy, ENTITY_PREFIX);
        ValidationUtil.validateOrderBy(orderBy, ENTITY_PREFIX);
        return ResponseHelper.getString(getFalconClient().getEntitySummary(
                entityType.name().toLowerCase(), cluster, start, end, fields, filterBy, filterTags,
                orderBy, sortOrder, offset, numResults, numInstances, getDoAs()));
    }
    // RESUME CHECKSTYLE CHECK ParameterNumberCheck
}
