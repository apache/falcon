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

package org.apache.falcon.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.client.FalconCLIConstants;
import org.apache.falcon.ValidationUtil;
import org.apache.falcon.ResponseHelper;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.client.FalconClient;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.resource.EntityList;
import org.apache.falcon.resource.FeedLookupResult;
import org.apache.falcon.resource.SchedulableEntityInstanceResult;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static org.apache.falcon.client.FalconCLIConstants.SUBMIT_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.UPDATE_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.SCHEDULE_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.SUSPEND_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.RESUME_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.DELETE_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.SUBMIT_AND_SCHEDULE_OPT;
import static org.apache.falcon.client.FalconCLIConstants.VALIDATE_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.SUBMIT_AND_SCHEDULE_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.VALIDATE_OPT;
import static org.apache.falcon.client.FalconCLIConstants.STATUS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.DEFINITION_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.DEPENDENCY_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.LIST_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.LOOKUP_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.SLA_MISS_ALERT_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.SUMMARY_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.TOUCH_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.UPDATE_CLUSTER_DEPENDENTS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.URL_OPTION_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.URL_OPTION;
import static org.apache.falcon.client.FalconCLIConstants.TYPE_OPT;
import static org.apache.falcon.client.FalconCLIConstants.FILE_PATH_OPT;
import static org.apache.falcon.client.FalconCLIConstants.FILE_PATH_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.ENTITY_NAME_OPT;
import static org.apache.falcon.client.FalconCLIConstants.ENTITY_NAME_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.START_OPT;
import static org.apache.falcon.client.FalconCLIConstants.START_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.END_OPT;
import static org.apache.falcon.client.FalconCLIConstants.END_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.COLO_OPT;
import static org.apache.falcon.client.FalconCLIConstants.COLO_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.TYPE_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.CLUSTER_OPT;
import static org.apache.falcon.client.FalconCLIConstants.FIELDS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.FIELDS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.FILTER_BY_OPT;
import static org.apache.falcon.client.FalconCLIConstants.FILTER_BY_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.TAGS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.TAGS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.NAMESEQ_OPT;
import static org.apache.falcon.client.FalconCLIConstants.CLUSTER_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.TAGKEYS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.TAGKEYS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.ORDER_BY_OPT;
import static org.apache.falcon.client.FalconCLIConstants.ORDER_BY_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.SORT_ORDER_OPT;
import static org.apache.falcon.client.FalconCLIConstants.SORT_ORDER_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.OFFSET_OPT;
import static org.apache.falcon.client.FalconCLIConstants.OFFSET_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.NUM_RESULTS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.NUM_RESULTS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.NUM_INSTANCES_OPT;
import static org.apache.falcon.client.FalconCLIConstants.NUM_INSTANCES_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.PATH_OPT;
import static org.apache.falcon.client.FalconCLIConstants.PATH_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.SKIPDRYRUN_OPT;
import static org.apache.falcon.client.FalconCLIConstants.SKIPDRYRUN_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.DO_AS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.DO_AS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.PROPS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.PROPS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.SHOWSCHEDULER_OPT;
import static org.apache.falcon.client.FalconCLIConstants.SHOWSCHEDULER_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.DEBUG_OPTION;
import static org.apache.falcon.client.FalconCLIConstants.DEBUG_OPTION_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.NAMESEQ_OPT_DESCRIPTION;
import static org.apache.falcon.ValidationUtil.validateNotEmpty;
import static org.apache.falcon.ValidationUtil.validateSortOrder;


/**
 * Entity extension to Falcon Command Line Interface - wraps the RESTful API for entities.
 */
public class FalconEntityCLI extends FalconCLI {

    public FalconEntityCLI() throws Exception {
        super();
    }

    public Options createEntityOptions() {

        Options entityOptions = new Options();

        Option submit = new Option(FalconCLIConstants.SUBMIT_OPT, false, SUBMIT_OPT_DESCRIPTION);
        Option update = new Option(FalconCLIConstants.UPDATE_OPT, false, UPDATE_OPT_DESCRIPTION);
        Option schedule = new Option(FalconCLIConstants.SCHEDULE_OPT, false, SCHEDULE_OPT_DESCRIPTION);
        Option suspend = new Option(FalconCLIConstants.SUSPEND_OPT, false, SUSPEND_OPT_DESCRIPTION);
        Option resume = new Option(FalconCLIConstants.RESUME_OPT, false, RESUME_OPT_DESCRIPTION);
        Option delete = new Option(FalconCLIConstants.DELETE_OPT, false, DELETE_OPT_DESCRIPTION);
        Option submitAndSchedule = new Option(FalconCLIConstants.SUBMIT_AND_SCHEDULE_OPT, false,
                SUBMIT_AND_SCHEDULE_OPT_DESCRIPTION);
        Option validate = new Option(FalconCLIConstants.VALIDATE_OPT, false, VALIDATE_OPT_DESCRIPTION);
        Option status = new Option(FalconCLIConstants.STATUS_OPT, false, STATUS_OPT_DESCRIPTION);
        Option definition = new Option(FalconCLIConstants.DEFINITION_OPT, false, DEFINITION_OPT_DESCRIPTION);
        Option dependency = new Option(FalconCLIConstants.DEPENDENCY_OPT, false, DEPENDENCY_OPT_DESCRIPTION);
        Option list = new Option(FalconCLIConstants.LIST_OPT, false, LIST_OPT_DESCRIPTION);
        Option lookup = new Option(FalconCLIConstants.LOOKUP_OPT, false, LOOKUP_OPT_DESCRIPTION);
        Option slaAlert = new Option(FalconCLIConstants.SLA_MISS_ALERT_OPT, false, SLA_MISS_ALERT_OPT_DESCRIPTION);
        Option entitySummary = new Option(FalconCLIConstants.SUMMARY_OPT, false, SUMMARY_OPT_DESCRIPTION);
        Option touch = new Option(FalconCLIConstants.TOUCH_OPT, false, TOUCH_OPT_DESCRIPTION);

        Option updateClusterDependents = new Option(UPDATE_CLUSTER_DEPENDENTS_OPT, false,
                "Updates dependent entities of a cluster in workflow engine");

        OptionGroup group = new OptionGroup();
        group.addOption(update);
        group.addOption(updateClusterDependents);
        group.addOption(schedule);
        group.addOption(suspend);
        group.addOption(resume);
        group.addOption(delete);
        group.addOption(submitAndSchedule);
        group.addOption(validate);
        group.addOption(status);
        group.addOption(definition);
        group.addOption(dependency);
        group.addOption(list);
        group.addOption(lookup);
        group.addOption(slaAlert);
        group.addOption(entitySummary);
        group.addOption(touch);
        group.addOption(submit);

        Option url = new Option(URL_OPTION, true, URL_OPTION_DESCRIPTION);
        Option entityType = new Option(TYPE_OPT, true, TYPE_OPT_DESCRIPTION);
        Option filePath = new Option(FILE_PATH_OPT, true, FILE_PATH_OPT_DESCRIPTION);
        Option entityName = new Option(ENTITY_NAME_OPT, true, ENTITY_NAME_OPT_DESCRIPTION);
        Option start = new Option(START_OPT, true, START_OPT_DESCRIPTION);
        Option end = new Option(END_OPT, true, END_OPT_DESCRIPTION);
        Option colo = new Option(COLO_OPT, true, COLO_OPT_DESCRIPTION);
        Option cluster = new Option(CLUSTER_OPT, true, CLUSTER_OPT_DESCRIPTION);
        colo.setRequired(false);

        Option fields = new Option(FIELDS_OPT, true, FIELDS_OPT_DESCRIPTION);
        Option filterBy = new Option(FILTER_BY_OPT, true, FILTER_BY_OPT_DESCRIPTION);
        Option filterTags = new Option(TAGS_OPT, true, TAGS_OPT_DESCRIPTION);
        Option nameSubsequence = new Option(NAMESEQ_OPT, true, NAMESEQ_OPT_DESCRIPTION);
        Option tagKeywords = new Option(TAGKEYS_OPT, true, TAGKEYS_OPT_DESCRIPTION);
        Option orderBy = new Option(ORDER_BY_OPT, true, ORDER_BY_OPT_DESCRIPTION);
        Option sortOrder = new Option(SORT_ORDER_OPT, true, SORT_ORDER_OPT_DESCRIPTION);
        Option offset = new Option(OFFSET_OPT, true, OFFSET_OPT_DESCRIPTION);
        Option numResults = new Option(NUM_RESULTS_OPT, true, NUM_RESULTS_OPT_DESCRIPTION);
        Option numInstances = new Option(NUM_INSTANCES_OPT, true, NUM_INSTANCES_OPT_DESCRIPTION);
        Option path = new Option(PATH_OPT, true, PATH_OPT_DESCRIPTION);
        Option skipDryRun = new Option(SKIPDRYRUN_OPT, false, SKIPDRYRUN_OPT_DESCRIPTION);
        Option doAs = new Option(DO_AS_OPT, true, DO_AS_OPT_DESCRIPTION);
        Option userProps = new Option(PROPS_OPT, true, PROPS_OPT_DESCRIPTION);
        Option showScheduler = new Option(SHOWSCHEDULER_OPT, false, SHOWSCHEDULER_OPT_DESCRIPTION);
        Option debug = new Option(DEBUG_OPTION, false, DEBUG_OPTION_DESCRIPTION);

        entityOptions.addOption(url);
        entityOptions.addOption(path);
        entityOptions.addOptionGroup(group);
        entityOptions.addOption(entityType);
        entityOptions.addOption(entityName);
        entityOptions.addOption(filePath);
        entityOptions.addOption(colo);
        entityOptions.addOption(cluster);
        entityOptions.addOption(start);
        entityOptions.addOption(end);
        entityOptions.addOption(fields);
        entityOptions.addOption(filterBy);
        entityOptions.addOption(filterTags);
        entityOptions.addOption(nameSubsequence);
        entityOptions.addOption(tagKeywords);
        entityOptions.addOption(orderBy);
        entityOptions.addOption(sortOrder);
        entityOptions.addOption(offset);
        entityOptions.addOption(numResults);
        entityOptions.addOption(numInstances);
        entityOptions.addOption(skipDryRun);
        entityOptions.addOption(doAs);
        entityOptions.addOption(userProps);
        entityOptions.addOption(debug);
        entityOptions.addOption(showScheduler);

        return entityOptions;
    }

    public void entityCommand(CommandLine commandLine, FalconClient client) throws IOException {
        Set<String> optionsList = new HashSet<String>();
        for (Option option : commandLine.getOptions()) {
            optionsList.add(option.getOpt());
        }
        String result = null;
        String entityType = commandLine.getOptionValue(FalconCLIConstants.TYPE_OPT);
        String entityName = commandLine.getOptionValue(FalconCLIConstants.ENTITY_NAME_OPT);
        String filePath = commandLine.getOptionValue(FalconCLIConstants.FILE_PATH_OPT);
        String colo = commandLine.getOptionValue(FalconCLIConstants.COLO_OPT);
        colo = getColo(colo);
        String cluster = commandLine.getOptionValue(FalconCLIConstants.CLUSTER_OPT);
        String start = commandLine.getOptionValue(FalconCLIConstants.START_OPT);
        String end = commandLine.getOptionValue(FalconCLIConstants.END_OPT);
        String orderBy = commandLine.getOptionValue(FalconCLIConstants.ORDER_BY_OPT);
        String sortOrder = commandLine.getOptionValue(FalconCLIConstants.SORT_ORDER_OPT);
        String filterBy = commandLine.getOptionValue(FalconCLIConstants.FILTER_BY_OPT);
        String filterTags = commandLine.getOptionValue(TAGS_OPT);
        String nameSubsequence = commandLine.getOptionValue(FalconCLIConstants.NAMESEQ_OPT);
        String tagKeywords = commandLine.getOptionValue(FalconCLIConstants.TAGKEYS_OPT);
        String fields = commandLine.getOptionValue(FalconCLIConstants.FIELDS_OPT);
        String feedInstancePath = commandLine.getOptionValue(PATH_OPT);
        Integer offset = parseIntegerInput(commandLine.getOptionValue(FalconCLIConstants.OFFSET_OPT), 0, "offset");
        Integer numResults = parseIntegerInput(commandLine.getOptionValue(FalconCLIConstants.NUM_RESULTS_OPT),
                null, "numResults");
        String doAsUser = commandLine.getOptionValue(FalconCLIConstants.DO_AS_OPT);
        Integer numInstances = parseIntegerInput(commandLine.getOptionValue(NUM_INSTANCES_OPT), 7, "numInstances");
        Boolean skipDryRun = null;
        if (optionsList.contains(FalconCLIConstants.SKIPDRYRUN_OPT)) {
            skipDryRun = true;
        }
        String userProps = commandLine.getOptionValue(PROPS_OPT);
        boolean showScheduler = false;
        if (optionsList.contains(SHOWSCHEDULER_OPT)) {
            showScheduler = true;
        }
        EntityType entityTypeEnum = null;
        if (optionsList.contains(FalconCLIConstants.LIST_OPT)
                || optionsList.contains(UPDATE_CLUSTER_DEPENDENTS_OPT)) {
            if (entityType == null) {
                entityType = "";
            }
            if (StringUtils.isNotEmpty(entityType)) {
                String[] types = entityType.split(",");
                for (String type : types) {
                    EntityType.getEnum(type);
                }
            }
        } else {
            validateNotEmpty(entityType, FalconCLIConstants.TYPE_OPT);
            entityTypeEnum = EntityType.getEnum(entityType);
        }
        validateSortOrder(sortOrder);
        String entityAction = "entity";

        if (optionsList.contains(FalconCLIConstants.SLA_MISS_ALERT_OPT)) {
            validateNotEmpty(entityType, FalconCLIConstants.TYPE_OPT);
            validateNotEmpty(start, FalconCLIConstants.START_OPT);
            parseDateString(start);
            parseDateString(end);
            SchedulableEntityInstanceResult response = client.getFeedSlaMissPendingAlerts(entityType,
                    entityName, start, end, colo);
            result = ResponseHelper.getString(response);
        } else if (optionsList.contains(FalconCLIConstants.SUBMIT_OPT)) {
            validateNotEmpty(filePath, FILE_PATH_OPT);
            validateColo(optionsList);
            result = client.submit(entityType, filePath, doAsUser).getMessage();
        } else if (optionsList.contains(FalconCLIConstants.LOOKUP_OPT)) {
            validateNotEmpty(feedInstancePath, PATH_OPT);
            FeedLookupResult resp = client.reverseLookUp(entityType, feedInstancePath, doAsUser);
            result = ResponseHelper.getString(resp);
        } else if (optionsList.contains(FalconCLIConstants.UPDATE_OPT)) {
            validateNotEmpty(filePath, "file");
            validateColo(optionsList);
            validateNotEmpty(entityName, FalconCLIConstants.ENTITY_NAME_OPT);
            result = client.update(entityType, entityName, filePath, skipDryRun, doAsUser).getMessage();
        } else if (optionsList.contains(SUBMIT_AND_SCHEDULE_OPT)) {
            validateNotEmpty(filePath, FILE_PATH_OPT);
            validateColo(optionsList);
            result = client.submitAndSchedule(entityType, filePath, skipDryRun, doAsUser, userProps).getMessage();
        }  else if (optionsList.contains(FalconCLIConstants.UPDATE_CLUSTER_DEPENDENTS_OPT)) {
            validateNotEmpty(cluster, FalconCLIConstants.CLUSTER_OPT);
            result = client.updateClusterDependents(cluster, skipDryRun, doAsUser).getMessage();
        } else if (optionsList.contains(VALIDATE_OPT)) {
            validateNotEmpty(filePath, FILE_PATH_OPT);
            validateColo(optionsList);
            result = client.validate(entityType, filePath, skipDryRun, doAsUser).getMessage();
        } else if (optionsList.contains(FalconCLIConstants.SUBMIT_AND_SCHEDULE_OPT)) {
            validateNotEmpty(filePath, "file");
            validateColo(optionsList);
            result = client.submitAndSchedule(entityType, filePath, skipDryRun, doAsUser, userProps).getMessage();
        } else if (optionsList.contains(FalconCLIConstants.SCHEDULE_OPT)) {
            validateNotEmpty(entityName, FalconCLIConstants.ENTITY_NAME_OPT);
            colo = getColo(colo);
            result = client.schedule(entityTypeEnum, entityName, colo, skipDryRun, doAsUser, userProps).getMessage();
        } else if (optionsList.contains(FalconCLIConstants.SUSPEND_OPT)) {
            validateNotEmpty(entityName, FalconCLIConstants.ENTITY_NAME_OPT);
            colo = getColo(colo);
            result = client.suspend(entityTypeEnum, entityName, colo, doAsUser).getMessage();
        } else if (optionsList.contains(FalconCLIConstants.RESUME_OPT)) {
            validateNotEmpty(entityName, FalconCLIConstants.ENTITY_NAME_OPT);
            colo = getColo(colo);
            result = client.resume(entityTypeEnum, entityName, colo, doAsUser).getMessage();
        } else if (optionsList.contains(FalconCLIConstants.DELETE_OPT)) {
            validateColo(optionsList);
            validateNotEmpty(entityName, FalconCLIConstants.ENTITY_NAME_OPT);
            result = client.delete(entityTypeEnum, entityName, doAsUser).getMessage();
        } else if (optionsList.contains(FalconCLIConstants.STATUS_OPT)) {
            validateNotEmpty(entityName, FalconCLIConstants.ENTITY_NAME_OPT);
            colo = getColo(colo);
            result = client.getStatus(entityTypeEnum, entityName, colo, doAsUser, showScheduler).getMessage();
        } else if (optionsList.contains(FalconCLIConstants.DEFINITION_OPT)) {
            validateColo(optionsList);
            validateNotEmpty(entityName, FalconCLIConstants.ENTITY_NAME_OPT);
            result = client.getDefinition(entityType, entityName, doAsUser).toString();
        } else if (optionsList.contains(FalconCLIConstants.DEPENDENCY_OPT)) {
            validateColo(optionsList);
            validateNotEmpty(entityName, FalconCLIConstants.ENTITY_NAME_OPT);
            result = client.getDependency(entityType, entityName, doAsUser).toString();
        } else if (optionsList.contains(FalconCLIConstants.LIST_OPT)) {
            validateColo(optionsList);
            ValidationUtil.validateEntityFields(fields);
            ValidationUtil.validateOrderBy(orderBy, entityAction);
            ValidationUtil.validateFilterBy(filterBy, entityAction);
            EntityList entityList = client.getEntityList(entityType, fields, nameSubsequence, tagKeywords,
                    filterBy, filterTags, orderBy, sortOrder, offset, numResults, doAsUser);
            result = entityList != null ? entityList.toString() : "No entity of type (" + entityType + ") found.";
        }  else if (optionsList.contains(FalconCLIConstants.SUMMARY_OPT)) {
            ValidationUtil.validateEntityTypeForSummary(entityType);
            validateNotEmpty(cluster, FalconCLIConstants.CLUSTER_OPT);
            ValidationUtil.validateEntityFields(fields);
            ValidationUtil.validateFilterBy(filterBy, entityAction);
            ValidationUtil.validateOrderBy(orderBy, entityAction);
            result = ResponseHelper.getString(client.getEntitySummary(
                    entityType, cluster, start, end, fields, filterBy, filterTags,
                    orderBy, sortOrder, offset, numResults, numInstances, doAsUser));
        } else if (optionsList.contains(FalconCLIConstants.TOUCH_OPT)) {
            validateNotEmpty(entityName, FalconCLIConstants.ENTITY_NAME_OPT);
            colo = getColo(colo);
            result = client.touch(entityType, entityName, colo, skipDryRun, doAsUser).getMessage();
        } else if (optionsList.contains(FalconCLIConstants.HELP_CMD)) {
            OUT.get().println("Falcon Help");
        } else {
            throw new FalconCLIException("Invalid/missing entity command. Supported commands include "
                    + "submit, suspend, resume, delete, status, definition, submitAndSchedule. "
                    + "Please refer to Falcon CLI twiki for more details.");
        }
        OUT.get().println(result);
    }

    static void validateColo(Set<String> optionsList) {
        if (optionsList.contains(FalconCLIConstants.COLO_OPT)) {
            throw new FalconCLIException("Invalid argument : " + FalconCLIConstants.COLO_OPT);
        }
    }

    private Date parseDateString(String time) {
        if (time != null && !time.isEmpty()) {
            try {
                return SchemaHelper.parseDateUTC(time);
            } catch(Exception e) {
                throw new FalconCLIException("Time " + time + " is not valid", e);
            }
        }
        return null;
    }

}
