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

/**
 * Entity extension to Falcon Command Line Interface - wraps the RESTful API for entities.
 */
public class FalconEntityCLI extends FalconCLI {

    public static final String SUBMIT_OPT = "submit";
    public static final String SUBMIT_OPT_DESCRIPTION = "Submits an entity xml to Falcon";
    public static final String UPDATE_OPT = "update";
    public static final String UPDATE_OPT_DESCRIPTION = "Updates an existing entity xml";
    public static final String DELETE_OPT = "delete";
    public static final String DELETE_OPT_DESCRIPTION = "Deletes an entity in Falcon, and kills its instance from workflow engine";
    public static final String SUBMIT_AND_SCHEDULE_OPT = "submitAndSchedule";
    public static final String SUBMIT_AND_SCHEDULE_OPT_DESCRIPTION
            = "Submits and entity to Falcon and schedules it immediately";
    public static final String VALIDATE_OPT = "validate";
    public static final String VALIDATE_OPT_DESCRIPTION = "Validates an entity based on the entity type";
    public static final String DEFINITION_OPT = "definition";
    public static final String DEFINITION_OPT_DESCRIPTION = "Gets the Definition of entity";
    public static final String SLA_MISS_ALERT_OPT = "slaAlert";
    public static final String SLA_MISS_ALERT_OPT_DESCRIPTION = "Get missing feed instances which missed SLA";


    public static final String LOOKUP_OPT = "lookup";
    public static final String LOOKUP_OPT_DESCRIPTION = "Lookup a feed given its instance's path";
    public static final String PATH_OPT = "path";
    public static final String PATH_OPT_DESCRIPTION = "Path for a feed's instance";
    public static final String TOUCH_OPT = "touch";
    public static final String TOUCH_OPT_DESCRIPTION
            = "Force update the entity in workflow engine(even without any changes to entity)";
    public static final String PROPS_OPT = "properties";
    public static final String PROPS_OPT_DESCRIPTION = "User supplied comma separated key value properties";
    public static final String FIELDS_OPT = "fields";
    public static final String FIELDS_OPT_DESCRIPTION = "Entity fields to show for a request";
    public static final String TAGS_OPT = "tags";
    public static final String TAGS_OPT_DESCRIPTION = "Filter returned entities by the specified tags";
    public static final String NUM_INSTANCES_OPT = "numInstances";
    public static final String NUM_INSTANCES_OPT_DESCRIPTION = "Number of instances to return per entity summary request";
    public static final String NAMESEQ_OPT = "nameseq";
    public static final String NAMESEQ_OPT_DESCRIPTION = "Subsequence of entity name";
    public static final String TAGKEYS_OPT = "tagkeys";
    public static final String TAGKEYS_OPT_DESCRIPTION = "Keywords in tags";
    public static final String COLO_OPT_DESCRIPTION = "Colo name";
    public static final String OFFSET_OPT_DESCRIPTION = "Start returning entities from this offset";

    public FalconEntityCLI() throws Exception {
        super();
    }

    public Options createEntityOptions() {

        Options entityOptions = new Options();

        Option submit = new Option(SUBMIT_OPT, false, SUBMIT_OPT_DESCRIPTION);
        Option update = new Option(UPDATE_OPT, false, UPDATE_OPT_DESCRIPTION);
        Option schedule = new Option(SCHEDULE_OPT, false, SCHEDULE_OPT_DESCRIPTION);
        Option suspend = new Option(SUSPEND_OPT, false, SUSPEND_OPT_DESCRIPTION);
        Option resume = new Option(RESUME_OPT, false, RESUME_OPT_DESCRIPTION);
        Option delete = new Option(DELETE_OPT, false, DELETE_OPT_DESCRIPTION);
        Option submitAndSchedule = new Option(SUBMIT_AND_SCHEDULE_OPT, false, SUBMIT_AND_SCHEDULE_OPT_DESCRIPTION);
        Option validate = new Option(VALIDATE_OPT, false, VALIDATE_OPT_DESCRIPTION);
        Option status = new Option(STATUS_OPT, false, STATUS_OPT_DESCRIPTION);
        Option definition = new Option(DEFINITION_OPT, false, DEFINITION_OPT_DESCRIPTION);
        Option dependency = new Option(DEPENDENCY_OPT, false, DEPENDENCY_OPT_DESCRIPTION);
        Option list = new Option(LIST_OPT, false, LIST_OPT_DESCRIPTION);
        Option lookup = new Option(LOOKUP_OPT, false, LOOKUP_OPT_DESCRIPTION);
        Option slaAlert = new Option(SLA_MISS_ALERT_OPT, false, SLA_MISS_ALERT_OPT_DESCRIPTION);
        Option entitySummary = new Option(SUMMARY_OPT, false, SUMMARY_OPT_DESCRIPTION);
        Option touch = new Option(TOUCH_OPT, false, TOUCH_OPT_DESCRIPTION);

        OptionGroup group = new OptionGroup();
        group.addOption(submit);
        group.addOption(update);
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

        return entityOptions;
    }

    public void entityCommand(CommandLine commandLine, FalconClient client) throws FalconCLIException, IOException {
        Set<String> optionsList = new HashSet<String>();
        for (Option option : commandLine.getOptions()) {
            optionsList.add(option.getOpt());
        }

        String result = null;
        String entityType = commandLine.getOptionValue(TYPE_OPT);
        String entityName = commandLine.getOptionValue(ENTITY_NAME_OPT);
        String filePath = commandLine.getOptionValue(FILE_PATH_OPT);
        String colo = commandLine.getOptionValue(COLO_OPT);
        colo = getColo(colo);
        String cluster = commandLine.getOptionValue(CLUSTER_OPT);
        String start = commandLine.getOptionValue(START_OPT);
        String end = commandLine.getOptionValue(END_OPT);
        String orderBy = commandLine.getOptionValue(ORDER_BY_OPT);
        String sortOrder = commandLine.getOptionValue(SORT_ORDER_OPT);
        String filterBy = commandLine.getOptionValue(FILTER_BY_OPT);
        String filterTags = commandLine.getOptionValue(TAGS_OPT);
        String nameSubsequence = commandLine.getOptionValue(NAMESEQ_OPT);
        String tagKeywords = commandLine.getOptionValue(TAGKEYS_OPT);
        String fields = commandLine.getOptionValue(FIELDS_OPT);
        String feedInstancePath = commandLine.getOptionValue(PATH_OPT);
        Integer offset = parseIntegerInput(commandLine.getOptionValue(OFFSET_OPT), 0, "offset");
        Integer numResults = parseIntegerInput(commandLine.getOptionValue(NUM_RESULTS_OPT),
                null, "numResults");
        String doAsUser = commandLine.getOptionValue(DO_AS_OPT);

        Integer numInstances = parseIntegerInput(commandLine.getOptionValue(NUM_INSTANCES_OPT), 7, "numInstances");
        Boolean skipDryRun = null;
        if (optionsList.contains(SKIPDRYRUN_OPT)) {
            skipDryRun = true;
        }

        String userProps = commandLine.getOptionValue(PROPS_OPT);

        EntityType entityTypeEnum = null;
        if (optionsList.contains(LIST_OPT)) {
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
            validateNotEmpty(entityType, TYPE_OPT);
            entityTypeEnum = EntityType.getEnum(entityType);
        }
        validateSortOrder(sortOrder);
        String entityAction = "entity";

        if (optionsList.contains(SLA_MISS_ALERT_OPT)) {
            validateNotEmpty(entityType, TYPE_OPT);
            validateNotEmpty(start, START_OPT);
            parseDateString(start);
            parseDateString(end);
            SchedulableEntityInstanceResult response = client.getFeedSlaMissPendingAlerts(entityType,
                    entityName, start, end, colo);
            result = ResponseHelper.getString(response);
        } else if (optionsList.contains(SUBMIT_OPT)) {
            validateNotEmpty(filePath, FILE_PATH_OPT);
            validateColo(optionsList);
            result = client.submit(entityType, filePath, doAsUser).getMessage();
        } else if (optionsList.contains(LOOKUP_OPT)) {
            validateNotEmpty(feedInstancePath, PATH_OPT);
            FeedLookupResult resp = client.reverseLookUp(entityType, feedInstancePath, doAsUser);
            result = ResponseHelper.getString(resp);
        } else if (optionsList.contains(UPDATE_OPT)) {
            validateNotEmpty(filePath, FILE_PATH_OPT);
            validateColo(optionsList);
            validateNotEmpty(entityName, ENTITY_NAME_OPT);
            result = client.update(entityType, entityName, filePath, skipDryRun, doAsUser).getMessage();
        } else if (optionsList.contains(SUBMIT_AND_SCHEDULE_OPT)) {
            validateNotEmpty(filePath, FILE_PATH_OPT);
            validateColo(optionsList);
            result = client.submitAndSchedule(entityType, filePath, skipDryRun, doAsUser, userProps).getMessage();
        } else if (optionsList.contains(VALIDATE_OPT)) {
            validateNotEmpty(filePath, FILE_PATH_OPT);
            validateColo(optionsList);
            result = client.validate(entityType, filePath, skipDryRun, doAsUser).getMessage();
        } else if (optionsList.contains(SCHEDULE_OPT)) {
            validateNotEmpty(entityName, ENTITY_NAME_OPT);
            colo = getColo(colo);
            result = client.schedule(entityTypeEnum, entityName, colo, skipDryRun, doAsUser, userProps).getMessage();
        } else if (optionsList.contains(SUSPEND_OPT)) {
            validateNotEmpty(entityName, ENTITY_NAME_OPT);
            colo = getColo(colo);
            result = client.suspend(entityTypeEnum, entityName, colo, doAsUser).getMessage();
        } else if (optionsList.contains(RESUME_OPT)) {
            validateNotEmpty(entityName, ENTITY_NAME_OPT);
            colo = getColo(colo);
            result = client.resume(entityTypeEnum, entityName, colo, doAsUser).getMessage();
        } else if (optionsList.contains(DELETE_OPT)) {
            validateColo(optionsList);
            validateNotEmpty(entityName, ENTITY_NAME_OPT);
            result = client.delete(entityTypeEnum, entityName, doAsUser).getMessage();
        } else if (optionsList.contains(STATUS_OPT)) {
            validateNotEmpty(entityName, ENTITY_NAME_OPT);
            colo = getColo(colo);
            result = client.getStatus(entityTypeEnum, entityName, colo, doAsUser).getMessage();
        } else if (optionsList.contains(DEFINITION_OPT)) {
            validateColo(optionsList);
            validateNotEmpty(entityName, ENTITY_NAME_OPT);
            result = client.getDefinition(entityType, entityName, doAsUser).toString();
        } else if (optionsList.contains(DEPENDENCY_OPT)) {
            validateColo(optionsList);
            validateNotEmpty(entityName, ENTITY_NAME_OPT);
            result = client.getDependency(entityType, entityName, doAsUser).toString();
        } else if (optionsList.contains(LIST_OPT)) {
            validateColo(optionsList);
            validateEntityFields(fields);
            validateOrderBy(orderBy, entityAction);
            validateFilterBy(filterBy, entityAction);
            EntityList entityList = client.getEntityList(entityType, fields, nameSubsequence, tagKeywords,
                    filterBy, filterTags, orderBy, sortOrder, offset, numResults, doAsUser);
            result = entityList != null ? entityList.toString() : "No entity of type (" + entityType + ") found.";
        }  else if (optionsList.contains(SUMMARY_OPT)) {
            validateEntityTypeForSummary(entityType);
            validateNotEmpty(cluster, CLUSTER_OPT);
            validateEntityFields(fields);
            validateFilterBy(filterBy, entityAction);
            validateOrderBy(orderBy, entityAction);
            result = ResponseHelper.getString(client.getEntitySummary(
                    entityType, cluster, start, end, fields, filterBy, filterTags,
                    orderBy, sortOrder, offset, numResults, numInstances, doAsUser));
        } else if (optionsList.contains(TOUCH_OPT)) {
            validateNotEmpty(entityName, ENTITY_NAME_OPT);
            colo = getColo(colo);
            result = client.touch(entityType, entityName, colo, skipDryRun, doAsUser).getMessage();
        } else if (optionsList.contains(HELP_CMD)) {
            OUT.get().println("Falcon Help");
        } else {
            throw new FalconCLIException("Invalid command");
        }
        OUT.get().println(result);
    }

    private void validateColo(Set<String> optionsList) throws FalconCLIException {
        if (optionsList.contains(COLO_OPT)) {
            throw new FalconCLIException("Invalid argument : " + COLO_OPT);
        }
    }

    private void validateEntityFields(String fields) throws FalconCLIException {
        if (StringUtils.isEmpty(fields)) {
            return;
        }
        String[] fieldsList = fields.split(",");
        for (String s : fieldsList) {
            try {
                EntityList.EntityFieldList.valueOf(s.toUpperCase());
            } catch (IllegalArgumentException ie) {
                throw new FalconCLIException("Invalid fields argument : " + FIELDS_OPT);
            }
        }
    }

    private Date parseDateString(String time) throws FalconCLIException {
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
