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
import org.apache.falcon.FalconCLIConstants;
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

    private static final String SUBMIT_OPT = "submit";
    private static final String UPDATE_OPT = "update";
    private static final String DELETE_OPT = "delete";
    private static final String SUBMIT_AND_SCHEDULE_OPT = "submitAndSchedule";
    private static final String VALIDATE_OPT = "validate";
    private static final String DEFINITION_OPT = "definition";
    public static final String SLA_MISS_ALERT_OPT = "slaAlert";

    private static final String LOOKUP_OPT = "lookup";
    private static final String PATH_OPT = "path";
    private static final String TOUCH_OPT = "touch";
    private static final String PROPS_OPT = "properties";
    private static final String FIELDS_OPT = "fields";
    private static final String TAGS_OPT = "tags";
    private static final String NUM_INSTANCES_OPT = "numInstances";
    private static final String NAMESEQ_OPT = "nameseq";
    private static final String TAGKEYS_OPT = "tagkeys";
    private static final String SHOWSCHEDULER_OPT = "showScheduler";

    public FalconEntityCLI() throws Exception {
        super();
    }

    public Options createEntityOptions() {

        Options entityOptions = new Options();

        Option submit = new Option(SUBMIT_OPT, false,
                "Submits an entity xml to Falcon");
        Option update = new Option(UPDATE_OPT, false,
                "Updates an existing entity xml");
        Option schedule = new Option(FalconCLIConstants.SCHEDULE_OPT, false,
                "Schedules a submited entity in Falcon");
        Option suspend = new Option(FalconCLIConstants.SUSPEND_OPT, false,
                "Suspends a running entity in Falcon");
        Option resume = new Option(FalconCLIConstants.RESUME_OPT, false,
                "Resumes a suspended entity in Falcon");
        Option delete = new Option(DELETE_OPT, false,
                "Deletes an entity in Falcon, and kills its instance from workflow engine");
        Option submitAndSchedule = new Option(SUBMIT_AND_SCHEDULE_OPT, false,
                "Submits and entity to Falcon and schedules it immediately");
        Option validate = new Option(VALIDATE_OPT, false,
                "Validates an entity based on the entity type");
        Option status = new Option(FalconCLIConstants.STATUS_OPT, false,
                "Gets the status of entity");
        Option definition = new Option(DEFINITION_OPT, false,
                "Gets the Definition of entity");
        Option dependency = new Option(FalconCLIConstants.DEPENDENCY_OPT, false,
                "Gets the dependencies of entity");
        Option list = new Option(FalconCLIConstants.LIST_OPT, false,
                "List entities registered for a type");
        Option lookup = new Option(LOOKUP_OPT, false, "Lookup a feed given its instance's path");
        Option slaAlert = new Option(SLA_MISS_ALERT_OPT, false, "Get missing feed instances which missed SLA");
        Option entitySummary = new Option(FalconCLIConstants.SUMMARY_OPT, false,
                "Get summary of instances for list of entities");
        Option touch = new Option(TOUCH_OPT, false,
                "Force update the entity in workflow engine(even without any changes to entity)");

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

        Option url = new Option(FalconCLIConstants.URL_OPTION, true, "Falcon URL");
        Option entityType = new Option(FalconCLIConstants.TYPE_OPT, true,
                "Entity type, can be cluster, feed or process xml");
        Option filePath = new Option(FalconCLIConstants.FILE_PATH_OPT, true,
                "Path to entity xml file");
        Option entityName = new Option(FalconCLIConstants.ENTITY_NAME_OPT, true,
                "Entity type, can be cluster, feed or process xml");
        Option start = new Option(FalconCLIConstants.START_OPT, true, "Start time is optional for summary");
        Option end = new Option(FalconCLIConstants.END_OPT, true, "End time is optional for summary");
        Option colo = new Option(FalconCLIConstants.COLO_OPT, true, "Colo name");
        Option cluster = new Option(FalconCLIConstants.CLUSTER_OPT, true, "Cluster name");
        colo.setRequired(false);
        Option fields = new Option(FIELDS_OPT, true, "Entity fields to show for a request");
        Option filterBy = new Option(FalconCLIConstants.FILTER_BY_OPT, true,
                "Filter returned entities by the specified status");
        Option filterTags = new Option(TAGS_OPT, true, "Filter returned entities by the specified tags");
        Option nameSubsequence = new Option(NAMESEQ_OPT, true, "Subsequence of entity name");
        Option tagKeywords = new Option(TAGKEYS_OPT, true, "Keywords in tags");
        Option orderBy = new Option(FalconCLIConstants.ORDER_BY_OPT, true,
                "Order returned entities by this field");
        Option sortOrder = new Option(FalconCLIConstants.SORT_ORDER_OPT, true, "asc or desc order for results");
        Option offset = new Option(FalconCLIConstants.OFFSET_OPT, true,
                "Start returning entities from this offset");
        Option numResults = new Option(FalconCLIConstants.NUM_RESULTS_OPT, true,
                "Number of results to return per request");
        Option numInstances = new Option(NUM_INSTANCES_OPT, true,
                "Number of instances to return per entity summary request");
        Option path = new Option(PATH_OPT, true, "Path for a feed's instance");
        Option skipDryRun = new Option(FalconCLIConstants.SKIPDRYRUN_OPT, false, "skip dry run in workflow engine");
        Option doAs = new Option(FalconCLIConstants.DO_AS_OPT, true, "doAs user");
        Option userProps = new Option(PROPS_OPT, true, "User supplied comma separated key value properties");
        Option showScheduler = new Option(SHOWSCHEDULER_OPT, false, "To return the scheduler "
                + "on which the entity is scheduled.");
        Option debug = new Option(FalconCLIConstants.DEBUG_OPTION, false,
                "Use debug mode to see debugging statements on stdout");

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

    public void entityCommand(CommandLine commandLine, FalconClient client) throws FalconCLIException, IOException {
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
        String nameSubsequence = commandLine.getOptionValue(NAMESEQ_OPT);
        String tagKeywords = commandLine.getOptionValue(TAGKEYS_OPT);
        String fields = commandLine.getOptionValue(FIELDS_OPT);
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
        if (optionsList.contains(FalconCLIConstants.LIST_OPT)) {
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

        if (optionsList.contains(SLA_MISS_ALERT_OPT)) {
            validateNotEmpty(entityType, FalconCLIConstants.TYPE_OPT);
            validateNotEmpty(start, FalconCLIConstants.START_OPT);
            parseDateString(start);
            parseDateString(end);
            SchedulableEntityInstanceResult response = client.getFeedSlaMissPendingAlerts(entityType,
                    entityName, start, end, colo);
            result = ResponseHelper.getString(response);
        } else if (optionsList.contains(SUBMIT_OPT)) {
            validateNotEmpty(filePath, "file");
            validateColo(optionsList);
            result = client.submit(entityType, filePath, doAsUser).getMessage();
        } else if (optionsList.contains(LOOKUP_OPT)) {
            validateNotEmpty(feedInstancePath, PATH_OPT);
            FeedLookupResult resp = client.reverseLookUp(entityType, feedInstancePath, doAsUser);
            result = ResponseHelper.getString(resp);
        } else if (optionsList.contains(UPDATE_OPT)) {
            validateNotEmpty(filePath, "file");
            validateColo(optionsList);
            validateNotEmpty(entityName, FalconCLIConstants.ENTITY_NAME_OPT);
            result = client.update(entityType, entityName, filePath, skipDryRun, doAsUser).getMessage();
        } else if (optionsList.contains(SUBMIT_AND_SCHEDULE_OPT)) {
            validateNotEmpty(filePath, "file");
            validateColo(optionsList);
            result = client.submitAndSchedule(entityType, filePath, skipDryRun, doAsUser, userProps).getMessage();
        } else if (optionsList.contains(VALIDATE_OPT)) {
            validateNotEmpty(filePath, "file");
            validateColo(optionsList);
            result = client.validate(entityType, filePath, skipDryRun, doAsUser).getMessage();
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
        } else if (optionsList.contains(DELETE_OPT)) {
            validateColo(optionsList);
            validateNotEmpty(entityName, FalconCLIConstants.ENTITY_NAME_OPT);
            result = client.delete(entityTypeEnum, entityName, doAsUser).getMessage();
        } else if (optionsList.contains(FalconCLIConstants.STATUS_OPT)) {
            validateNotEmpty(entityName, FalconCLIConstants.ENTITY_NAME_OPT);
            colo = getColo(colo);
            result = client.getStatus(entityTypeEnum, entityName, colo, doAsUser, showScheduler).getMessage();
        } else if (optionsList.contains(DEFINITION_OPT)) {
            validateColo(optionsList);
            validateNotEmpty(entityName, FalconCLIConstants.ENTITY_NAME_OPT);
            result = client.getDefinition(entityType, entityName, doAsUser).toString();
        } else if (optionsList.contains(FalconCLIConstants.DEPENDENCY_OPT)) {
            validateColo(optionsList);
            validateNotEmpty(entityName, FalconCLIConstants.ENTITY_NAME_OPT);
            result = client.getDependency(entityType, entityName, doAsUser).toString();
        } else if (optionsList.contains(FalconCLIConstants.LIST_OPT)) {
            validateColo(optionsList);
            validateEntityFields(fields);
            validateOrderBy(orderBy, entityAction);
            validateFilterBy(filterBy, entityAction);
            EntityList entityList = client.getEntityList(entityType, fields, nameSubsequence, tagKeywords,
                    filterBy, filterTags, orderBy, sortOrder, offset, numResults, doAsUser);
            result = entityList != null ? entityList.toString() : "No entity of type (" + entityType + ") found.";
        }  else if (optionsList.contains(FalconCLIConstants.SUMMARY_OPT)) {
            validateEntityTypeForSummary(entityType);
            validateNotEmpty(cluster, FalconCLIConstants.CLUSTER_OPT);
            validateEntityFields(fields);
            validateFilterBy(filterBy, entityAction);
            validateOrderBy(orderBy, entityAction);
            result = ResponseHelper.getString(client.getEntitySummary(
                    entityType, cluster, start, end, fields, filterBy, filterTags,
                    orderBy, sortOrder, offset, numResults, numInstances, doAsUser));
        } else if (optionsList.contains(TOUCH_OPT)) {
            validateNotEmpty(entityName, FalconCLIConstants.ENTITY_NAME_OPT);
            colo = getColo(colo);
            result = client.touch(entityType, entityName, colo, skipDryRun, doAsUser).getMessage();
        } else if (optionsList.contains(FalconCLIConstants.HELP_CMD)) {
            OUT.get().println("Falcon Help");
        } else {
            throw new FalconCLIException("Invalid command");
        }
        OUT.get().println(result);
    }

    private void validateColo(Set<String> optionsList) throws FalconCLIException {
        if (optionsList.contains(FalconCLIConstants.COLO_OPT)) {
            throw new FalconCLIException("Invalid argument : " + FalconCLIConstants.COLO_OPT);
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
