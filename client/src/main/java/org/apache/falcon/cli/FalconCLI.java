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

import org.apache.falcon.ResponseHelper;
import com.sun.jersey.api.client.ClientHandlerException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.client.FalconClient;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.resource.EntityList;
import org.apache.falcon.resource.FeedLookupResult;
import org.apache.falcon.resource.InstancesResult;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Falcon Command Line Interface - wraps the RESTful API.
 */
public class FalconCLI {

    public static final AtomicReference<PrintStream> ERR = new AtomicReference<PrintStream>(System.err);
    public static final AtomicReference<PrintStream> OUT = new AtomicReference<PrintStream>(System.out);

    public static final String FALCON_URL = "FALCON_URL";
    public static final String URL_OPTION = "url";
    public static final String VERSION_OPTION = "version";
    public static final String STATUS_OPTION = "status";
    public static final String ADMIN_CMD = "admin";
    public static final String HELP_CMD = "help";
    public static final String METADATA_CMD = "metadata";
    private static final String VERSION_CMD = "version";
    private static final String STACK_OPTION = "stack";

    public static final String ENTITY_CMD = "entity";
    public static final String ENTITY_TYPE_OPT = "type";
    public static final String COLO_OPT = "colo";
    public static final String CLUSTER_OPT = "cluster";
    public static final String ENTITY_NAME_OPT = "name";
    public static final String FILE_PATH_OPT = "file";
    public static final String SUBMIT_OPT = "submit";
    public static final String UPDATE_OPT = "update";
    public static final String SCHEDULE_OPT = "schedule";
    public static final String SUSPEND_OPT = "suspend";
    public static final String RESUME_OPT = "resume";
    public static final String DELETE_OPT = "delete";
    public static final String SUBMIT_AND_SCHEDULE_OPT = "submitAndSchedule";
    public static final String VALIDATE_OPT = "validate";
    public static final String STATUS_OPT = "status";
    public static final String SUMMARY_OPT = "summary";
    public static final String DEFINITION_OPT = "definition";
    public static final String DEPENDENCY_OPT = "dependency";
    public static final String LOOKUP_OPT = "lookup";
    public static final String PATH_OPT = "path";
    public static final String LIST_OPT = "list";
    public static final String TOUCH_OPT = "touch";

    public static final String FIELDS_OPT = "fields";
    public static final String FILTER_BY_OPT = "filterBy";
    public static final String TAGS_OPT = "tags";
    public static final String ORDER_BY_OPT = "orderBy";
    public static final String SORT_ORDER_OPT = "sortOrder";
    public static final String OFFSET_OPT = "offset";
    public static final String NUM_RESULTS_OPT = "numResults";
    public static final String NUM_INSTANCES_OPT = "numInstances";
    public static final String PATTERN_OPT = "pattern";
    public static final String FORCE_RERUN_FLAG = "force";

    public static final String INSTANCE_CMD = "instance";
    public static final String START_OPT = "start";
    public static final String END_OPT = "end";
    public static final String RUNNING_OPT = "running";
    public static final String KILL_OPT = "kill";
    public static final String RERUN_OPT = "rerun";
    public static final String LOG_OPT = "logs";
    public static final String RUNID_OPT = "runid";
    public static final String CLUSTERS_OPT = "clusters";
    public static final String SOURCECLUSTER_OPT = "sourceClusters";
    public static final String CURRENT_COLO = "current.colo";
    public static final String CLIENT_PROPERTIES = "/client.properties";
    public static final String LIFECYCLE_OPT = "lifecycle";
    public static final String PARARMS_OPT = "params";
    public static final String LISTING_OPT = "listing";

    // Recipe Command
    public static final String RECIPE_CMD = "recipe";
    public static final String RECIPE_NAME = "name";
    public static final String RECIPE_TOOL_CLASS_NAME = "tool";

    private final Properties clientProperties;

    public FalconCLI() throws Exception {
        clientProperties = getClientProperties();
    }

    /**
     * Entry point for the Falcon CLI when invoked from the command line. Upon
     * completion this method exits the JVM with '0' (success) or '-1'
     * (failure).
     *
     * @param args options and arguments for the Falcon CLI.
     */
    public static void main(final String[] args) throws Exception {
        System.exit(new FalconCLI().run(args));
    }

    // TODO help and headers
    private static final String[] FALCON_HELP = { "the env variable '" + FALCON_URL
                                                          + "' is used as default value for the '-" + URL_OPTION
                                                          + "' option",
                                                  "custom headers for Falcon web services can be specified using '-D"
                                                          + FalconClient.WS_HEADER_PREFIX + "NAME=VALUE'", };
    /**
     * Run a CLI programmatically.
     * <p/>
     * It does not exit the JVM.
     * <p/>
     * A CLI instance can be used only once.
     *
     * @param args options and arguments for the Oozie CLI.
     * @return '0' (success), '-1' (failure).
     */
    public synchronized int run(final String[] args) {

        CLIParser parser = new CLIParser("falcon", FALCON_HELP);
        FalconMetadataCLI metadataCLI = new FalconMetadataCLI();

        parser.addCommand(ADMIN_CMD, "", "admin operations", createAdminOptions(), true);
        parser.addCommand(HELP_CMD, "", "display usage", new Options(), false);
        parser.addCommand(VERSION_CMD, "", "show client version", new Options(), false);
        parser.addCommand(ENTITY_CMD, "",
                "Entity operations like submit, suspend, resume, delete, status, definition, submitAndSchedule",
                entityOptions(), false);
        parser.addCommand(INSTANCE_CMD, "",
                "Process instances operations like running, status, kill, suspend, resume, rerun, logs",
                instanceOptions(), false);
        parser.addCommand(METADATA_CMD, "", "Metadata operations like list, relations",
                metadataCLI.createMetadataOptions(), true);
        parser.addCommand(RECIPE_CMD, "", "recipe operations", createRecipeOptions(), true);

        try {
            CLIParser.Command command = parser.parse(args);
            int exitValue = 0;
            if (command.getName().equals(HELP_CMD)) {
                parser.showHelp();
            } else {
                CommandLine commandLine = command.getCommandLine();
                String falconUrl = getFalconEndpoint(commandLine);
                FalconClient client = new FalconClient(falconUrl, clientProperties);

                if (command.getName().equals(ADMIN_CMD)) {
                    exitValue = adminCommand(commandLine, client, falconUrl);
                } else if (command.getName().equals(ENTITY_CMD)) {
                    entityCommand(commandLine, client);
                } else if (command.getName().equals(INSTANCE_CMD)) {
                    instanceCommand(commandLine, client);
                } else if (command.getName().equals(METADATA_CMD)) {
                    metadataCLI.metadataCommand(commandLine, client);
                } else if (command.getName().equals(RECIPE_CMD)) {
                    recipeCommand(commandLine, client);
                }
            }
            return exitValue;
        } catch (ParseException ex) {
            ERR.get().println("Invalid sub-command: " + ex.getMessage());
            ERR.get().println();
            ERR.get().println(parser.shortHelp());
            ERR.get().println("Stacktrace:");
            ex.printStackTrace();
            return -1;
        } catch (ClientHandlerException ex) {
            ERR.get().print("Unable to connect to Falcon server, "
                    + "please check if the URL is correct and Falcon server is up and running\n");
            ERR.get().println("Stacktrace:");
            ex.printStackTrace();
            return -1;
        } catch (FalconCLIException e) {
            ERR.get().println("ERROR: " + e.getMessage());
            return -1;
        } catch (Exception ex) {
            ERR.get().println("Stacktrace:");
            ex.printStackTrace();
            return -1;
        }
    }

    private void instanceCommand(CommandLine commandLine, FalconClient client)
        throws FalconCLIException, IOException {
        Set<String> optionsList = new HashSet<String>();
        for (Option option : commandLine.getOptions()) {
            optionsList.add(option.getOpt());
        }

        String result;
        String type = commandLine.getOptionValue(ENTITY_TYPE_OPT);
        String entity = commandLine.getOptionValue(ENTITY_NAME_OPT);
        String start = commandLine.getOptionValue(START_OPT);
        String end = commandLine.getOptionValue(END_OPT);
        String filePath = commandLine.getOptionValue(FILE_PATH_OPT);
        String runId = commandLine.getOptionValue(RUNID_OPT);
        String colo = commandLine.getOptionValue(COLO_OPT);
        String clusters = commandLine.getOptionValue(CLUSTERS_OPT);
        String sourceClusters = commandLine.getOptionValue(SOURCECLUSTER_OPT);
        List<LifeCycle> lifeCycles = getLifeCycle(commandLine.getOptionValue(LIFECYCLE_OPT));
        String filterBy = commandLine.getOptionValue(FILTER_BY_OPT);
        String orderBy = commandLine.getOptionValue(ORDER_BY_OPT);
        String sortOrder = commandLine.getOptionValue(SORT_ORDER_OPT);
        Integer offset = parseIntegerInput(commandLine.getOptionValue(OFFSET_OPT), 0, "offset");
        Integer numResults = parseIntegerInput(commandLine.getOptionValue(NUM_RESULTS_OPT),
                FalconClient.DEFAULT_NUM_RESULTS, "numResults");

        colo = getColo(colo);
        String instanceAction = "instance";
        validateSortOrder(sortOrder);
        validateInstanceCommands(optionsList, entity, type, colo);


        if (optionsList.contains(RUNNING_OPT)) {
            validateOrderBy(orderBy, instanceAction);
            validateFilterBy(filterBy, instanceAction);
            result =
                ResponseHelper.getString(client.getRunningInstances(type,
                        entity, colo, lifeCycles, filterBy, orderBy, sortOrder,
                        offset, numResults));
        } else if (optionsList.contains(STATUS_OPT) || optionsList.contains(LIST_OPT)) {
            validateOrderBy(orderBy, instanceAction);
            validateFilterBy(filterBy, instanceAction);
            result =
                ResponseHelper.getString(client
                        .getStatusOfInstances(type, entity, start, end, colo,
                                lifeCycles,
                                filterBy, orderBy, sortOrder, offset, numResults));
        } else if (optionsList.contains(SUMMARY_OPT)) {
            result =
                ResponseHelper.getString(client
                        .getSummaryOfInstances(type, entity, start, end, colo,
                                lifeCycles));
        } else if (optionsList.contains(KILL_OPT)) {
            validateNotEmpty(start, START_OPT);
            validateNotEmpty(end, END_OPT);
            result =
                ResponseHelper.getString(client
                        .killInstances(type, entity, start, end, colo, clusters,
                                sourceClusters, lifeCycles));
        } else if (optionsList.contains(SUSPEND_OPT)) {
            validateNotEmpty(start, START_OPT);
            validateNotEmpty(end, END_OPT);
            result =
                ResponseHelper.getString(client
                        .suspendInstances(type, entity, start, end, colo, clusters,
                                sourceClusters, lifeCycles));
        } else if (optionsList.contains(RESUME_OPT)) {
            validateNotEmpty(start, START_OPT);
            validateNotEmpty(end, END_OPT);
            result =
                ResponseHelper.getString(client
                        .resumeInstances(type, entity, start, end, colo, clusters,
                                sourceClusters, lifeCycles));
        } else if (optionsList.contains(RERUN_OPT)) {
            validateNotEmpty(start, START_OPT);
            validateNotEmpty(end, END_OPT);
            boolean isForced = false;
            if (optionsList.contains(FORCE_RERUN_FLAG)) {
                isForced = true;
            }
            result =
                ResponseHelper.getString(client
                        .rerunInstances(type, entity, start, end, filePath, colo,
                                clusters, sourceClusters,
                                lifeCycles, isForced));
        } else if (optionsList.contains(LOG_OPT)) {
            validateOrderBy(orderBy, instanceAction);
            validateFilterBy(filterBy, instanceAction);
            result =
                ResponseHelper.getString(client
                                .getLogsOfInstances(type, entity, start, end, colo, runId,
                                        lifeCycles,
                                        filterBy, orderBy, sortOrder, offset, numResults),
                        runId);
        } else if (optionsList.contains(PARARMS_OPT)) {
            // start time is the nominal time of instance
            result =
                ResponseHelper
                    .getString(client.getParamsOfInstance(
                            type, entity, start, colo, lifeCycles));
        } else if (optionsList.contains(LISTING_OPT)) {
            result =
                ResponseHelper.getString(client
                        .getFeedListing(type, entity, start, end, colo));
        } else {
            throw new FalconCLIException("Invalid command");
        }

        OUT.get().println(result);
    }

    private Integer parseIntegerInput(String optionValue, int defaultVal, String optionName) throws FalconCLIException {
        Integer integer = defaultVal;
        if (optionValue != null) {
            try {
                return Integer.parseInt(optionValue);
            } catch (NumberFormatException e) {
                throw new FalconCLIException("Input value provided for queryParam \""+ optionName
                        +"\" is not a valid Integer");
            }
        }
        return integer;
    }

    private void validateInstanceCommands(Set<String> optionsList,
                                          String entity, String type,
                                          String colo) throws FalconCLIException {

        validateNotEmpty(entity, ENTITY_NAME_OPT);
        validateNotEmpty(type, ENTITY_TYPE_OPT);
        validateNotEmpty(colo, COLO_OPT);

        if (optionsList.contains(CLUSTERS_OPT)) {
            if (optionsList.contains(RUNNING_OPT)
                    || optionsList.contains(LOG_OPT)
                    || optionsList.contains(STATUS_OPT)
                    || optionsList.contains(SUMMARY_OPT)) {
                throw new FalconCLIException("Invalid argument: clusters");
            }
        }

        if (optionsList.contains(SOURCECLUSTER_OPT)) {
            if (optionsList.contains(RUNNING_OPT)
                    || optionsList.contains(LOG_OPT)
                    || optionsList.contains(STATUS_OPT)
                    || optionsList.contains(SUMMARY_OPT) || !type.equals("feed")) {
                throw new FalconCLIException("Invalid argument: sourceClusters");
            }
        }

        if (optionsList.contains(FORCE_RERUN_FLAG)) {
            if (!optionsList.contains(RERUN_OPT)) {
                throw new FalconCLIException("Force option can be used only with instance rerun");
            }
        }
    }

    private void entityCommand(CommandLine commandLine, FalconClient client)
        throws FalconCLIException, IOException {
        Set<String> optionsList = new HashSet<String>();
        for (Option option : commandLine.getOptions()) {
            optionsList.add(option.getOpt());
        }

        String result = null;
        String entityType = commandLine.getOptionValue(ENTITY_TYPE_OPT);
        String entityName = commandLine.getOptionValue(ENTITY_NAME_OPT);
        String filePath = commandLine.getOptionValue(FILE_PATH_OPT);
        String colo = commandLine.getOptionValue(COLO_OPT);
        String cluster = commandLine.getOptionValue(CLUSTER_OPT);
        String start = commandLine.getOptionValue(START_OPT);
        String end = commandLine.getOptionValue(END_OPT);
        String orderBy = commandLine.getOptionValue(ORDER_BY_OPT);
        String sortOrder = commandLine.getOptionValue(SORT_ORDER_OPT);
        String filterBy = commandLine.getOptionValue(FILTER_BY_OPT);
        String filterTags = commandLine.getOptionValue(TAGS_OPT);
        String searchPattern = commandLine.getOptionValue(PATTERN_OPT);
        String fields = commandLine.getOptionValue(FIELDS_OPT);
        String feedInstancePath = commandLine.getOptionValue(PATH_OPT);
        Integer offset = parseIntegerInput(commandLine.getOptionValue(OFFSET_OPT), 0, "offset");
        Integer numResults = parseIntegerInput(commandLine.getOptionValue(NUM_RESULTS_OPT),
                FalconClient.DEFAULT_NUM_RESULTS, "numResults");
        Integer numInstances = parseIntegerInput(commandLine.getOptionValue(NUM_INSTANCES_OPT), 7, "numInstances");
        validateNotEmpty(entityType, ENTITY_TYPE_OPT);
        EntityType entityTypeEnum = EntityType.getEnum(entityType);
        validateSortOrder(sortOrder);
        String entityAction = "entity";

        if (optionsList.contains(SUBMIT_OPT)) {
            validateNotEmpty(filePath, "file");
            validateColo(optionsList);
            result = client.submit(entityType, filePath).getMessage();
        } else if (optionsList.contains(LOOKUP_OPT)) {
            validateNotEmpty(feedInstancePath, PATH_OPT);
            FeedLookupResult resp = client.reverseLookUp(entityType, feedInstancePath);
            result = ResponseHelper.getString(resp);

        } else if (optionsList.contains(UPDATE_OPT)) {
            validateNotEmpty(filePath, "file");
            validateColo(optionsList);
            validateNotEmpty(entityName, ENTITY_NAME_OPT);
            result = client.update(entityType, entityName, filePath).getMessage();
        } else if (optionsList.contains(SUBMIT_AND_SCHEDULE_OPT)) {
            validateNotEmpty(filePath, "file");
            validateColo(optionsList);
            result =
                    client.submitAndSchedule(entityType, filePath).getMessage();
        } else if (optionsList.contains(VALIDATE_OPT)) {
            validateNotEmpty(filePath, "file");
            validateColo(optionsList);
            result = client.validate(entityType, filePath).getMessage();
        } else if (optionsList.contains(SCHEDULE_OPT)) {
            validateNotEmpty(entityName, ENTITY_NAME_OPT);
            colo = getColo(colo);
            result = client.schedule(entityTypeEnum, entityName, colo).getMessage();
        } else if (optionsList.contains(SUSPEND_OPT)) {
            validateNotEmpty(entityName, ENTITY_NAME_OPT);
            colo = getColo(colo);
            result = client.suspend(entityTypeEnum, entityName, colo).getMessage();
        } else if (optionsList.contains(RESUME_OPT)) {
            validateNotEmpty(entityName, ENTITY_NAME_OPT);
            colo = getColo(colo);
            result = client.resume(entityTypeEnum, entityName, colo).getMessage();
        } else if (optionsList.contains(DELETE_OPT)) {
            validateColo(optionsList);
            validateNotEmpty(entityName, ENTITY_NAME_OPT);
            result = client.delete(entityTypeEnum, entityName).getMessage();
        } else if (optionsList.contains(STATUS_OPT)) {
            validateNotEmpty(entityName, ENTITY_NAME_OPT);
            colo = getColo(colo);
            result =
                    client.getStatus(entityTypeEnum, entityName, colo).getMessage();
        } else if (optionsList.contains(DEFINITION_OPT)) {
            validateColo(optionsList);
            validateNotEmpty(entityName, ENTITY_NAME_OPT);
            result = client.getDefinition(entityType, entityName).toString();
        } else if (optionsList.contains(DEPENDENCY_OPT)) {
            validateColo(optionsList);
            validateNotEmpty(entityName, ENTITY_NAME_OPT);
            result = client.getDependency(entityType, entityName).toString();
        } else if (optionsList.contains(LIST_OPT)) {
            validateColo(optionsList);
            validateEntityFields(fields);
            validateOrderBy(orderBy, entityAction);
            validateFilterBy(filterBy, entityAction);
            EntityList entityList = client.getEntityList(entityType, fields, filterBy,
                    filterTags, orderBy, sortOrder, offset, numResults, searchPattern);
            result = entityList != null ? entityList.toString() : "No entity of type (" + entityType + ") found.";
        }  else if (optionsList.contains(SUMMARY_OPT)) {
            validateEntityTypeForSummary(entityType);
            validateNotEmpty(cluster, CLUSTER_OPT);
            validateEntityFields(fields);
            validateFilterBy(filterBy, entityAction);
            validateOrderBy(orderBy, entityAction);
            result =
                    ResponseHelper.getString(client
                            .getEntitySummary(
                                    entityType, cluster, start, end, fields, filterBy,
                                    filterTags,
                                    orderBy, sortOrder, offset, numResults, numInstances));
        } else if (optionsList.contains(TOUCH_OPT)) {
            validateNotEmpty(entityName, ENTITY_NAME_OPT);
            colo = getColo(colo);
            result = client.touch(entityType, entityName, colo).getMessage();
        } else if (optionsList.contains(HELP_CMD)) {
            OUT.get().println("Falcon Help");
        } else {
            throw new FalconCLIException("Invalid command");
        }
        OUT.get().println(result);
    }

    private void validateEntityTypeForSummary(String type) throws FalconCLIException {
        EntityType entityType = EntityType.getEnum(type);
        if (!entityType.isSchedulable()) {
            throw new FalconCLIException("Invalid entity type " + entityType
                    + " for EntitySummary API. Valid options are feed or process");
        }
    }

    private void validateNotEmpty(String paramVal, String paramName) throws FalconCLIException {
        if (StringUtils.isEmpty(paramVal)) {
            throw new FalconCLIException("Missing argument : " + paramName);
        }
    }

    private void validateSortOrder(String sortOrder) throws FalconCLIException {
        if (!StringUtils.isEmpty(sortOrder)) {
            if (!sortOrder.equalsIgnoreCase("asc") && !sortOrder.equalsIgnoreCase("desc")) {
                throw new FalconCLIException("Value for param sortOrder should be \"asc\" or \"desc\". It is  : "
                        + sortOrder);
            }
        }
    }

    private String getColo(String colo) throws FalconCLIException, IOException {
        if (colo == null) {
            Properties prop = getClientProperties();
            colo = prop.getProperty(CURRENT_COLO, "*");
        }
        return colo;
    }

    private void validateColo(Set<String> optionsList)
        throws FalconCLIException {

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

    private void validateFilterBy(String filterBy, String filterType) throws FalconCLIException {
        if (StringUtils.isEmpty(filterBy)) {
            return;
        }
        String[] filterSplits = filterBy.split(",");
        for (String s : filterSplits) {
            String[] tempKeyVal = s.split(":", 2);
            try {
                if (filterType.equals("entity")) {
                    EntityList.EntityFilterByFields.valueOf(tempKeyVal[0].toUpperCase());
                } else if (filterType.equals("instance")) {
                    InstancesResult.InstanceFilterFields.valueOf(tempKeyVal[0].toUpperCase());
                } else {
                    throw new IllegalArgumentException("Invalid API call");
                }
            } catch (IllegalArgumentException ie) {
                throw new FalconCLIException("Invalid filterBy argument : " + FILTER_BY_OPT);
            }
        }
    }

    private void validateOrderBy(String orderBy, String action) throws FalconCLIException {
        if (StringUtils.isEmpty(orderBy)) {
            return;
        }
        if (action.equals("instance")) {
            if (Arrays.asList(new String[]{"status", "cluster", "starttime", "endtime"})
                .contains(orderBy.toLowerCase())) {
                return;
            }
        } else if (action.equals("entity")) {
            if (Arrays.asList(new String[] {"type", "name"}).contains(orderBy.toLowerCase())) {
                return;
            }
        }
        throw new FalconCLIException("Invalid orderBy argument : " + ORDER_BY_OPT);
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

    private Options createAdminOptions() {
        Options adminOptions = new Options();
        Option url = new Option(URL_OPTION, true, "Falcon URL");
        adminOptions.addOption(url);

        OptionGroup group = new OptionGroup();
        Option status = new Option(STATUS_OPTION, false,
                "show the current system status");
        Option version = new Option(VERSION_OPTION, false,
                "show Falcon server build version");
        Option stack = new Option(STACK_OPTION, false,
                "show the thread stack dump");
        Option help = new Option("help", false, "show Falcon help");
        group.addOption(status);
        group.addOption(version);
        group.addOption(stack);
        group.addOption(help);

        adminOptions.addOptionGroup(group);
        return adminOptions;
    }

    private Options entityOptions() {

        Options entityOptions = new Options();

        Option submit = new Option(SUBMIT_OPT, false,
                "Submits an entity xml to Falcon");
        Option update = new Option(UPDATE_OPT, false,
                "Updates an existing entity xml");
        Option schedule = new Option(SCHEDULE_OPT, false,
                "Schedules a submited entity in Falcon");
        Option suspend = new Option(SUSPEND_OPT, false,
                "Suspends a running entity in Falcon");
        Option resume = new Option(RESUME_OPT, false,
                "Resumes a suspended entity in Falcon");
        Option delete = new Option(DELETE_OPT, false,
                "Deletes an entity in Falcon, and kills its instance from workflow engine");
        Option submitAndSchedule = new Option(SUBMIT_AND_SCHEDULE_OPT, false,
                "Submits and entity to Falcon and schedules it immediately");
        Option validate = new Option(VALIDATE_OPT, false,
                "Validates an entity based on the entity type");
        Option status = new Option(STATUS_OPT, false,
                "Gets the status of entity");
        Option definition = new Option(DEFINITION_OPT, false,
                "Gets the Definition of entity");
        Option dependency = new Option(DEPENDENCY_OPT, false,
                "Gets the dependencies of entity");
        Option list = new Option(LIST_OPT, false,
                "List entities registered for a type");
        Option lookup = new Option(LOOKUP_OPT, false, "Lookup a feed given its instance's path");
        Option entitySummary = new Option(SUMMARY_OPT, false,
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
        group.addOption(entitySummary);
        group.addOption(touch);

        Option url = new Option(URL_OPTION, true, "Falcon URL");
        Option entityType = new Option(ENTITY_TYPE_OPT, true,
                "Entity type, can be cluster, feed or process xml");
        entityType.setRequired(true);
        Option filePath = new Option(FILE_PATH_OPT, true,
                "Path to entity xml file");
        Option entityName = new Option(ENTITY_NAME_OPT, true,
                "Entity type, can be cluster, feed or process xml");
        Option start = new Option(START_OPT, true, "Start time is optional for summary");
        Option end = new Option(END_OPT, true, "End time is optional for summary");
        Option colo = new Option(COLO_OPT, true, "Colo name");
        Option cluster = new Option(CLUSTER_OPT, true, "Cluster name");
        colo.setRequired(false);
        Option fields = new Option(FIELDS_OPT, true, "Entity fields to show for a request");
        Option filterBy = new Option(FILTER_BY_OPT, true,
                "Filter returned entities by the specified status");
        Option searchPattern = new Option(PATTERN_OPT, true,
                "Filter entities by fuzzy matching with specified pattern");
        Option filterTags = new Option(TAGS_OPT, true, "Filter returned entities by the specified tags");
        Option orderBy = new Option(ORDER_BY_OPT, true,
                "Order returned entities by this field");
        Option sortOrder = new Option(SORT_ORDER_OPT, true, "asc or desc order for results");
        Option offset = new Option(OFFSET_OPT, true,
                "Start returning entities from this offset");
        Option numResults = new Option(NUM_RESULTS_OPT, true,
                "Number of results to return per request");
        Option numInstances = new Option(NUM_INSTANCES_OPT, true,
                "Number of instances to return per entity summary request");
        Option path = new Option(PATH_OPT, true, "Path for a feed's instance");

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
        entityOptions.addOption(searchPattern);
        entityOptions.addOption(filterTags);
        entityOptions.addOption(orderBy);
        entityOptions.addOption(sortOrder);
        entityOptions.addOption(offset);
        entityOptions.addOption(numResults);
        entityOptions.addOption(numInstances);

        return entityOptions;
    }

    private Options instanceOptions() {

        Options instanceOptions = new Options();

        Option running = new Option(RUNNING_OPT, false,
                "Gets running process instances for a given process");
        Option list = new Option(LIST_OPT, false,
                "Gets all instances for a given process in the range start time and optional end time");
        Option status = new Option(
                STATUS_OPT,
                false,
                "Gets status of process instances for a given process in the range start time and optional end time");
        Option summary = new Option(
                SUMMARY_OPT,
                false,
                "Gets summary of instances for a given process in the range start time and optional end time");
        Option kill = new Option(
                KILL_OPT,
                false,
                "Kills active process instances for a given process in the range start time and optional end time");
        Option suspend = new Option(
                SUSPEND_OPT,
                false,
                "Suspends active process instances for a given process in the range start time and optional end time");
        Option resume = new Option(
                RESUME_OPT,
                false,
                "Resumes suspended process instances for a given process "
                        + "in the range start time and optional end time");
        Option rerun = new Option(
                RERUN_OPT,
                false,
                "Reruns process instances for a given process in the range start time and "
                        + "optional end time and overrides properties present in job.properties file");

        Option logs = new Option(
                LOG_OPT,
                false,
                "Logs print the logs for process instances for a given process in "
                        + "the range start time and optional end time");

        Option params = new Option(
                PARARMS_OPT,
                false,
                "Displays the workflow parameters for a given instance of specified nominal time"
                        + "start time represents nominal time and end time is not considered");

        Option listing = new Option(
                LISTING_OPT,
                false,
                "Displays feed listing and their status between a start and end time range.");

        OptionGroup group = new OptionGroup();
        group.addOption(running);
        group.addOption(list);
        group.addOption(status);
        group.addOption(summary);
        group.addOption(kill);
        group.addOption(resume);
        group.addOption(suspend);
        group.addOption(resume);
        group.addOption(rerun);
        group.addOption(logs);
        group.addOption(params);
        group.addOption(listing);

        Option url = new Option(URL_OPTION, true, "Falcon URL");
        Option start = new Option(START_OPT, true,
                "Start time is required for commands, status, kill, suspend, resume and re-run"
                        + "and it is nominal time while displaying workflow params");
        Option end = new Option(
                END_OPT,
                true,
                "End time is optional for commands, status, kill, suspend, resume and re-run; "
                        + "if not specified then current time is considered as end time");
        Option runid = new Option(RUNID_OPT, true,
                "Instance runid  is optional and user can specify the runid, defaults to 0");
        Option clusters = new Option(CLUSTERS_OPT, true,
                "clusters is optional for commands kill, suspend and resume, "
                        + "should not be specified for other commands");
        Option sourceClusters = new Option(SOURCECLUSTER_OPT, true,
                " source cluster is optional for commands kill, suspend and resume, "
                        + "should not be specified for other commands (required for only feed)");
        Option filePath = new Option(
                FILE_PATH_OPT,
                true,
                "Path to job.properties file is required for rerun command, "
                        + "it should contain name=value pair for properties to override for rerun");
        Option entityType = new Option(ENTITY_TYPE_OPT, true,
                "Entity type, can be feed or process xml");
        Option entityName = new Option(ENTITY_NAME_OPT, true,
                "Entity name, can be feed or process name");
        Option colo = new Option(COLO_OPT, true,
                "Colo on which the cmd has to be executed");
        Option lifecycle = new Option(LIFECYCLE_OPT,
                true,
                "describes life cycle of entity , for feed it can be replication/retention "
                       + "and for process it can be execution");
        Option filterBy = new Option(FILTER_BY_OPT, true,
                "Filter returned instances by the specified fields");
        Option orderBy = new Option(ORDER_BY_OPT, true,
                "Order returned instances by this field");
        Option sortOrder = new Option(SORT_ORDER_OPT, true, "asc or desc order for results");
        Option offset = new Option(OFFSET_OPT, true,
                "Start returning instances from this offset");
        Option numResults = new Option(NUM_RESULTS_OPT, true,
                "Number of results to return per request");
        Option forceRerun = new Option(FORCE_RERUN_FLAG, false,
                "Flag to forcefully rerun entire workflow of an instance");

        instanceOptions.addOption(url);
        instanceOptions.addOptionGroup(group);
        instanceOptions.addOption(start);
        instanceOptions.addOption(end);
        instanceOptions.addOption(filePath);
        instanceOptions.addOption(entityType);
        instanceOptions.addOption(entityName);
        instanceOptions.addOption(runid);
        instanceOptions.addOption(clusters);
        instanceOptions.addOption(sourceClusters);
        instanceOptions.addOption(colo);
        instanceOptions.addOption(lifecycle);
        instanceOptions.addOption(filterBy);
        instanceOptions.addOption(offset);
        instanceOptions.addOption(orderBy);
        instanceOptions.addOption(sortOrder);
        instanceOptions.addOption(numResults);
        instanceOptions.addOption(forceRerun);

        return instanceOptions;
    }

    private Options createRecipeOptions() {
        Options recipeOptions = new Options();
        Option url = new Option(URL_OPTION, true, "Falcon URL");
        recipeOptions.addOption(url);

        Option recipeFileOpt = new Option(RECIPE_NAME, true, "recipe name");
        recipeOptions.addOption(recipeFileOpt);

        Option recipeToolClassName = new Option(RECIPE_TOOL_CLASS_NAME, true, "recipe class");
        recipeOptions.addOption(recipeToolClassName);

        return recipeOptions;
    }

    protected String getFalconEndpoint(CommandLine commandLine) throws FalconCLIException, IOException {
        String url = commandLine.getOptionValue(URL_OPTION);
        if (url == null) {
            url = System.getenv(FALCON_URL);
        }
        if (url == null) {
            if (clientProperties.containsKey("falcon.url")) {
                url = clientProperties.getProperty("falcon.url");
            }
        }
        if (url == null) {
            throw new FalconCLIException("Failed to get falcon url from cmdline, or environment or client properties");
        }

        return url;
    }

    private int adminCommand(CommandLine commandLine, FalconClient client,
                             String falconUrl) throws FalconCLIException, IOException {
        String result;
        Set<String> optionsList = new HashSet<String>();
        for (Option option : commandLine.getOptions()) {
            optionsList.add(option.getOpt());
        }

        if (optionsList.contains(STACK_OPTION)) {
            result = client.getThreadDump();
            OUT.get().println(result);
        }
        int exitValue = 0;
        if (optionsList.contains(STATUS_OPTION)) {
            try {
                int status = client.getStatus();
                if (status != 200) {
                    ERR.get().println("Falcon server is not fully operational (on " + falconUrl + "). "
                            + "Please check log files.");
                    exitValue = status;
                } else {
                    OUT.get().println("Falcon server is running (on " + falconUrl + ")");
                }
            } catch (Exception e) {
                ERR.get().println("Falcon server doesn't seem to be running on " + falconUrl);
                exitValue = -1;
            }
        } else if (optionsList.contains(VERSION_OPTION)) {
            result = client.getVersion();
            OUT.get().println("Falcon server build version: " + result);
        } else if (optionsList.contains(HELP_CMD)) {
            OUT.get().println("Falcon Help");
        }

        return exitValue;
    }

    private Properties getClientProperties() throws IOException {
        InputStream inputStream = null;
        try {
            inputStream = FalconCLI.class.getResourceAsStream(CLIENT_PROPERTIES);
            Properties prop = new Properties();
            if (inputStream != null) {
                prop.load(inputStream);
            }
            return prop;
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
    }

    public static List<LifeCycle> getLifeCycle(String lifeCycleValue) throws FalconCLIException {

        if (lifeCycleValue != null) {
            String[] lifeCycleValues = lifeCycleValue.split(",");
            List<LifeCycle> lifeCycles = new ArrayList<LifeCycle>();
            try {
                for (String lifeCycle : lifeCycleValues) {
                    lifeCycles.add(LifeCycle.valueOf(lifeCycle.toUpperCase().trim()));
                }
            } catch (IllegalArgumentException e) {
                throw new FalconCLIException("Invalid life cycle values: " + lifeCycles, e);
            }
            return lifeCycles;
        }
        return null;
    }

    private void recipeCommand(CommandLine commandLine, FalconClient client) throws FalconCLIException {
        String recipeName = commandLine.getOptionValue(RECIPE_NAME);
        String recipeToolClass = commandLine.getOptionValue(RECIPE_TOOL_CLASS_NAME);

        validateNotEmpty(recipeName, RECIPE_NAME);

        String result =
            client.submitRecipe(recipeName, recipeToolClass).getMessage();
        OUT.get().println(result);
    }
}
