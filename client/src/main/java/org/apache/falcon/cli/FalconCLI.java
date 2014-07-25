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

import com.sun.jersey.api.client.ClientHandlerException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.client.FalconClient;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.resource.EntityList;
import org.apache.falcon.resource.InstancesResult;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
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
    private static final String VERSION_CMD = "version";
    private static final String STACK_OPTION = "stack";

    public static final String ENTITY_CMD = "entity";
    public static final String ENTITY_TYPE_OPT = "type";
    public static final String COLO_OPT = "colo";
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
    public static final String LIST_OPT = "list";

    public static final String FIELDS_OPT = "fields";
    public static final String STATUS_FILTER_OPT = "statusFilter";
    public static final String ORDER_BY_OPT = "orderBy";
    public static final String OFFSET_OPT = "offset";
    public static final String NUM_RESULTS_OPT = "numResults";

    public static final String INSTANCE_CMD = "instance";
    public static final String START_OPT = "start";
    public static final String END_OPT = "end";
    public static final String EFFECTIVE_OPT = "effective";
    public static final String RUNNING_OPT = "running";
    public static final String KILL_OPT = "kill";
    public static final String RERUN_OPT = "rerun";
    public static final String CONTINUE_OPT = "continue";
    public static final String LOG_OPT = "logs";
    public static final String RUNID_OPT = "runid";
    public static final String CLUSTERS_OPT = "clusters";
    public static final String SOURCECLUSTER_OPT = "sourceClusters";
    public static final String CURRENT_COLO = "current.colo";
    public static final String CLIENT_PROPERTIES = "/client.properties";
    public static final String LIFECYCLE_OPT = "lifecycle";
    public static final String PARARMS_OPT = "params";

    // Graph Commands
    public static final String GRAPH_CMD = "graph";
    public static final String VERTEX_CMD = "vertex";
    public static final String VERTICES_CMD = "vertices";
    public static final String VERTEX_EDGES_CMD = "edges";

    // Graph Command Options
    public static final String EDGE_CMD = "edge";
    public static final String ID_OPT = "id";
    public static final String KEY_OPT = "key";
    public static final String VALUE_OPT = "value";
    public static final String DIRECTION_OPT = "direction";

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

        parser.addCommand(ADMIN_CMD, "", "admin operations", createAdminOptions(), true);
        parser.addCommand(HELP_CMD, "", "display usage", new Options(), false);
        parser.addCommand(VERSION_CMD, "", "show client version", new Options(), false);
        parser.addCommand(ENTITY_CMD,
                "",
                "Entity operations like submit, suspend, resume, delete, status, definition, submitAndSchedule",
                entityOptions(), false);
        parser.addCommand(INSTANCE_CMD,
                "",
                "Process instances operations like running, status, kill, suspend, resume, rerun, logs",
                instanceOptions(), false);
        parser.addCommand(GRAPH_CMD, "", "graph operations", createGraphOptions(), true);

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
                } else if (command.getName().equals(GRAPH_CMD)) {
                    graphCommand(commandLine, client);
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
        String runid = commandLine.getOptionValue(RUNID_OPT);
        String colo = commandLine.getOptionValue(COLO_OPT);
        String clusters = commandLine.getOptionValue(CLUSTERS_OPT);
        String sourceClusters = commandLine.getOptionValue(SOURCECLUSTER_OPT);
        List<LifeCycle> lifeCycles = getLifeCycle(commandLine.getOptionValue(LIFECYCLE_OPT));
        String statusFilter = commandLine.getOptionValue(STATUS_FILTER_OPT);
        String orderBy = commandLine.getOptionValue(ORDER_BY_OPT);

        Integer offset = 0;
        if (commandLine.getOptionValue(OFFSET_OPT) != null) {
            try {
                offset = Integer.parseInt(commandLine.getOptionValue(OFFSET_OPT));
            } catch (NumberFormatException e) {
                throw new FalconCLIException("Input value provided for queryParam \"offset\" is not a valid Integer");
            }
        }
        Integer numResults = -1;
        if (commandLine.getOptionValue(NUM_RESULTS_OPT) != null) {
            try {
                numResults = Integer.parseInt(commandLine.getOptionValue(NUM_RESULTS_OPT));
            } catch (NumberFormatException e) {
                throw new FalconCLIException("Input value provided for queryParam "
                        + "\"numResults\" is not a valid Integer");
            }
        }


        colo = getColo(colo);

        validateInstanceCommands(optionsList, entity, type, start, colo);

        if (optionsList.contains(RUNNING_OPT)) {
            System.out.println("INSTANCE RUNNING OPT STARTING");
            orderBy = validateOrderByInstanceList(orderBy);
            if (statusFilter != null) {
                throw new FalconCLIException("Invalid Argument statusFilter");
            }
            result = client.getRunningInstances(type, entity, colo, lifeCycles, orderBy, offset, numResults);
        } else if (optionsList.contains(STATUS_OPT) || optionsList.contains(LIST_OPT)) {
            System.out.println("INSTANCE LIST OPT STARTING");

            orderBy = validateOrderByInstanceList(orderBy);
            statusFilter = validateInstanceStatusFilter(statusFilter);
            result = client.getStatusOfInstances(type, entity, start, end, colo, lifeCycles,
                    statusFilter, orderBy, offset, numResults);
        } else if (optionsList.contains(SUMMARY_OPT)) {
            result = client.getSummaryOfInstances(type, entity, start, end, colo, lifeCycles);
        } else if (optionsList.contains(KILL_OPT)) {
            result = client.killInstances(type, entity, start, end, colo, clusters, sourceClusters, lifeCycles);
        } else if (optionsList.contains(SUSPEND_OPT)) {
            result = client.suspendInstances(type, entity, start, end, colo, clusters, sourceClusters, lifeCycles);
        } else if (optionsList.contains(RESUME_OPT)) {
            result = client.resumeInstances(type, entity, start, end, colo, clusters, sourceClusters, lifeCycles);
        } else if (optionsList.contains(RERUN_OPT)) {
            result = client.rerunInstances(type, entity, start, end, filePath, colo, clusters, sourceClusters,
                    lifeCycles);
        } else if (optionsList.contains(CONTINUE_OPT)) {
            result = client.rerunInstances(type, entity, start, end, colo, clusters, sourceClusters, lifeCycles);
        } else if (optionsList.contains(LOG_OPT)) {
            System.out.println("INSTANCE LOG OPT STARTING");

            orderBy = validateOrderByInstanceList(orderBy);
            statusFilter = validateInstanceStatusFilter(statusFilter);
            result = client.getLogsOfInstances(type, entity, start, end, colo, runid, lifeCycles,
                    statusFilter, orderBy, offset, numResults);
        } else if (optionsList.contains(PARARMS_OPT)) {
            // start time is the nominal time of instance
            result = client.getParamsOfInstance(type, entity, start, colo, clusters, sourceClusters, lifeCycles);
        } else {
            throw new FalconCLIException("Invalid command");
        }
        System.out.println("INSTANCE RESULT IS +++++"+result);

        OUT.get().println(result);
    }

    private void validateInstanceCommands(Set<String> optionsList,
                                          String entity, String type,
                                          String start, String colo) throws FalconCLIException {

        if (entity == null || entity.equals("")) {
            throw new FalconCLIException("Missing argument: name");
        }

        if (type == null || type.equals("")) {
            throw new FalconCLIException("Missing argument: type");
        }

        if (colo == null || colo.equals("")) {
            throw new FalconCLIException("Missing argument: colo");
        }

        if (!optionsList.contains(RUNNING_OPT)) {
            if (start == null || start.equals("")) {
                throw new FalconCLIException("Missing argument: start");
            }
        }

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
        String time = commandLine.getOptionValue(EFFECTIVE_OPT);
        String orderBy = commandLine.getOptionValue(ORDER_BY_OPT);
        String statusFilter = commandLine.getOptionValue(STATUS_FILTER_OPT);
        String fields = commandLine.getOptionValue(FIELDS_OPT);

        Integer offset = 0;
        if (commandLine.getOptionValue(OFFSET_OPT) != null) {
            try {
                offset = Integer.parseInt(commandLine.getOptionValue(OFFSET_OPT));
            } catch (NumberFormatException e) {
                throw new FalconCLIException("Input value provided for queryParam \"offset\" is not a valid Integer");
            }
        }
        Integer numResults = -1;
        if (commandLine.getOptionValue(NUM_RESULTS_OPT) != null) {
            try {
                numResults = Integer.parseInt(commandLine.getOptionValue(NUM_RESULTS_OPT));
            } catch (NumberFormatException e) {
                throw new FalconCLIException("Input value provided for queryParam "
                        + "\"numResults\" is not a valid Integer");
            }
        }


        validateEntityType(entityType);

        if (optionsList.contains(SUBMIT_OPT)) {
            validateFilePath(filePath);
            validateColo(optionsList);
            result = client.submit(entityType, filePath);
        } else if (optionsList.contains(UPDATE_OPT)) {
            validateFilePath(filePath);
            validateColo(optionsList);
            validateEntityName(entityName);
            Date effectiveTime = validateTime(time);
            result = client.update(entityType, entityName, filePath, effectiveTime);
        } else if (optionsList.contains(SUBMIT_AND_SCHEDULE_OPT)) {
            validateFilePath(filePath);
            validateColo(optionsList);
            result = client.submitAndSchedule(entityType, filePath);
        } else if (optionsList.contains(VALIDATE_OPT)) {
            validateFilePath(filePath);
            validateColo(optionsList);
            result = client.validate(entityType, filePath);
        } else if (optionsList.contains(SCHEDULE_OPT)) {
            validateEntityName(entityName);
            colo = getColo(colo);
            result = client.schedule(entityType, entityName, colo);
        } else if (optionsList.contains(SUSPEND_OPT)) {
            validateEntityName(entityName);
            colo = getColo(colo);
            result = client.suspend(entityType, entityName, colo);
        } else if (optionsList.contains(RESUME_OPT)) {
            validateEntityName(entityName);
            colo = getColo(colo);
            result = client.resume(entityType, entityName, colo);
        } else if (optionsList.contains(DELETE_OPT)) {
            validateColo(optionsList);
            validateEntityName(entityName);
            result = client.delete(entityType, entityName);
        } else if (optionsList.contains(STATUS_OPT)) {
            validateEntityName(entityName);
            colo = getColo(colo);
            result = client.getStatus(entityType, entityName, colo);
        } else if (optionsList.contains(DEFINITION_OPT)) {
            validateColo(optionsList);
            validateEntityName(entityName);
            result = client.getDefinition(entityType, entityName);
        } else if (optionsList.contains(DEPENDENCY_OPT)) {
            validateColo(optionsList);
            validateEntityName(entityName);
            result = client.getDependency(entityType, entityName).toString();
        } else if (optionsList.contains(LIST_OPT)) {
            System.out.println("ENTITY LIST OPT STARTING");
            // Validate fields. For now, supporting only "status"
            if (fields == null) {
                fields = "";
            } else if (!fields.equalsIgnoreCase("status")) {
                throw new FalconCLIException("Invalid value for queryParam \"fields\" : "+fields);
            }
            validateColo(optionsList);
            orderBy = validateOrderByEntityList(orderBy);
            if (statusFilter == null) {
                statusFilter = "";
            }
            EntityList entityList = client.getEntityList(entityType, fields, statusFilter, orderBy, offset, numResults);
            result = entityList != null ? entityList.toString() : "No entity of type (" + entityType + ") found.";
        } else if (optionsList.contains(HELP_CMD)) {
            OUT.get().println("Falcon Help");
        } else {
            throw new FalconCLIException("Invalid command");
        }

        System.out.println("ENTITY RESULT IS +++++"+result);

        OUT.get().println(result);
    }

    private String getColo(String colo) throws FalconCLIException, IOException {
        if (colo == null) {
            Properties prop = getClientProperties();
            colo = prop.getProperty(CURRENT_COLO, "*");
        }
        return colo;
    }

    private void validateFilePath(String filePath)
        throws FalconCLIException {

        if (filePath == null || filePath.equals("")) {
            throw new FalconCLIException("Missing argument: file");
        }
    }

    private void validateColo(Set<String> optionsList)
        throws FalconCLIException {

        if (optionsList.contains(COLO_OPT)) {
            throw new FalconCLIException("Invalid argument : " + COLO_OPT);
        }
    }

    private String validateOrderByEntityList(String orderBy)
        throws FalconCLIException {
        if (orderBy == null) {
            return "";
        } else {
            if (orderBy.equalsIgnoreCase("status") || orderBy.equalsIgnoreCase("type")
                    || orderBy.equalsIgnoreCase("name")) {
                return orderBy;
            } else {
                throw new FalconCLIException("Invalid orderBy argument : " + ORDER_BY_OPT);
            }
        }
    }

    private String validateOrderByInstanceList(String orderBy) throws FalconCLIException {
        if (orderBy == null) {
            return "";
        } else {
            if (orderBy.equalsIgnoreCase("status") || orderBy.equalsIgnoreCase("cluster")
                    || orderBy.equalsIgnoreCase("startTime") || orderBy.equalsIgnoreCase("endTime")) {
                return orderBy;
            } else {
                throw new FalconCLIException("Invalid orderBy argument : " + ORDER_BY_OPT);
            }
        }
    }

    private String validateInstanceStatusFilter(String statusFilter) throws FalconCLIException {
        if (statusFilter == null) {
            return "";
        } else {
            for (InstancesResult.WorkflowStatus s : InstancesResult.WorkflowStatus.values()) {
                if (s.toString().equalsIgnoreCase(statusFilter)) {
                    return statusFilter;
                }
            }
            throw new FalconCLIException("Invalid statusFilter argument : " + STATUS_FILTER_OPT);
        }
    }

    private Date validateTime(String time) throws FalconCLIException {
        if (time != null && !time.isEmpty()) {
            try {
                return SchemaHelper.parseDateUTC(time);
            } catch(Exception e) {
                throw new FalconCLIException("Time " + time + " is not valid", e);
            }
        }
        return null;
    }

    private void validateEntityName(String entityName)
        throws FalconCLIException {
        if (entityName == null || entityName.equals("")) {
            throw new FalconCLIException("Missing argument: name");
        }
    }

    private void validateEntityType(String entityType)
        throws FalconCLIException {

        if (entityType == null || entityType.equals("")) {
            throw new FalconCLIException("Missing argument: type");
        }
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
                "List entities registerd for a type");

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

        Option url = new Option(URL_OPTION, true, "Falcon URL");
        Option entityType = new Option(ENTITY_TYPE_OPT, true,
                "Entity type, can be cluster, feed or process xml");
        entityType.setRequired(true);
        Option filePath = new Option(FILE_PATH_OPT, true,
                "Path to entity xml file");
        Option entityName = new Option(ENTITY_NAME_OPT, true,
                "Entity type, can be cluster, feed or process xml");
        Option colo = new Option(COLO_OPT, true,
                "Colo name");
        colo.setRequired(false);
        Option effective = new Option(EFFECTIVE_OPT, true, "Effective time for update");
        Option fields = new Option(FIELDS_OPT, true, "Entity fields to show for a request");
        Option statusFilter = new Option(STATUS_FILTER_OPT, true,
                "Filter returned entities by the specified status");
        Option orderBy = new Option(ORDER_BY_OPT, true,
                "Order returned entities by this field");
        Option offset = new Option(OFFSET_OPT, true,
                "Start returning entities from this offset");
        Option numResults = new Option(NUM_RESULTS_OPT, true,
                "Number of results to return per request");

        entityOptions.addOption(url);
        entityOptions.addOptionGroup(group);
        entityOptions.addOption(entityType);
        entityOptions.addOption(entityName);
        entityOptions.addOption(filePath);
        entityOptions.addOption(colo);
        entityOptions.addOption(effective);
        entityOptions.addOption(fields);
        entityOptions.addOption(statusFilter);
        entityOptions.addOption(orderBy);
        entityOptions.addOption(offset);
        entityOptions.addOption(numResults);

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

        Option continues = new Option(
                CONTINUE_OPT,
                false,
                "resume process instance execution for a given process in the range start time and "
                        + "optional end time and overrides properties present in job.properties file");

        Option logs = new Option(
                LOG_OPT,
                false,
                "Logs print the logs for process instances for a given process in "
                        + "the range start time and optional end time");

        Option params = new Option(
                PARARMS_OPT,
                false,
                "Displays the workflow parameters for a given instance of specified nominal time");


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
        group.addOption(continues);
        group.addOption(params);

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
        Option statusFilter = new Option(STATUS_FILTER_OPT, true,
                "Filter returned instances by the specified status");
        Option orderBy = new Option(ORDER_BY_OPT, true,
                "Order returned instances by this field");
        Option offset = new Option(OFFSET_OPT, true,
                "Start returning instances from this offset");
        Option numResults = new Option(NUM_RESULTS_OPT, true,
                "Number of results to return per request");

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
        instanceOptions.addOption(statusFilter);
        instanceOptions.addOption(offset);
        instanceOptions.addOption(orderBy);
        instanceOptions.addOption(numResults);

        return instanceOptions;
    }

    private Options createGraphOptions() {
        Options graphOptions = new Options();
        Option url = new Option(URL_OPTION, true, "Falcon URL");
        graphOptions.addOption(url);

        Option vertex = new Option(VERTEX_CMD, false, "show the vertices");
        Option vertices = new Option(VERTICES_CMD, false, "show the vertices");
        Option vertexEdges = new Option(VERTEX_EDGES_CMD, false, "show the edges for a given vertex");
        Option edges = new Option(EDGE_CMD, false, "show the edges");

        OptionGroup group = new OptionGroup();
        group.addOption(vertex);
        group.addOption(vertices);
        group.addOption(vertexEdges);
        group.addOption(edges);
        graphOptions.addOptionGroup(group);

        Option id = new Option(ID_OPT, true, "vertex or edge id");
        graphOptions.addOption(id);

        Option key = new Option(KEY_OPT, true, "key property");
        graphOptions.addOption(key);

        Option value = new Option(VALUE_OPT, true, "value property");
        graphOptions.addOption(value);

        Option direction = new Option(DIRECTION_OPT, true, "edge direction property");
        graphOptions.addOption(direction);

        return graphOptions;
    }

    private void graphCommand(CommandLine commandLine,
                              FalconClient client) throws FalconCLIException {
        Set<String> optionsList = new HashSet<String>();
        for (Option option : commandLine.getOptions()) {
            optionsList.add(option.getOpt());
        }

        String result;
        String id = commandLine.getOptionValue(ID_OPT);
        String key = commandLine.getOptionValue(KEY_OPT);
        String value = commandLine.getOptionValue(VALUE_OPT);
        String direction = commandLine.getOptionValue(DIRECTION_OPT);

        if (optionsList.contains(VERTEX_CMD)) {
            validateId(id);
            result = client.getVertex(id);
        } else if (optionsList.contains(VERTICES_CMD)) {
            validateVerticesCommand(key, value);
            result = client.getVertices(key, value);
        } else if (optionsList.contains(VERTEX_EDGES_CMD)) {
            validateVertexEdgesCommand(id, direction);
            result = client.getVertexEdges(id, direction);
        } else if (optionsList.contains(EDGE_CMD)) {
            validateId(id);
            result = client.getEdge(id);
        } else {
            throw new FalconCLIException("Invalid command");
        }

        OUT.get().println(result);
    }

    private void validateId(String id) throws FalconCLIException {
        if (id == null || id.length() == 0) {
            throw new FalconCLIException("Missing argument: id");
        }
    }

    private void validateVerticesCommand(String key, String value) throws FalconCLIException {
        if (key == null || key.length() == 0) {
            throw new FalconCLIException("Missing argument: key");
        }

        if (value == null || value.length() == 0) {
            throw new FalconCLIException("Missing argument: value");
        }
    }

    private void validateVertexEdgesCommand(String id, String direction) throws FalconCLIException {
        if (id == null || id.length() == 0) {
            throw new FalconCLIException("Missing argument: id");
        }

        if (direction == null || direction.length() == 0) {
            throw new FalconCLIException("Missing argument: direction");
        }
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
}
