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
import org.apache.falcon.LifeCycle;
import org.apache.falcon.ResponseHelper;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.client.FalconClient;
import org.apache.falcon.resource.InstanceDependencyResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Instance extension to Falcon Command Line Interface - wraps the RESTful API for instances.
 */
public class FalconInstanceCLI extends FalconCLI {

    public static final String FORCE_RERUN_FLAG = "force";
    public static final String INSTANCE_TIME_OPT = "instanceTime";
    public static final String RUNNING_OPT = "running";
    public static final String KILL_OPT = "kill";
    public static final String RERUN_OPT = "rerun";
    public static final String LOG_OPT = "logs";
    public static final String ALL_ATTEMPTS = "allAttempts";
    public static final String RUNID_OPT = "runid";
    public static final String CLUSTERS_OPT = "clusters";
    public static final String SOURCECLUSTER_OPT = "sourceClusters";
    public static final String LIFECYCLE_OPT = "lifecycle";
    public static final String PARARMS_OPT = "params";
    public static final String LISTING_OPT = "listing";
    public static final String TRIAGE_OPT = "triage";
    public static final String RUNNING_OPT_DESCRIPTION = "Gets running process instances for a given process";
    public static final String LIST_OPT_DESCRIPTION = "Gets all instances for a given process in the range start "
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
    public static final String NUM_RESULTS_OPT_DESCRIPTION = "Number of results to return per request";
    public static final String FORCE_RERUN_FLAG_DESCRIPTION = "Flag to forcefully rerun entire workflow "
      + "of an instance";
    public static final String DO_AS_OPT_DESCRIPTION = "doAs user";
    public static final String DEBUG_OPTION_DESCRIPTION = "Use debug mode to see debugging statements on stdout";
    public static final String INSTANCE_TIME_OPT_DESCRIPTION = "Time for an instance";
    public static final String ALL_ATTEMPTS_DESCRIPTION = "To get all attempts of corresponding instances";
    public FalconInstanceCLI() throws Exception {
        super();
    }

    public Options createInstanceOptions() {

        Options instanceOptions = new Options();

        Option running = new Option(RUNNING_OPT, false, RUNNING_OPT_DESCRIPTION);
        Option list = new Option(LIST_OPT, false, LIST_OPT_DESCRIPTION);
        Option status = new Option(STATUS_OPT, false, STATUS_OPT_DESCRIPTION);
        Option summary = new Option(SUMMARY_OPT, false, SUMMARY_OPT_DESCRIPTION);
        Option kill = new Option(KILL_OPT, false, KILL_OPT_DESCRIPTION);
        Option suspend = new Option(SUSPEND_OPT, false, SUSPEND_OPT_DESCRIPTION);
        Option resume = new Option(RESUME_OPT, false, RESUME_OPT_DESCRIPTION);
        Option rerun = new Option(RERUN_OPT, false, RERUN_OPT_DESCRIPTION);
        Option logs = new Option(LOG_OPT, false, LOG_OPT_DESCRIPTION);
        Option params = new Option(PARARMS_OPT, false, PARARMS_OPT_DESCRIPTION);
        Option listing = new Option(LISTING_OPT, false, LISTING_OPT_DESCRIPTION);
        Option dependency = new Option(DEPENDENCY_OPT, false, DEPENDENCY_OPT_DESCRIPTION);
        Option triage = new Option(TRIAGE_OPT, false, TRIAGE_OPT_DESCRIPTION);

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
        group.addOption(dependency);
        group.addOption(triage);

        Option url = new Option(URL_OPTION, true, URL_OPTION_DESCRIPTION);
        Option start = new Option(START_OPT, true, START_OPT_DESCRIPTION);
        Option end = new Option(END_OPT, true, END_OPT_DESCRIPTION);
        Option runid = new Option(RUNID_OPT, true, RUNID_OPT_DESCRIPTION);
        Option clusters = new Option(CLUSTERS_OPT, true, CLUSTERS_OPT_DESCRIPTION);
        Option sourceClusters = new Option(SOURCECLUSTER_OPT, true, SOURCECLUSTER_OPT_DESCRIPTION);
        Option filePath = new Option(FILE_PATH_OPT, true, FILE_PATH_OPT_DESCRIPTION);
        Option entityType = new Option(TYPE_OPT, true, TYPE_OPT_DESCRIPTION);
        Option entityName = new Option(ENTITY_NAME_OPT, true, ENTITY_NAME_OPT_DESCRIPTION);
        Option colo = new Option(COLO_OPT, true, COLO_OPT_DESCRIPTION);
        Option lifecycle = new Option(LIFECYCLE_OPT, true, LIFECYCLE_OPT_DESCRIPTION);
        Option filterBy = new Option(FILTER_BY_OPT, true, FILTER_BY_OPT_DESCRIPTION);
        Option orderBy = new Option(ORDER_BY_OPT, true, ORDER_BY_OPT_DESCRIPTION);
        Option sortOrder = new Option(SORT_ORDER_OPT, true, SORT_ORDER_OPT_DESCRIPTION);
        Option offset = new Option(OFFSET_OPT, true, OFFSET_OPT_DESCRIPTION);
        Option numResults = new Option(NUM_RESULTS_OPT, true, NUM_RESULTS_OPT_DESCRIPTION);
        Option forceRerun = new Option(FORCE_RERUN_FLAG, false, FORCE_RERUN_FLAG_DESCRIPTION);
        Option doAs = new Option(DO_AS_OPT, true, DO_AS_OPT_DESCRIPTION);
        Option debug = new Option(DEBUG_OPTION, false, DEBUG_OPTION_DESCRIPTION);

        Option instanceTime = new Option(INSTANCE_TIME_OPT, true, INSTANCE_TIME_OPT_DESCRIPTION);

        Option allAttempts = new Option(ALL_ATTEMPTS, false, ALL_ATTEMPTS_DESCRIPTION);

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
        instanceOptions.addOption(doAs);
        instanceOptions.addOption(debug);
        instanceOptions.addOption(instanceTime);
        instanceOptions.addOption(allAttempts);

        return instanceOptions;
    }

    public void instanceCommand(CommandLine commandLine, FalconClient client) throws IOException {
        Set<String> optionsList = new HashSet<String>();
        for (Option option : commandLine.getOptions()) {
            optionsList.add(option.getOpt());
        }

        String result;
        String type = commandLine.getOptionValue(TYPE_OPT);
        String entity = commandLine.getOptionValue(ENTITY_NAME_OPT);
        String instanceTime = commandLine.getOptionValue(INSTANCE_TIME_OPT);
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
        String doAsUser = commandLine.getOptionValue(DO_AS_OPT);
        Integer offset = parseIntegerInput(commandLine.getOptionValue(OFFSET_OPT), 0, "offset");
        Integer numResults = parseIntegerInput(commandLine.getOptionValue(NUM_RESULTS_OPT), null, "numResults");

        colo = getColo(colo);
        String instanceAction = "instance";
        validateSortOrder(sortOrder);
        validateInstanceCommands(optionsList, entity, type, colo);

        if (optionsList.contains(TRIAGE_OPT)) {
            validateNotEmpty(colo, COLO_OPT);
            validateNotEmpty(start, START_OPT);
            validateNotEmpty(type, TYPE_OPT);
            validateEntityTypeForSummary(type);
            validateNotEmpty(entity, ENTITY_NAME_OPT);
            result = client.triage(type, entity, start, colo).toString();
        } else if (optionsList.contains(DEPENDENCY_OPT)) {
            validateNotEmpty(instanceTime, INSTANCE_TIME_OPT);
            InstanceDependencyResult response = client.getInstanceDependencies(type, entity, instanceTime, colo);
            result = ResponseHelper.getString(response);

        } else if (optionsList.contains(RUNNING_OPT)) {
            validateOrderBy(orderBy, instanceAction);
            validateFilterBy(filterBy, instanceAction);
            result = ResponseHelper.getString(client.getRunningInstances(type,
                    entity, colo, lifeCycles, filterBy, orderBy, sortOrder, offset, numResults, doAsUser));
        } else if (optionsList.contains(STATUS_OPT) || optionsList.contains(LIST_OPT)) {
            boolean allAttempts = false;
            if (optionsList.contains(ALL_ATTEMPTS)) {
                allAttempts = true;
            }
            validateOrderBy(orderBy, instanceAction);
            validateFilterBy(filterBy, instanceAction);
            result = ResponseHelper.getString(client.getStatusOfInstances(type, entity, start, end, colo,
                    lifeCycles, filterBy, orderBy, sortOrder, offset, numResults, doAsUser, allAttempts));
        } else if (optionsList.contains(SUMMARY_OPT)) {
            validateOrderBy(orderBy, "summary");
            validateFilterBy(filterBy, "summary");
            result = ResponseHelper.getString(client.getSummaryOfInstances(type, entity, start, end, colo,
                    lifeCycles, filterBy, orderBy, sortOrder, doAsUser));
        } else if (optionsList.contains(KILL_OPT)) {
            validateNotEmpty(start, START_OPT);
            validateNotEmpty(end, END_OPT);
            result = ResponseHelper.getString(client.killInstances(type, entity, start, end, colo, clusters,
                    sourceClusters, lifeCycles, doAsUser));
        } else if (optionsList.contains(SUSPEND_OPT)) {
            validateNotEmpty(start, START_OPT);
            validateNotEmpty(end, END_OPT);
            result = ResponseHelper.getString(client.suspendInstances(type, entity, start, end, colo, clusters,
                    sourceClusters, lifeCycles, doAsUser));
        } else if (optionsList.contains(RESUME_OPT)) {
            validateNotEmpty(start, START_OPT);
            validateNotEmpty(end, END_OPT);
            result = ResponseHelper.getString(client.resumeInstances(type, entity, start, end, colo, clusters,
                    sourceClusters, lifeCycles, doAsUser));
        } else if (optionsList.contains(RERUN_OPT)) {
            validateNotEmpty(start, START_OPT);
            validateNotEmpty(end, END_OPT);
            boolean isForced = false;
            if (optionsList.contains(FORCE_RERUN_FLAG)) {
                isForced = true;
            }
            result = ResponseHelper.getString(client.rerunInstances(type, entity, start, end, filePath, colo,
                    clusters, sourceClusters, lifeCycles, isForced, doAsUser));
        } else if (optionsList.contains(LOG_OPT)) {
            validateOrderBy(orderBy, instanceAction);
            validateFilterBy(filterBy, instanceAction);
            result = ResponseHelper.getString(client.getLogsOfInstances(type, entity, start, end, colo, runId,
                    lifeCycles, filterBy, orderBy, sortOrder, offset, numResults, doAsUser), runId);
        } else if (optionsList.contains(PARARMS_OPT)) {
            // start time is the nominal time of instance
            result = ResponseHelper.getString(client.getParamsOfInstance(type, entity,
                    start, colo, lifeCycles, doAsUser));
        } else if (optionsList.contains(LISTING_OPT)) {
            result = ResponseHelper.getString(client.getFeedInstanceListing(type, entity, start, end, colo, doAsUser));
        } else {
            throw new FalconCLIException("Invalid command");
        }

        OUT.get().println(result);
    }

    private void validateInstanceCommands(Set<String> optionsList,
                                          String entity, String type,
                                          String colo) {

        validateNotEmpty(entity, ENTITY_NAME_OPT);
        validateNotEmpty(type, TYPE_OPT);
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

    private List<LifeCycle> getLifeCycle(String lifeCycleValue) {

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
