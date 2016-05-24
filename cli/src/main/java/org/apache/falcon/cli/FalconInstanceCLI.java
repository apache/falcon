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
import org.apache.falcon.FalconCLIConstants;
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

    private static final String FORCE_RERUN_FLAG = "force";
    private static final String INSTANCE_TIME_OPT = "instanceTime";
    private static final String RUNNING_OPT = "running";
    private static final String KILL_OPT = "kill";
    private static final String RERUN_OPT = "rerun";
    private static final String LOG_OPT = "logs";
    private static final String SEARCH_OPT = "search";
    private static final String ALL_ATTEMPTS = "allAttempts";
    private static final String RUNID_OPT = "runid";
    private static final String CLUSTERS_OPT = "clusters";
    private static final String SOURCECLUSTER_OPT = "sourceClusters";
    private static final String LIFECYCLE_OPT = "lifecycle";
    private static final String PARARMS_OPT = "params";
    private static final String LISTING_OPT = "listing";
    private static final String TRIAGE_OPT = "triage";

    public FalconInstanceCLI() throws Exception {
        super();
    }

    public Options createInstanceOptions() {

        Options instanceOptions = new Options();

        Option running = new Option(RUNNING_OPT, false,
                "Gets running process instances for a given process");
        Option list = new Option(FalconCLIConstants.LIST_OPT, false,
                "Gets all instances for a given process in the range start time and optional end time");
        Option status = new Option(FalconCLIConstants.STATUS_OPT, false,
                "Gets status of process instances for a given process in the range start time and optional end time");
        Option summary = new Option(FalconCLIConstants.SUMMARY_OPT, false,
                "Gets summary of instances for a given process in the range start time and optional end time");
        Option kill = new Option(KILL_OPT, false,
                "Kills active process instances for a given process in the range start time and optional end time");
        Option suspend = new Option(FalconCLIConstants.SUSPEND_OPT, false,
                "Suspends active process instances for a given process in the range start time and optional end time");
        Option resume = new Option(FalconCLIConstants.RESUME_OPT, false,
                "Resumes suspended process instances for a given process "
                        + "in the range start time and optional end time");
        Option rerun = new Option(RERUN_OPT, false,
                "Reruns process instances for a given process in the range start time and "
                        + "optional end time and overrides properties present in job.properties file");
        Option logs = new Option(LOG_OPT, false,
                "Logs print the logs for process instances for a given process in "
                        + "the range start time and optional end time");
        Option params = new Option(PARARMS_OPT, false,
                "Displays the workflow parameters for a given instance of specified nominal time"
                        + "start time represents nominal time and end time is not considered");
        Option listing = new Option(LISTING_OPT, false,
                "Displays feed listing and their status between a start and end time range.");
        Option dependency = new Option(FalconCLIConstants.DEPENDENCY_OPT, false,
                "Displays dependent instances for a specified instance.");
        Option triage = new Option(TRIAGE_OPT, false,
                "Triage a feed or process instance and find the failures in it's lineage.");
        Option search = new Option(SEARCH_OPT, false,
                "Search instances with filtering criteria on the entity, instance time and status.");

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
        group.addOption(search);

        Option url = new Option(FalconCLIConstants.URL_OPTION, true, "Falcon URL");
        Option start = new Option(FalconCLIConstants.START_OPT, true,
                "Start time is required for commands, status, kill, suspend, resume and re-run"
                        + "and it is nominal time while displaying workflow params");
        Option end = new Option(FalconCLIConstants.END_OPT, true,
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
        Option filePath = new Option(FalconCLIConstants.FILE_PATH_OPT, true,
                "Path to job.properties file is required for rerun command, "
                        + "it should contain name=value pair for properties to override for rerun");
        Option entityType = new Option(FalconCLIConstants.TYPE_OPT, true,
                "Entity type, can be feed or process xml");
        Option entityName = new Option(FalconCLIConstants.ENTITY_NAME_OPT, true,
                "Entity name, can be feed or process name");
        Option colo = new Option(FalconCLIConstants.COLO_OPT, true,
                "Colo on which the cmd has to be executed");
        Option lifecycle = new Option(LIFECYCLE_OPT, true,
                "describes life cycle of entity , for feed it can be replication/retention "
                        + "and for process it can be execution");
        Option filterBy = new Option(FalconCLIConstants.FILTER_BY_OPT, true,
                "Filter returned instances by the specified fields");
        Option orderBy = new Option(FalconCLIConstants.ORDER_BY_OPT, true,
                "Order returned instances by this field");
        Option sortOrder = new Option(FalconCLIConstants.SORT_ORDER_OPT, true, "asc or desc order for results");
        Option offset = new Option(FalconCLIConstants.OFFSET_OPT, true,
                "Start returning instances from this offset");
        Option numResults = new Option(FalconCLIConstants.NUM_RESULTS_OPT, true,
                "Number of results to return per request");
        Option forceRerun = new Option(FORCE_RERUN_FLAG, false,
                "Flag to forcefully rerun entire workflow of an instance");
        Option doAs = new Option(FalconCLIConstants.DO_AS_OPT, true, "doAs user");
        Option debug = new Option(FalconCLIConstants.DEBUG_OPTION, false, "Use debug mode to see"
                + " debugging statements on stdout");
        Option instanceTime = new Option(INSTANCE_TIME_OPT, true, "Time for an instance");
        Option instanceStatus = new Option(FalconCLIConstants.INSTANCE_STATUS_OPT, true, "Instance status");
        Option nameSubsequence = new Option(FalconCLIConstants.NAMESEQ_OPT, true, "Subsequence of entity name");
        Option tagKeywords = new Option(FalconCLIConstants.TAGKEYS_OPT, true, "Keywords in tags");
        Option allAttempts = new Option(ALL_ATTEMPTS, false, "To get all attempts of corresponding instances");

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
        instanceOptions.addOption(instanceStatus);
        instanceOptions.addOption(nameSubsequence);
        instanceOptions.addOption(tagKeywords);
        instanceOptions.addOption(allAttempts);

        return instanceOptions;
    }

    public void instanceCommand(CommandLine commandLine, FalconClient client) throws IOException {
        Set<String> optionsList = new HashSet<String>();
        for (Option option : commandLine.getOptions()) {
            optionsList.add(option.getOpt());
        }

        String result;
        String type = commandLine.getOptionValue(FalconCLIConstants.TYPE_OPT);
        String entity = commandLine.getOptionValue(FalconCLIConstants.ENTITY_NAME_OPT);
        String instanceTime = commandLine.getOptionValue(INSTANCE_TIME_OPT);
        String start = commandLine.getOptionValue(FalconCLIConstants.START_OPT);
        String end = commandLine.getOptionValue(FalconCLIConstants.END_OPT);
        String status = commandLine.getOptionValue(FalconCLIConstants.INSTANCE_STATUS_OPT);
        String nameSubsequence = commandLine.getOptionValue(FalconCLIConstants.NAMESEQ_OPT);
        String tagKeywords = commandLine.getOptionValue(FalconCLIConstants.TAGKEYS_OPT);
        String filePath = commandLine.getOptionValue(FalconCLIConstants.FILE_PATH_OPT);
        String runId = commandLine.getOptionValue(RUNID_OPT);
        String colo = commandLine.getOptionValue(FalconCLIConstants.COLO_OPT);
        String clusters = commandLine.getOptionValue(CLUSTERS_OPT);
        String sourceClusters = commandLine.getOptionValue(SOURCECLUSTER_OPT);
        List<LifeCycle> lifeCycles = getLifeCycle(commandLine.getOptionValue(LIFECYCLE_OPT));
        String filterBy = commandLine.getOptionValue(FalconCLIConstants.FILTER_BY_OPT);
        String orderBy = commandLine.getOptionValue(FalconCLIConstants.ORDER_BY_OPT);
        String sortOrder = commandLine.getOptionValue(FalconCLIConstants.SORT_ORDER_OPT);
        String doAsUser = commandLine.getOptionValue(FalconCLIConstants.DO_AS_OPT);
        Integer offset = parseIntegerInput(commandLine.getOptionValue(FalconCLIConstants.OFFSET_OPT), 0, "offset");
        Integer numResults = parseIntegerInput(commandLine.getOptionValue(FalconCLIConstants.NUM_RESULTS_OPT),
                null, "numResults");

        colo = getColo(colo);
        String instanceAction = "instance";
        validateSortOrder(sortOrder);
        if (!optionsList.contains(SEARCH_OPT)) {
            validateInstanceCommands(optionsList, entity, type, colo);
        }

        if (optionsList.contains(TRIAGE_OPT)) {
            validateNotEmpty(colo, FalconCLIConstants.COLO_OPT);
            validateNotEmpty(start, FalconCLIConstants.START_OPT);
            validateNotEmpty(type, FalconCLIConstants.TYPE_OPT);
            validateEntityTypeForSummary(type);
            validateNotEmpty(entity, FalconCLIConstants.ENTITY_NAME_OPT);
            result = client.triage(type, entity, start, colo).toString();
        } else if (optionsList.contains(FalconCLIConstants.DEPENDENCY_OPT)) {
            validateNotEmpty(instanceTime, INSTANCE_TIME_OPT);
            InstanceDependencyResult response = client.getInstanceDependencies(type, entity, instanceTime, colo);
            result = ResponseHelper.getString(response);

        } else if (optionsList.contains(RUNNING_OPT)) {
            validateOrderBy(orderBy, instanceAction);
            validateFilterBy(filterBy, instanceAction);
            result = ResponseHelper.getString(client.getRunningInstances(type,
                    entity, colo, lifeCycles, filterBy, orderBy, sortOrder, offset, numResults, doAsUser));
        } else if (optionsList.contains(FalconCLIConstants.STATUS_OPT)
                || optionsList.contains(FalconCLIConstants.LIST_OPT)) {
            boolean allAttempts = false;
            if (optionsList.contains(ALL_ATTEMPTS)) {
                allAttempts = true;
            }
            validateOrderBy(orderBy, instanceAction);
            validateFilterBy(filterBy, instanceAction);
            result = ResponseHelper.getString(client.getStatusOfInstances(type, entity, start, end, colo,
                    lifeCycles, filterBy, orderBy, sortOrder, offset, numResults, doAsUser, allAttempts));
        } else if (optionsList.contains(FalconCLIConstants.SUMMARY_OPT)) {
            validateOrderBy(orderBy, "summary");
            validateFilterBy(filterBy, "summary");
            result = ResponseHelper.getString(client.getSummaryOfInstances(type, entity, start, end, colo,
                    lifeCycles, filterBy, orderBy, sortOrder, doAsUser));
        } else if (optionsList.contains(KILL_OPT)) {
            validateNotEmpty(start, FalconCLIConstants.START_OPT);
            validateNotEmpty(end, FalconCLIConstants.END_OPT);
            result = ResponseHelper.getString(client.killInstances(type, entity, start, end, colo, clusters,
                    sourceClusters, lifeCycles, doAsUser));
        } else if (optionsList.contains(FalconCLIConstants.SUSPEND_OPT)) {
            validateNotEmpty(start, FalconCLIConstants.START_OPT);
            validateNotEmpty(end, FalconCLIConstants.END_OPT);
            result = ResponseHelper.getString(client.suspendInstances(type, entity, start, end, colo, clusters,
                    sourceClusters, lifeCycles, doAsUser));
        } else if (optionsList.contains(FalconCLIConstants.RESUME_OPT)) {
            validateNotEmpty(start, FalconCLIConstants.START_OPT);
            validateNotEmpty(end, FalconCLIConstants.END_OPT);
            result = ResponseHelper.getString(client.resumeInstances(type, entity, start, end, colo, clusters,
                    sourceClusters, lifeCycles, doAsUser));
        } else if (optionsList.contains(RERUN_OPT)) {
            validateNotEmpty(start, FalconCLIConstants.START_OPT);
            validateNotEmpty(end, FalconCLIConstants.END_OPT);
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
        } else if (optionsList.contains(SEARCH_OPT)) {
            result = ResponseHelper.getString(client.searchInstances(
                    type, nameSubsequence, tagKeywords, start, end, status, orderBy, offset, numResults));
        } else {
            throw new FalconCLIException("Invalid/missing instance command. Supported commands include "
                    + "running, status, kill, suspend, resume, rerun, logs, search. "
                    + "Please refer to Falcon CLI twiki for more details.");
        }

        OUT.get().println(result);
    }

    private void validateInstanceCommands(Set<String> optionsList,
                                          String entity, String type,
                                          String colo) {

        validateNotEmpty(entity, FalconCLIConstants.ENTITY_NAME_OPT);
        validateNotEmpty(type, FalconCLIConstants.TYPE_OPT);
        validateNotEmpty(colo, FalconCLIConstants.COLO_OPT);

        if (optionsList.contains(CLUSTERS_OPT)) {
            if (optionsList.contains(RUNNING_OPT)
                    || optionsList.contains(LOG_OPT)
                    || optionsList.contains(FalconCLIConstants.STATUS_OPT)
                    || optionsList.contains(FalconCLIConstants.SUMMARY_OPT)) {
                throw new FalconCLIException("Invalid argument: clusters");
            }
        }

        if (optionsList.contains(SOURCECLUSTER_OPT)) {
            if (optionsList.contains(RUNNING_OPT)
                    || optionsList.contains(LOG_OPT)
                    || optionsList.contains(FalconCLIConstants.STATUS_OPT)
                    || optionsList.contains(FalconCLIConstants.SUMMARY_OPT) || !type.equals("feed")) {
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
