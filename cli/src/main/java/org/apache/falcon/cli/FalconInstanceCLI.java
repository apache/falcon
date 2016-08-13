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
import org.apache.falcon.client.FalconCLIConstants;
import org.apache.falcon.ValidationUtil;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.ResponseHelper;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.client.FalconClient;
import org.apache.falcon.resource.InstanceDependencyResult;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.falcon.client.FalconCLIConstants.RUNNING_OPT;
import static org.apache.falcon.client.FalconCLIConstants.RUNNING_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.LIST_OPT;
import static org.apache.falcon.client.FalconCLIConstants.LIST_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.STATUS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.STATUS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.SUMMARY_OPT;
import static org.apache.falcon.client.FalconCLIConstants.SUMMARY_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.KILL_OPT;
import static org.apache.falcon.client.FalconCLIConstants.KILL_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.SUSPEND_OPT;
import static org.apache.falcon.client.FalconCLIConstants.SUSPEND_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.RESUME_OPT;
import static org.apache.falcon.client.FalconCLIConstants.RESUME_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.RERUN_OPT;
import static org.apache.falcon.client.FalconCLIConstants.LOG_OPT;
import static org.apache.falcon.client.FalconCLIConstants.LOG_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.PARARMS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.PARARMS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.LISTING_OPT;
import static org.apache.falcon.client.FalconCLIConstants.LISTING_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.DEPENDENCY_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.DEPENDENCY_OPT;
import static org.apache.falcon.client.FalconCLIConstants.TRIAGE_OPT;
import static org.apache.falcon.client.FalconCLIConstants.TRIAGE_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.SEARCH_OPT;
import static org.apache.falcon.client.FalconCLIConstants.URL_OPTION;
import static org.apache.falcon.client.FalconCLIConstants.START_OPT;
import static org.apache.falcon.client.FalconCLIConstants.START_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.END_OPT;
import static org.apache.falcon.client.FalconCLIConstants.END_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.RUNID_OPT;
import static org.apache.falcon.client.FalconCLIConstants.CLUSTERS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.CLUSTERS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.SOURCECLUSTER_OPT;
import static org.apache.falcon.client.FalconCLIConstants.SOURCECLUSTER_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.FILE_PATH_OPT;
import static org.apache.falcon.client.FalconCLIConstants.FILE_PATH_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.TYPE_OPT;
import static org.apache.falcon.client.FalconCLIConstants.TYPE_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.ENTITY_NAME_OPT;
import static org.apache.falcon.client.FalconCLIConstants.ENTITY_NAME_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.COLO_OPT;
import static org.apache.falcon.client.FalconCLIConstants.COLO_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.RERUN_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.URL_OPTION_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.RUNID_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.LIFECYCLE_OPT;
import static org.apache.falcon.client.FalconCLIConstants.LIFECYCLE_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.FILTER_BY_OPT;
import static org.apache.falcon.client.FalconCLIConstants.FILTER_BY_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.ORDER_BY_OPT;
import static org.apache.falcon.client.FalconCLIConstants.ORDER_BY_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.SORT_ORDER_OPT;
import static org.apache.falcon.client.FalconCLIConstants.SORT_ORDER_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.OFFSET_OPT;
import static org.apache.falcon.client.FalconCLIConstants.OFFSET_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.NUM_RESULTS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.NUM_RESULTS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.FORCE_RERUN_FLAG;
import static org.apache.falcon.client.FalconCLIConstants.FORCE_RERUN_FLAG_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.DO_AS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.DO_AS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.DEBUG_OPTION;
import static org.apache.falcon.client.FalconCLIConstants.DEBUG_OPTION_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.INSTANCE_TIME_OPT;
import static org.apache.falcon.client.FalconCLIConstants.INSTANCE_TIME_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.ALL_ATTEMPTS;
import static org.apache.falcon.client.FalconCLIConstants.ALL_ATTEMPTS_DESCRIPTION;
import static org.apache.falcon.ValidationUtil.getLifeCycle;
import static org.apache.falcon.ValidationUtil.validateSortOrder;
import static org.apache.falcon.ValidationUtil.validateNotEmpty;


/**
 * Instance extension to Falcon Command Line Interface - wraps the RESTful API for instances.
 */
public class FalconInstanceCLI extends FalconCLI {

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
        Option instanceStatus = new Option(FalconCLIConstants.INSTANCE_STATUS_OPT, true, "Instance status");
        Option nameSubsequence = new Option(FalconCLIConstants.NAMESEQ_OPT, true, "Subsequence of entity name");
        Option tagKeywords = new Option(FalconCLIConstants.TAGKEYS_OPT, true, "Keywords in tags");

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
            ValidationUtil.validateEntityTypeForSummary(type);
            validateNotEmpty(entity, FalconCLIConstants.ENTITY_NAME_OPT);
            result = client.triage(type, entity, start, colo).toString();
        } else if (optionsList.contains(FalconCLIConstants.DEPENDENCY_OPT)) {
            validateNotEmpty(instanceTime, INSTANCE_TIME_OPT);
            InstanceDependencyResult response = client.getInstanceDependencies(type, entity, instanceTime, colo);
            result = ResponseHelper.getString(response);

        } else if (optionsList.contains(RUNNING_OPT)) {
            ValidationUtil.validateOrderBy(orderBy, instanceAction);
            ValidationUtil.validateFilterBy(filterBy, instanceAction);
            result = ResponseHelper.getString(client.getRunningInstances(type,
                    entity, colo, lifeCycles, filterBy, orderBy, sortOrder, offset, numResults, doAsUser));
        } else if (optionsList.contains(FalconCLIConstants.STATUS_OPT)
                || optionsList.contains(FalconCLIConstants.LIST_OPT)) {
            boolean allAttempts = false;
            if (optionsList.contains(ALL_ATTEMPTS)) {
                allAttempts = true;
            }
            ValidationUtil.validateOrderBy(orderBy, instanceAction);
            ValidationUtil.validateFilterBy(filterBy, instanceAction);
            result = ResponseHelper.getString(client.getStatusOfInstances(type, entity, start, end, colo,
                    lifeCycles, filterBy, orderBy, sortOrder, offset, numResults, doAsUser, allAttempts));
        } else if (optionsList.contains(FalconCLIConstants.SUMMARY_OPT)) {
            ValidationUtil.validateOrderBy(orderBy, "summary");
            ValidationUtil.validateFilterBy(filterBy, "summary");
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
            ValidationUtil.validateOrderBy(orderBy, instanceAction);
            ValidationUtil.validateFilterBy(filterBy, instanceAction);
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

}
