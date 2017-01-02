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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.client.FalconCLIConstants;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.client.FalconClient;
import org.apache.falcon.resource.ExtensionInstanceList;
import org.apache.falcon.resource.ExtensionJobList;

import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Falcon extensions Command Line Interface - wraps the RESTful API for extensions.
 */
public class FalconExtensionCLI {
    public static final AtomicReference<PrintStream> OUT = new AtomicReference<>(System.out);

    // Extension commands
    public static final String ENUMERATE_OPT = "enumerate";
    public static final String DEFINITION_OPT = "definition";
    public static final String DESCRIBE_OPT = "describe";
    public static final String INSTANCES_OPT = "instances";
    public static final String UNREGISTER_OPT = "unregister";
    public static final String DETAIL_OPT = "detail";
    public static final String REGISTER_OPT = "register";

    // Input parameters
    public static final String EXTENSION_NAME_OPT = "extensionName";
    public static final String JOB_NAME_OPT = "jobName";
    public static final String DESCRIPTION = "description";
    public static final String PATH = "path";

    public FalconExtensionCLI() {
    }

    public void extensionCommand(CommandLine commandLine, FalconClient client) {
        Set<String> optionsList = new HashSet<>();
        for (Option option : commandLine.getOptions()) {
            optionsList.add(option.getOpt());
        }

        String result;
        String extensionName = commandLine.getOptionValue(EXTENSION_NAME_OPT);
        String jobName = commandLine.getOptionValue(JOB_NAME_OPT);
        String filePath = commandLine.getOptionValue(FalconCLIConstants.FILE_PATH_OPT);
        String doAsUser = commandLine.getOptionValue(FalconCLIConstants.DO_AS_OPT);
        String path = commandLine.getOptionValue(FalconCLIConstants.PATH);
        String description = commandLine.getOptionValue(FalconCLIConstants.DESCRIPTION);

        if (optionsList.contains(ENUMERATE_OPT)) {
            result = client.enumerateExtensions().getMessage();
            result = prettyPrintJson(result);
        } else if (optionsList.contains(DEFINITION_OPT)) {
            validateRequiredParameter(extensionName, EXTENSION_NAME_OPT);
            result = client.getExtensionDefinition(extensionName).getMessage();
            result = prettyPrintJson(result);
        } else if (optionsList.contains(DESCRIBE_OPT)) {
            validateRequiredParameter(extensionName, EXTENSION_NAME_OPT);
            result = client.getExtensionDescription(extensionName).getMessage();
        } else if (optionsList.contains(UNREGISTER_OPT)) {
            validateRequiredParameter(extensionName, EXTENSION_NAME_OPT);
            result = client.unregisterExtension(extensionName).getMessage();
        } else if (optionsList.contains(DETAIL_OPT)) {
            if (optionsList.contains(JOB_NAME_OPT)) {
                validateRequiredParameter(jobName, JOB_NAME_OPT);
                result = client.getExtensionJobDetails(jobName).getMessage();
                result = prettyPrintJson(result);
            } else {
                validateRequiredParameter(extensionName, EXTENSION_NAME_OPT);
                result = client.getExtensionDetail(extensionName).getMessage();
                result = prettyPrintJson(result);
            }
        } else if (optionsList.contains(FalconCLIConstants.SUBMIT_OPT)) {
            validateRequiredParameter(extensionName, EXTENSION_NAME_OPT);
            validateRequiredParameter(jobName, JOB_NAME_OPT);
            validateRequiredParameter(filePath, FalconCLIConstants.FILE_PATH_OPT);
            result = client.submitExtensionJob(extensionName, jobName, filePath, doAsUser).getMessage();
        } else if (optionsList.contains(REGISTER_OPT)) {
            validateRequiredParameter(extensionName, EXTENSION_NAME_OPT);
            validateRequiredParameter(path, PATH);
            result = client.registerExtension(extensionName, path, description).getMessage();
        } else if (optionsList.contains(FalconCLIConstants.SUBMIT_AND_SCHEDULE_OPT)) {
            validateRequiredParameter(extensionName, EXTENSION_NAME_OPT);
            validateRequiredParameter(jobName, JOB_NAME_OPT);
            validateRequiredParameter(filePath, FalconCLIConstants.FILE_PATH_OPT);
            result = client.submitAndScheduleExtensionJob(extensionName, jobName, filePath, doAsUser).getMessage();
        } else if (optionsList.contains(FalconCLIConstants.UPDATE_OPT)) {
            validateRequiredParameter(jobName, JOB_NAME_OPT);
            validateRequiredParameter(filePath, FalconCLIConstants.FILE_PATH_OPT);
            result = client.updateExtensionJob(jobName, filePath, doAsUser).getMessage();
        } else if (optionsList.contains(FalconCLIConstants.VALIDATE_OPT)) {
            validateRequiredParameter(extensionName, EXTENSION_NAME_OPT);
            validateRequiredParameter(filePath, FalconCLIConstants.FILE_PATH_OPT);
            result = client.validateExtensionJob(extensionName, jobName, filePath, doAsUser).getMessage();
        } else if (optionsList.contains(FalconCLIConstants.SCHEDULE_OPT)) {
            validateRequiredParameter(jobName, JOB_NAME_OPT);
            result = client.scheduleExtensionJob(jobName, doAsUser).getMessage();
        } else if (optionsList.contains(FalconCLIConstants.SUSPEND_OPT)) {
            validateRequiredParameter(jobName, JOB_NAME_OPT);
            result = client.suspendExtensionJob(jobName, doAsUser).getMessage();
        } else if (optionsList.contains(FalconCLIConstants.RESUME_OPT)) {
            validateRequiredParameter(jobName, JOB_NAME_OPT);
            result = client.resumeExtensionJob(jobName, doAsUser).getMessage();
        } else if (optionsList.contains(FalconCLIConstants.DELETE_OPT)) {
            validateRequiredParameter(jobName, JOB_NAME_OPT);
            result = client.deleteExtensionJob(jobName, doAsUser).getMessage();
        } else if (optionsList.contains(FalconCLIConstants.LIST_OPT)) {
            validateRequiredParameter(extensionName, EXTENSION_NAME_OPT);
            ExtensionJobList jobs = client.listExtensionJob(extensionName, doAsUser,
                    commandLine.getOptionValue(FalconCLIConstants.SORT_ORDER_OPT),
                    commandLine.getOptionValue(FalconCLIConstants.OFFSET_OPT),
                    commandLine.getOptionValue(FalconCLIConstants.NUM_RESULTS_OPT),
                    commandLine.getOptionValue(FalconCLIConstants.FIELDS_OPT));
            result = jobs != null ? jobs.toString() : "No extension job (" + extensionName + ") found.";
        } else if (optionsList.contains(INSTANCES_OPT)) {
            validateRequiredParameter(jobName, JOB_NAME_OPT);
            ExtensionInstanceList instances = client.listExtensionInstance(jobName, doAsUser,
                    commandLine.getOptionValue(FalconCLIConstants.FIELDS_OPT),
                    commandLine.getOptionValue(FalconCLIConstants.START_OPT),
                    commandLine.getOptionValue(FalconCLIConstants.END_OPT),
                    commandLine.getOptionValue(FalconCLIConstants.INSTANCE_STATUS_OPT),
                    commandLine.getOptionValue(FalconCLIConstants.ORDER_BY_OPT),
                    commandLine.getOptionValue(FalconCLIConstants.SORT_ORDER_OPT),
                    commandLine.getOptionValue(FalconCLIConstants.OFFSET_OPT),
                    commandLine.getOptionValue(FalconCLIConstants.NUM_RESULTS_OPT));
            result = instances != null ? instances.toString() : "No instance (" + jobName + ") found.";
        } else {
            throw new FalconCLIException("Invalid/missing extension command. Supported commands include "
                    + "enumerate, definition, describe, list, instances, submit, submitAndSchedule, "
                    + "schedule, suspend, resume, delete, update, validate. "
                    + "Please refer to Falcon CLI twiki for more details.");
        }
        OUT.get().println(result);
    }

    public Options createExtensionOptions() {
        Options extensionOptions = new Options();

        Option enumerate = new Option(ENUMERATE_OPT, false, "Enumerate all extensions");
        Option definition = new Option(DEFINITION_OPT, false, "Get extension definition");
        Option describe = new Option(DESCRIBE_OPT, false, "Get extension description");
        Option list = new Option(FalconCLIConstants.LIST_OPT, false, "List extension jobs and associated entities");
        Option instances = new Option(INSTANCES_OPT, false, "List instances of an extension job");
        Option submit = new Option(FalconCLIConstants.SUBMIT_OPT, false, "Submit an extension job");
        Option submitAndSchedule = new Option(FalconCLIConstants.SUBMIT_AND_SCHEDULE_OPT, false,
                "Submit and schedule an extension job");
        Option update = new Option(FalconCLIConstants.UPDATE_OPT, false, "Update an extension job");
        Option validate = new Option(FalconCLIConstants.VALIDATE_OPT, false, "Validate an extension job");
        Option schedule = new Option(FalconCLIConstants.SCHEDULE_OPT, false, "Schedule an extension job");
        Option suspend = new Option(FalconCLIConstants.SUSPEND_OPT, false, "Suspend an extension job");
        Option resume = new Option(FalconCLIConstants.RESUME_OPT, false, "Resume an extension job");
        Option delete = new Option(FalconCLIConstants.DELETE_OPT, false, "Delete an extension job");
        Option unregister = new Option(FalconCLIConstants.UREGISTER, false, "Un-register an extension. This will make"
                + " the extension unavailable for instantiation");
        Option detail = new Option(FalconCLIConstants.DETAIL, false, "Show details of a given extension");
        Option register = new Option(FalconCLIConstants.REGISTER, false, "Register an extension with Falcon. This will "
                + "make the extension available for instantiation for all users.");

        OptionGroup group = new OptionGroup();
        group.addOption(enumerate);
        group.addOption(definition);
        group.addOption(describe);
        group.addOption(list);
        group.addOption(instances);
        group.addOption(submit);
        group.addOption(submitAndSchedule);
        group.addOption(update);
        group.addOption(validate);
        group.addOption(schedule);
        group.addOption(suspend);
        group.addOption(resume);
        group.addOption(delete);
        group.addOption(unregister);
        group.addOption(detail);
        group.addOption(register);
        extensionOptions.addOptionGroup(group);

        Option url = new Option(FalconCLIConstants.URL_OPTION, true, "Falcon URL");
        Option doAs = new Option(FalconCLIConstants.DO_AS_OPT, true, "doAs user");
        Option debug = new Option(FalconCLIConstants.DEBUG_OPTION, false,
                "Use debug mode to see debugging statements on stdout");
        Option extensionName = new Option(EXTENSION_NAME_OPT, true, "Extension name");
        Option jobName = new Option(JOB_NAME_OPT, true, "Extension job name");
        Option instanceStatus = new Option(FalconCLIConstants.INSTANCE_STATUS_OPT, true, "Instance status");
        Option sortOrder = new Option(FalconCLIConstants.SORT_ORDER_OPT, true, "asc or desc order for results");
        Option offset = new Option(FalconCLIConstants.OFFSET_OPT, true, "Start returning instances from this offset");
        Option numResults = new Option(FalconCLIConstants.NUM_RESULTS_OPT, true,
                "Number of results to return per request");
        Option fields = new Option(FalconCLIConstants.FIELDS_OPT, true, "Entity fields to show for a request");
        Option start = new Option(FalconCLIConstants.START_OPT, true, "Start time of instances");
        Option end = new Option(FalconCLIConstants.END_OPT, true, "End time of instances");
        Option status = new Option(FalconCLIConstants.STATUS_OPT, true, "Filter returned instances by status");
        Option orderBy = new Option(FalconCLIConstants.ORDER_BY_OPT, true, "Order returned instances by this field");
        Option filePath = new Option(FalconCLIConstants.FILE_PATH_OPT, true, "File path of extension parameters");
        Option path = new Option(FalconCLIConstants.PATH, true, "Path of hdfs location for extension");
        Option description = new Option(FalconCLIConstants.DESCRIPTION, true, "Short Description for extension");

        extensionOptions.addOption(url);
        extensionOptions.addOption(doAs);
        extensionOptions.addOption(debug);
        extensionOptions.addOption(extensionName);
        extensionOptions.addOption(jobName);
        extensionOptions.addOption(instanceStatus);
        extensionOptions.addOption(sortOrder);
        extensionOptions.addOption(offset);
        extensionOptions.addOption(numResults);
        extensionOptions.addOption(fields);
        extensionOptions.addOption(start);
        extensionOptions.addOption(end);
        extensionOptions.addOption(status);
        extensionOptions.addOption(orderBy);
        extensionOptions.addOption(filePath);
        extensionOptions.addOption(path);
        extensionOptions.addOption(description);

        return extensionOptions;
    }

    private void validateRequiredParameter(final String parameter, final String parameterName) {
        if (StringUtils.isBlank(parameter)) {
            throw new FalconCLIException("The parameter " + parameterName + " cannot be null or empty");
        }
    }

    private static String prettyPrintJson(final String jsonString) {
        if (StringUtils.isBlank(jsonString)) {
            return "No result returned";
        }
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonParser jp = new JsonParser();
        JsonElement je = jp.parse(jsonString.trim());
        return gson.toJson(je);
    }
}
