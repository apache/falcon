/*
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

package org.apache.ivory.cli;

import java.net.ConnectException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.ivory.client.IvoryCLIException;
import org.apache.ivory.client.IvoryClient;

import com.sun.jersey.api.client.ClientHandlerException;

public class IvoryCLI {

	public static final String IVORY_URL = "IVORY_URL";
	public static final String URL_OPTION = "url";
	public static final String VERSION_OPTION = "version";
	public static final String ADMIN_CMD = "admin";
	public static final String HELP_CMD = "help";
	private static final String VERSION_CMD = "version";

	public static final String ENTITY_CMD = "entity";
	public static final String ENTITY_TYPE_OPT = "entityType";
	public static final String ENTITY_NAME_OPT = "entityName";
	public static final String FILE_PATH_OPT = "filePath";
	public static final String SUBMIT_OPT = "submit";
	public static final String SCHEDULE_OPT = "schedule";
	public static final String SUSPEND_OPT = "suspend";
	public static final String RESUME_OPT = "resume";
	public static final String DELETE_OPT = "delete";
	public static final String SUBMIT_AND_SCHEDULE_OPT = "submitAndSchedule";
	public static final String VALIDATE_OPT = "validate";
	public static final String STATUS_OPT = "status";
	public static final String DEFINITION_OPT = "definition";
	public static final String DEPENDENCY_OPT = "dependency";

	public static final String INSTANCE_CMD = "instance";
	public static final String PROCESS_OPT = "processName";
	public static final String START_OPT = "start";
	public static final String END_OPT = "end";
	public static final String RUNNING_OPT = "running";
	public static final String KILL_OPT = "kill";
	public static final String RERUN_OPT = "rerun";

	/**
	 * Entry point for the Ivory CLI when invoked from the command line. Upon
	 * completion this method exits the JVM with '0' (success) or '-1'
	 * (failure).
	 * 
	 * @param args
	 *            options and arguments for the Ivory CLI.
	 */
	public static void main(String[] args) {
		System.exit(new IvoryCLI().run(args));
	}

	// TODO help and headers
	private static final String[] IVORY_HELP = {
			"the env variable '" + IVORY_URL
					+ "' is used as default value for the '-" + URL_OPTION
					+ "' option",
			"custom headers for Ivory web services can be specified using '-D"
					+ IvoryClient.WS_HEADER_PREFIX + "NAME=VALUE'" };

	/**
	 * Run a CLI programmatically.
	 * <p/>
	 * It does not exit the JVM.
	 * <p/>
	 * A CLI instance can be used only once.
	 * 
	 * @param args
	 *            options and arguments for the Oozie CLI.
	 * @return '0' (success), '-1' (failure).
	 */
	public synchronized int run(String[] args) {

		CLIParser parser = new CLIParser("ivory", IVORY_HELP);

		parser.addCommand(ADMIN_CMD, "", "admin operations",
				createAdminOptions(), true);
		parser.addCommand(HELP_CMD, "", "display usage", new Options(), false);
		parser.addCommand(VERSION_CMD, "", "show client version",
				new Options(), false);
		parser.addCommand(
				ENTITY_CMD,
				"",
				"Entity opertions like submit, suspend, resume, delete, status, defintion, submitAndSchedule",
				entityOptions(), false);
		parser.addCommand(
				INSTANCE_CMD,
				"",
				"Process instances operation like running, status, kill, suspend, resume, rerun",
				instanceOptions(), false);

		try {
			CLIParser.Command command = parser.parse(args);
			if (command.getName().equals(HELP_CMD)) {
				parser.showHelp();
			} else if (command.getName().equals(ADMIN_CMD)) {
				adminCommand(command.getCommandLine());
			} else if (command.getName().equals(ENTITY_CMD)) {
				entityCommand(command.getCommandLine());
			} else if (command.getName().equals(INSTANCE_CMD)) {
				instanceCommand(command.getCommandLine());
			}

			return 0;
		} catch (IvoryCLIException ex) {
			System.err.println("Error: " + ex.getMessage());
			return -1;
		} catch (ParseException ex) {
			System.err.println("Invalid sub-command: " + ex.getMessage());
			System.err.println();
			System.err.println(parser.shortHelp());
			return -1;
		} catch (ClientHandlerException ex) {
			System.err
					.print("Unable to connect to Ivory server, please check if the URL is correct and Ivory server is up and running\n");
			System.err.println(ex.getMessage());
			return -1;
		} catch (Exception ex) {
			ex.printStackTrace();
			System.err.println(ex.getMessage());
			return -1;
		}

	}

	private void instanceCommand(CommandLine commandLine)
			throws IvoryCLIException {
		String ivoryUrl = validateIvoryUrl(commandLine);
		IvoryClient client = new IvoryClient(ivoryUrl);

		Set<String> optionsList = new HashSet<String>();
		for (Option option : commandLine.getOptions()) {
			optionsList.add(option.getOpt());
		}

		String result = null;
		String processName = commandLine.getOptionValue(PROCESS_OPT);
		String start = commandLine.getOptionValue(START_OPT);
		String end = commandLine.getOptionValue(END_OPT);
		String filePath = commandLine.getOptionValue(FILE_PATH_OPT);

		validateInstanceCommands(optionsList, processName, start, end, filePath);

		if (optionsList.contains(RUNNING_OPT)) {
			result = client.getRunningInstances(processName);
		} else if (optionsList.contains(STATUS_OPT)) {
			result = client.getStatusOfInstances(processName, start, end);
		} else if (optionsList.contains(KILL_OPT)) {
			result = client.killInstances(processName, start, end);
		} else if (optionsList.contains(SUSPEND_OPT)) {
			result = client.suspendInstances(processName, start, end);
		} else if (optionsList.contains(RESUME_OPT)) {
			result = client.resumeInstances(processName, start, end);
		} else if (optionsList.contains(RERUN_OPT)) {
			result = client.rerunInstances(processName, start, end, filePath);
		} else {
			throw new IvoryCLIException("Invalid command");
		}
		System.out.println(result);

	}

	private void validateInstanceCommands(Set<String> optionsList,
			String processName, String start, String end, String filePath)
			throws IvoryCLIException {

		if (processName == null || processName.equals("")) {
			throw new IvoryCLIException("Missing argument: processName");
		}

		if (!optionsList.contains(RUNNING_OPT)) {
			if (start == null || start.equals("")) {
				throw new IvoryCLIException("Missing argument: start");
			}
		}
		if (optionsList.contains(RERUN_OPT)) {
			if (filePath == null || filePath.equals("")) {
				throw new IvoryCLIException("Missing argument: filePath");
			}
		}

	}

	private void entityCommand(CommandLine commandLine)
			throws IvoryCLIException, ConnectException {
		String ivoryUrl = validateIvoryUrl(commandLine);
		IvoryClient client = new IvoryClient(ivoryUrl);

		Set<String> optionsList = new HashSet<String>();
		for (Option option : commandLine.getOptions()) {
			optionsList.add(option.getOpt());
		}

		String result = null;
		String entityType = commandLine.getOptionValue(ENTITY_TYPE_OPT);
		String entityName = commandLine.getOptionValue(ENTITY_NAME_OPT);
		String filePath = commandLine.getOptionValue(FILE_PATH_OPT);

		validateEntityCommands(optionsList, entityType, entityName, filePath);

		if (optionsList.contains(SUBMIT_OPT)) {
			result = client.submit(entityType, filePath);
		} else if (optionsList.contains(SUBMIT_AND_SCHEDULE_OPT)) {
			result = client.submitAndSchedule(entityType, filePath);
		} else if (optionsList.contains(VALIDATE_OPT)) {
			result = client.validate(entityType, filePath);
		} else if (optionsList.contains(SCHEDULE_OPT)) {
			result = client.schedule(entityType, entityName);
		} else if (optionsList.contains(SUSPEND_OPT)) {
			result = client.suspend(entityType, entityName);
		} else if (optionsList.contains(RESUME_OPT)) {
			result = client.resume(entityType, entityName);
		} else if (optionsList.contains(DELETE_OPT)) {
			result = client.delete(entityType, entityName);
		} else if (optionsList.contains(STATUS_OPT)) {
			result = client.getStatus(entityType, entityName);
		} else if (optionsList.contains(DEFINITION_OPT)) {
			result = client.getDefinition(entityType, entityName);
		} else if (optionsList.contains(DEPENDENCY_OPT)) {
			result = client.getDependency(entityType, entityName);
		} else if (optionsList.contains(HELP_CMD)) {
			System.out.println("Ivory Help");
		} else {
			throw new IvoryCLIException("Invalid command");
		}
		System.out.println(result);
	}

	private void validateEntityCommands(Set<String> optionsList,
			String entityType, String entityName, String filePath)
			throws IvoryCLIException {
		if (optionsList.contains(SUBMIT_AND_SCHEDULE_OPT)
				|| optionsList.contains(SUBMIT_OPT)
				|| optionsList.contains(VALIDATE_OPT)) {
			if (entityType == null || entityType.equals("") || filePath == null
					|| filePath.equals("")) {
				throw new IvoryCLIException(
						"Missing argument: entityType or filePath");
			}
		} else {
			if (entityType == null || entityType.equals("")
					|| entityName == null || entityName.equals("")) {
				throw new IvoryCLIException(
						"Missing argument: entityType or entityName");
			}
		}
	}

	// TODO
	private void versionCommand() {
		System.out.println("Apache Ivory version: 1.0");

	}

	private Options createAdminOptions() {
		Options adminOptions = new Options();
		Option url = new Option(URL_OPTION, true, "Ivory URL");
		adminOptions.addOption(url);

		OptionGroup group = new OptionGroup();
		// Option status = new Option(STATUS_OPTION, false,
		// "show the current system status");
		Option version = new Option(VERSION_OPTION, false,
				"show Ivory server build version");
		Option help = new Option("help", false, "show Ivory help");
		group.addOption(version);
		group.addOption(help);

		adminOptions.addOptionGroup(group);
		return adminOptions;
	}

	private Options entityOptions() {

		Options entityOptions = new Options();

		Option submit = new Option(SUBMIT_OPT, false,
				"Submits an entity xml to Ivory");
		Option schedule = new Option(SCHEDULE_OPT, false,
				"Schedules a submited entity in Ivory");
		Option suspend = new Option(SUSPEND_OPT, false,
				"Suspends a running entity in Ivory");
		Option resume = new Option(RESUME_OPT, false,
				"Resumes a suspended entity in Ivory");
		Option delete = new Option(DELETE_OPT, false,
				"Deletes an entity in Ivory, and kills its instance from workflow engine");
		Option submitAndSchedule = new Option(SUBMIT_AND_SCHEDULE_OPT, false,
				"Submits and entity to Ivory and schedules it immediately");
		Option validate = new Option(VALIDATE_OPT, false,
				"Validates an entity based on the entity type");
		Option status = new Option(STATUS_OPT, false,
				"Gets the status of entity");
		Option definition = new Option(DEFINITION_OPT, false,
				"Gets the Definition of entity");
		Option dependency = new Option(DEPENDENCY_OPT, false,
				"Gets the dependencies of entity");

		OptionGroup group = new OptionGroup();
		group.addOption(submit);
		group.addOption(schedule);
		group.addOption(suspend);
		group.addOption(resume);
		group.addOption(delete);
		group.addOption(submitAndSchedule);
		group.addOption(validate);
		group.addOption(status);
		group.addOption(definition);
		group.addOption(dependency);

		Option url = new Option(URL_OPTION, true, "Ivory URL");
		Option entityType = new Option(ENTITY_TYPE_OPT, true,
				"Entity type, can be cluster, feed or process xml");
		Option filePath = new Option(FILE_PATH_OPT, true,
				"Path to entity xml file");
		Option entityName = new Option(ENTITY_NAME_OPT, true,
				"Entity type, can be cluster, feed or process xml");

		entityOptions.addOption(url);
		entityOptions.addOptionGroup(group);
		entityOptions.addOption(entityType);
		entityOptions.addOption(entityName);
		entityOptions.addOption(filePath);

		return entityOptions;

	}

	private Options instanceOptions() {

		Options instanceOptions = new Options();

		Option running = new Option(RUNNING_OPT, false,
				"Gets running process instances for a given process");
		Option status = new Option(
				STATUS_OPT,
				false,
				"Gets status of process instances for a given process in the range start time and optional end time");
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
				"Resumes suspended process instances for a given process in the range start time and optional end time");
		Option rerun = new Option(
				RERUN_OPT,
				false,
				"Reruns process instances for a given process in the range start time and optional end time and overrides properties present in job.properties file");

		OptionGroup group = new OptionGroup();
		group.addOption(running);
		group.addOption(status);
		group.addOption(kill);
		group.addOption(resume);
		group.addOption(suspend);
		group.addOption(resume);
		group.addOption(rerun);

		Option url = new Option(URL_OPTION, true, "Ivory URL");
		Option start = new Option(START_OPT, true,
				"Start time is required for commands, status, kill, suspend, resume and re-run");
		Option end = new Option(
				END_OPT,
				true,
				"End time is optional for commands, status, kill, suspend, resume and re-run; if not specified then current time is considered as end time");
		Option filePath = new Option(
				FILE_PATH_OPT,
				true,
				"Path to job.properties file is required for rerun command, it should contain name=value pair for properties to override for rerun");
		Option processName = new Option(PROCESS_OPT, true,
				"Name of the process is required for all process instance commands");

		instanceOptions.addOption(url);
		instanceOptions.addOptionGroup(group);
		instanceOptions.addOption(start);
		instanceOptions.addOption(end);
		instanceOptions.addOption(filePath);
		instanceOptions.addOption(processName);

		return instanceOptions;

	}

	protected String validateIvoryUrl(CommandLine commandLine) {
		String url = commandLine.getOptionValue(URL_OPTION);
		if (url == null) {
			url = System.getenv(IVORY_URL);
			if (url == null) {
				throw new IllegalArgumentException(
						"Ivory URL is neither available in command option nor in the environment");
			}
		}
		return url;
	}

	private void adminCommand(CommandLine commandLine) throws IvoryCLIException {

		validateIvoryUrl(commandLine);

		Set<String> optionsList = new HashSet<String>();
		for (Option option : commandLine.getOptions()) {
			optionsList.add(option.getOpt());
		}
		if (optionsList.contains(VERSION_OPTION)) {
			System.out.println("Ivory server build version: 1.0");
		}

		else if (optionsList.contains(HELP_CMD)) {
			System.out.println("Ivory Help");
		}
	}

}
