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
	public static final String WS_HEADER_PREFIX = "header:";
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
					+ WS_HEADER_PREFIX + "NAME=VALUE'" };

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

		try {
			CLIParser.Command command = parser.parse(args);
			if (command.getName().equals(HELP_CMD)) {
				parser.showHelp();
			} else if (command.getName().equals(ADMIN_CMD)) {
				adminCommand(command.getCommandLine());
			} else if (command.getName().equals(ENTITY_CMD)) {
				entityCommand(command.getCommandLine());
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

		if (optionsList.contains(SUBMIT_AND_SCHEDULE_OPT)
				|| optionsList.contains(SUBMIT_OPT)
				|| optionsList.contains(VALIDATE_OPT)) {
			if (entityType == null || entityType.equals("") || filePath == null
					|| filePath.equals("")) {
				throw new IvoryCLIException(
						"Required arguments: entityType and filePath");
			}
		} else {
			if (entityType == null || entityType.equals("")
					|| entityName == null || entityName.equals("")) {
				throw new IvoryCLIException(
						"Required arguments: entityType and entityName");
			}
		}

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
		} else if (optionsList.contains(HELP_CMD)) {
			System.out.println("Ivory Help");
		} else {
			throw new IvoryCLIException("Invalid command");
		}
		System.out.println(result);
	}

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
