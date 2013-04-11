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

package org.apache.falcon.cli;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.client.FalconClient;

import com.sun.jersey.api.client.ClientHandlerException;

public class FalconCLI {

	public static PrintStream ERR_STREAM = System.err;
	public static PrintStream OUT_STREAM = System.out;
	
	public static final String FALCON_URL = "FALCON_URL";
	public static final String URL_OPTION = "url";
	public static final String VERSION_OPTION = "version";
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
	public static final String DEFINITION_OPT = "definition";
	public static final String DEPENDENCY_OPT = "dependency";
	public static final String LIST_OPT = "list";

	public static final String INSTANCE_CMD = "instance";
	public static final String START_OPT = "start";
	public static final String END_OPT = "end";
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

	/**
	 * Entry point for the Falcon CLI when invoked from the command line. Upon
	 * completion this method exits the JVM with '0' (success) or '-1'
	 * (failure).
	 * 
	 * @param args
	 *            options and arguments for the Falcon CLI.
	 */
	public static void main(String[] args) {
		System.exit(new FalconCLI().run(args));
	}

	// TODO help and headers
	private static final String[] FALCON_HELP = {
			"the env variable '" + FALCON_URL
					+ "' is used as default value for the '-" + URL_OPTION
					+ "' option",
			"custom headers for Falcon web services can be specified using '-D"
					+ FalconClient.WS_HEADER_PREFIX + "NAME=VALUE'" };

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

		CLIParser parser = new CLIParser("falcon", FALCON_HELP);

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
				"Process instances operations like running, status, kill, suspend, resume, rerun, logs",
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
		} catch (FalconCLIException ex) {
			ERR_STREAM.println("Error: " + ex.getMessage());
			return -1;
		} catch (ParseException ex) {
			ERR_STREAM.println("Invalid sub-command: " + ex.getMessage());
			ERR_STREAM.println();
			ERR_STREAM.println(parser.shortHelp());
			return -1;
		} catch (ClientHandlerException ex) {
			ERR_STREAM
					.print("Unable to connect to Falcon server, please check if the URL is correct and Falcon server is up and running\n");
			ERR_STREAM.println(ex.getMessage());
			return -1;
		} catch (Exception ex) {
			ex.printStackTrace();
			ERR_STREAM.println(ex.getMessage());
			return -1;
		}

	}

	private void instanceCommand(CommandLine commandLine)
			throws FalconCLIException, IOException {
		String falconUrl = getFalconEndpoint(commandLine);
		FalconClient client = new FalconClient(falconUrl);

		Set<String> optionsList = new HashSet<String>();
		for (Option option : commandLine.getOptions()) {
			optionsList.add(option.getOpt());
		}

		String result = null;
		String type = commandLine.getOptionValue(ENTITY_TYPE_OPT);
		String entity = commandLine.getOptionValue(ENTITY_NAME_OPT);
		String start = commandLine.getOptionValue(START_OPT);
		String end = commandLine.getOptionValue(END_OPT);
		String filePath = commandLine.getOptionValue(FILE_PATH_OPT);
		String runid = commandLine.getOptionValue(RUNID_OPT);
		String colo = commandLine.getOptionValue(COLO_OPT);
		String clusters = commandLine.getOptionValue(CLUSTERS_OPT);
		String sourceClusters = commandLine.getOptionValue(SOURCECLUSTER_OPT);
		
		colo = getColo(colo);
		
		validateInstanceCommands(optionsList, entity, type, start, end,
				filePath, colo, clusters, sourceClusters);
		
		if (optionsList.contains(RUNNING_OPT)) {
			result = client.getRunningInstances(type, entity, colo);
		} else if (optionsList.contains(STATUS_OPT)) {
			result = client.getStatusOfInstances(type, entity, start, end,
					runid, colo);
		} else if (optionsList.contains(KILL_OPT)) {
			result = client.killInstances(type, entity, start, end, colo, clusters, sourceClusters);
		} else if (optionsList.contains(SUSPEND_OPT)) {
			result = client.suspendInstances(type, entity, start, end, colo, clusters, sourceClusters);
		} else if (optionsList.contains(RESUME_OPT)) {
			result = client.resumeInstances(type, entity, start, end, colo, clusters, sourceClusters);
		} else if (optionsList.contains(RERUN_OPT)) {
			result = client.rerunInstances(type, entity, start, end, filePath, colo, clusters, sourceClusters);
		}  else if (optionsList.contains(CONTINUE_OPT)) {
			result = client.rerunInstances(type, entity, start, end, colo, clusters, sourceClusters);
		} else if(optionsList.contains(LOG_OPT)){
			result = client.getLogsOfInstances(type, entity, start, end, colo, runid);
		} else {
			throw new FalconCLIException("Invalid command");
		}
		OUT_STREAM.println(result);

	}

	private void validateInstanceCommands(Set<String> optionsList,
			String entity, String type, String start, String end,
			String filePath, String colo, String clusters, String sourceClusters) 
					throws FalconCLIException {

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
					|| optionsList.contains(STATUS_OPT)) {
				throw new FalconCLIException("Invalid argument: clusters");
			}
		}
		
		if (optionsList.contains(SOURCECLUSTER_OPT)) {
			if ( optionsList.contains(RUNNING_OPT)
					|| optionsList.contains(LOG_OPT)
					|| optionsList.contains(STATUS_OPT) || !type.equals("feed") ) {
				throw new FalconCLIException("Invalid argument: sourceClusters");
			}
		}

	}

	private void entityCommand(CommandLine commandLine)
			throws FalconCLIException, IOException {
		String falconUrl = getFalconEndpoint(commandLine);
		FalconClient client = new FalconClient(falconUrl);

		Set<String> optionsList = new HashSet<String>();
		for (Option option : commandLine.getOptions()) {
			optionsList.add(option.getOpt());
		}

		String result = null;
		String entityType = commandLine.getOptionValue(ENTITY_TYPE_OPT);
		String entityName = commandLine.getOptionValue(ENTITY_NAME_OPT);
		String filePath = commandLine.getOptionValue(FILE_PATH_OPT);
		String colo = commandLine.getOptionValue(COLO_OPT);
		
		
		validateEntityType(optionsList, entityType);

		if (optionsList.contains(SUBMIT_OPT)) {
			validateFilePath(optionsList, filePath);
			validateColo(optionsList);
			result = client.submit(entityType, filePath);
		} else if (optionsList.contains(UPDATE_OPT)) {
			validateFilePath(optionsList, filePath);
			validateColo(optionsList);
			validateEntityName(optionsList, entityName);
			result = client.update(entityType, entityName, filePath);
		} else if (optionsList.contains(SUBMIT_AND_SCHEDULE_OPT)) {
			validateFilePath(optionsList, filePath);
			validateColo(optionsList);
			result = client.submitAndSchedule(entityType, filePath);
		} else if (optionsList.contains(VALIDATE_OPT)) {
			validateFilePath(optionsList, filePath);
			validateColo(optionsList);
			result = client.validate(entityType, filePath);
		} else if (optionsList.contains(SCHEDULE_OPT)) {
			validateEntityName(optionsList, entityName);
			colo = getColo(colo);
			result = client.schedule(entityType, entityName, colo);
		} else if (optionsList.contains(SUSPEND_OPT)) {
			validateEntityName(optionsList, entityName);
			colo = getColo(colo);
			result = client.suspend(entityType, entityName, colo);
		} else if (optionsList.contains(RESUME_OPT)) {
			validateEntityName(optionsList, entityName);
			colo = getColo(colo);
			result = client.resume(entityType, entityName, colo);
		} else if (optionsList.contains(DELETE_OPT)) {
			validateColo(optionsList);
			validateEntityName(optionsList, entityName);
			result = client.delete(entityType, entityName);
		} else if (optionsList.contains(STATUS_OPT)) {
			validateEntityName(optionsList, entityName);
			colo = getColo(colo);
			result = client.getStatus(entityType, entityName, colo);
		} else if (optionsList.contains(DEFINITION_OPT)) {
			validateColo(optionsList);
			validateEntityName(optionsList, entityName);
			result = client.getDefinition(entityType, entityName);
		} else if (optionsList.contains(DEPENDENCY_OPT)) {
			validateColo(optionsList);
			validateEntityName(optionsList, entityName);
			result = client.getDependency(entityType, entityName);
		} else if (optionsList.contains(LIST_OPT)) {
			validateColo(optionsList);
			result = client.getEntityList(entityType);
		} else if (optionsList.contains(HELP_CMD)) {
			OUT_STREAM.println("Falcon Help");
		} else {
			throw new FalconCLIException("Invalid command");
		}
		OUT_STREAM.println(result);
	}
	
	private String getColo(String colo) throws FalconCLIException, IOException
	{
		if (colo == null) {
			Properties prop = getClientProperties();
			colo = prop.getProperty(CURRENT_COLO, "*");
		}
		return colo;
	}
	private void validateFilePath(Set<String> optionsList, String filePath)
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

	private void validateEntityName(Set<String> optionsList, String entityName)
			throws FalconCLIException {
		if (entityName == null || entityName.equals("")) {
			throw new FalconCLIException("Missing argument: name");
		}
	}

	private void validateEntityType(Set<String> optionsList, String entityType)
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
		// Option status = new Option(STATUS_OPTION, false,
		// "show the current system status");
		Option version = new Option(VERSION_OPTION, false,
				"show Falcon server build version");
		Option help = new Option("help", false, "show Falcon help");
		group.addOption(version);
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

		entityOptions.addOption(url);
		entityOptions.addOptionGroup(group);
		entityOptions.addOption(entityType);
		entityOptions.addOption(entityName);
		entityOptions.addOption(filePath);
		entityOptions.addOption(colo);

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
		
		Option continues = new Option(
				CONTINUE_OPT,
				false,
				"resume process instance execution for a given process in the range start time and optional end time and overrides properties present in job.properties file");
		
		Option logs = new Option(
				LOG_OPT,
				false,
				"Logs print the logs for process instances for a given process in the range start time and optional end time");
		OptionGroup group = new OptionGroup();
		group.addOption(running);
		group.addOption(status);
		group.addOption(kill);
		group.addOption(resume);
		group.addOption(suspend);
		group.addOption(resume);
		group.addOption(rerun);
		group.addOption(logs);
		group.addOption(continues);

		Option url = new Option(URL_OPTION, true, "Falcon URL");
		Option start = new Option(START_OPT, true,
				"Start time is required for commands, status, kill, suspend, resume and re-run");
		Option end = new Option(
				END_OPT,
				true,
				"End time is optional for commands, status, kill, suspend, resume and re-run; if not specified then current time is considered as end time");
		Option runid = new Option(RUNID_OPT, true,
				"Instance runid  is optional and user can specify the runid, defaults to 0");
		Option clusters = new Option(CLUSTERS_OPT, true,
				"clusters is optional for commands kill, suspend and resume, should not be specified for other commands");
		Option sourceClusters = new Option(SOURCECLUSTER_OPT, true,
				" source cluster is optional for commands kill, suspend and resume, should not be specified for other commands (required for only feed)");
		Option filePath = new Option(
				FILE_PATH_OPT,
				true,
				"Path to job.properties file is required for rerun command, it should contain name=value pair for properties to override for rerun");
		Option entityType = new Option(ENTITY_TYPE_OPT, true,
				"Entity type, can be feed or process xml");
		Option entityName = new Option(ENTITY_NAME_OPT, true,
				"Entity type, can be feed or process xml");
        Option colo = new Option(COLO_OPT, true,
                "Colo on which the cmd has to be executed");

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
		
		return instanceOptions;

	}

	protected String getFalconEndpoint(CommandLine commandLine)
			throws FalconCLIException, IOException {
		String url = commandLine.getOptionValue(URL_OPTION);
		if (url == null) {
		    url = System.getenv(FALCON_URL);
		}
		if(url == null) {
            Properties prop = getClientProperties();
            if (prop.containsKey("falcon.url"))
                url = prop.getProperty("falcon.url");
		}
		if(url == null)
		    throw new FalconCLIException("Failed to get falcon url from cmdline, or environment or client properties");
		return url;
	}

	private void adminCommand(CommandLine commandLine) throws FalconCLIException, IOException {
		String result;
		String falconUrl = getFalconEndpoint(commandLine);
		FalconClient client = new FalconClient(falconUrl);
		
		Set<String> optionsList = new HashSet<String>();
		for (Option option : commandLine.getOptions()) {
			optionsList.add(option.getOpt());
		}
		
		if(optionsList.contains(STACK_OPTION)){
			result = client.getThreadDump();
			OUT_STREAM.println(result);
		}
		if (optionsList.contains(VERSION_OPTION)) {
			result = client.getVersion();
			OUT_STREAM.println("Falcon server build version: " + result);
		}

		else if (optionsList.contains(HELP_CMD)) {
			OUT_STREAM.println("Falcon Help");
		}
	}
	
	private Properties getClientProperties() throws IOException
	{
        Properties prop = new Properties();
        InputStream input = FalconCLI.class.getResourceAsStream(CLIENT_PROPERTIES);
		if (input != null) {
	        prop.load(input);
		}
		return prop;
	}

}
