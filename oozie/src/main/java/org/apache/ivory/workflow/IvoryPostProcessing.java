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
package org.apache.ivory.workflow;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.ivory.logging.LogMover;
import org.apache.ivory.messaging.MessageProducer;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

public class IvoryPostProcessing extends Configured implements Tool{
	private static final Logger LOG = Logger.getLogger(IvoryPostProcessing.class);

	public enum Arg{
		CLUSTER("cluster","name of the current cluster"),
		ENTITY_TYPE("entityType","type of the entity"),
		ENTITY_NAME("entityName","name of the entity"),
		NOMINAL_TIME("nominalTime","instance time"),
		OPERATION("operation","operation like generate, delete, replicate"),
		WORKFLOW_ID("workflowId","current workflow-id of the instance"),
		RUN_ID("runId","current run-id of the instance"),
		STATUS("status","status of the user workflow isnstance"),
		TIMESTAMP("timeStamp","current timestamp"),
		TOPIC_NAME("topicName","name of the topic to be used to send JMS message"),
		BRKR_IMPL_CLASS("brokerImplClass","ivory message broker Implementation class"),
		BRKR_URL("brokerUrl","ivory message broker url"),
		USER_BRKR_IMPL_CLASS("userBrokerImplClass","user broker Impl class"),
		USER_BRKR_URL("userBrokerUrl","user broker url"),
		BRKR_TTL("brokerTTL", "time to live for broker message in sec"),
		FEED_NAMES("feedNames", "name of the feeds which are generated/replicated/deleted"),
		FEED_INSTANCE_PATHS("feedInstancePaths","comma seperated feed instance paths"),
		LOG_FILE("logFile","log file path where feeds to be deleted are recorded"),
		WF_ENGINE_URL("workflowEngineUrl","url of workflow engine server, ex:oozie"),
		USER_SUBFLOW_ID("subflowId","external id of user workflow"),
		LOG_DIR("logDir","log dir where job logs are copied");		

		private String name;
		private String description;
		Arg(String name, String description){
			this.name=name;
			this.description=description;
		}

		public Option getOption() {
			return new Option(this.name, true, this.description);
		}

		public String getOptionName(){
			return this.name;
		}

		public String getOptionValue(CommandLine cmd){
			return cmd.getOptionValue(this.name);
		}

	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new IvoryPostProcessing(), args);
	}

	@Override
	public int run(String[] args) throws Exception {

		CommandLine cmd = getCommand(args);

		LOG.info("Sending user message "+cmd);
		invokeUserMessageProducer(cmd);
		//LogMover doesnt throw exception, a failed logmover will not fail the user workflow
		LOG.info("Moving logs "+cmd);
		invokeLogProducer(cmd);
		LOG.info("Sending ivory message "+cmd);
		invokeIvoryMessageProducer(cmd);

		return 0;
	}

	private void invokeUserMessageProducer(CommandLine cmd) throws Exception {
		List<String> args = new ArrayList<String>();
		addArg(args,cmd, Arg.CLUSTER);
		addArg(args,cmd, Arg.ENTITY_TYPE);
		addArg(args,cmd,Arg.ENTITY_NAME);
		addArg(args,cmd,Arg.NOMINAL_TIME);
		addArg(args,cmd,Arg.OPERATION);
		addArg(args,cmd,Arg.WORKFLOW_ID);
		addArg(args,cmd,Arg.RUN_ID);
		addArg(args,cmd,Arg.STATUS);
		addArg(args,cmd,Arg.TIMESTAMP);
		//special args for user JMS message producer
		args.add("-"+Arg.TOPIC_NAME.getOptionName()); //user topic
		args.add("IVORY."+Arg.ENTITY_NAME.getOptionValue(cmd));
		//note, the user broker impl class arg name to MessageProducer is brokerImplClass 
		args.add("-"+Arg.BRKR_IMPL_CLASS.getOptionName());
		args.add(Arg.USER_BRKR_IMPL_CLASS.getOptionValue(cmd));
		args.add("-"+Arg.BRKR_URL.getOptionName());
		args.add(Arg.USER_BRKR_URL.getOptionValue(cmd));
		addArg(args,cmd,Arg.BRKR_TTL);
		addArg(args,cmd,Arg.FEED_NAMES);
		addArg(args,cmd,Arg.FEED_INSTANCE_PATHS);
		addArg(args,cmd,Arg.LOG_FILE);

		MessageProducer.main(args.toArray(new String[0]));
	}

	private void invokeIvoryMessageProducer(CommandLine cmd) throws Exception {
		List<String> args = new ArrayList<String>();
		addArg(args,cmd, Arg.CLUSTER);
		addArg(args,cmd, Arg.ENTITY_TYPE);
		addArg(args,cmd,Arg.ENTITY_NAME);
		addArg(args,cmd,Arg.NOMINAL_TIME);
		addArg(args,cmd,Arg.OPERATION);
		addArg(args,cmd,Arg.WORKFLOW_ID);
		addArg(args,cmd,Arg.RUN_ID);
		addArg(args,cmd,Arg.STATUS);
		addArg(args,cmd,Arg.TIMESTAMP);
		//special args Ivory JMS message producer
		args.add("-"+Arg.TOPIC_NAME.getOptionName());
		args.add("IVORY.ENTITY.TOPIC");
		args.add("-"+Arg.BRKR_IMPL_CLASS.getOptionName());
		args.add(Arg.BRKR_IMPL_CLASS.getOptionValue(cmd));
		args.add("-"+Arg.BRKR_URL.getOptionName());
		args.add(Arg.BRKR_URL.getOptionValue(cmd));
		addArg(args,cmd,Arg.BRKR_TTL);
		addArg(args,cmd,Arg.FEED_NAMES);
		addArg(args,cmd,Arg.FEED_INSTANCE_PATHS);
		addArg(args,cmd,Arg.LOG_FILE);

		MessageProducer.main(args.toArray(new String[0]));

	}
	private void invokeLogProducer(CommandLine cmd) throws Exception{
		List<String> args = new ArrayList<String>();
		addArg(args,cmd,Arg.WF_ENGINE_URL);
		addArg(args,cmd,Arg.ENTITY_TYPE);
		addArg(args,cmd,Arg.USER_SUBFLOW_ID);
		addArg(args,cmd,Arg.RUN_ID);
		addArg(args,cmd,Arg.LOG_DIR);
		addArg(args,cmd,Arg.STATUS);

		LogMover.main(args.toArray(new String[0]));

	}

	private void addArg(List<String> args, CommandLine cmd, Arg arg) {
		args.add("-"+arg.getOptionName());
		args.add(arg.getOptionValue(cmd));
	}

	private static CommandLine getCommand(String[] arguments)
			throws ParseException {
		Options options = new Options();
		addOption(options, Arg.CLUSTER);
		addOption(options, Arg.ENTITY_TYPE);
		addOption(options, Arg.ENTITY_NAME);
		addOption(options, Arg.NOMINAL_TIME);
		addOption(options, Arg.OPERATION);
		addOption(options, Arg.WORKFLOW_ID);
		addOption(options, Arg.RUN_ID);
		addOption(options, Arg.STATUS);
		addOption(options, Arg.TIMESTAMP);
		addOption(options, Arg.BRKR_IMPL_CLASS);
		addOption(options, Arg.BRKR_URL);
		addOption(options, Arg.USER_BRKR_IMPL_CLASS);
		addOption(options, Arg.USER_BRKR_URL);
		addOption(options, Arg.BRKR_TTL);
		addOption(options, Arg.FEED_NAMES);
		addOption(options, Arg.FEED_INSTANCE_PATHS);
		addOption(options, Arg.LOG_FILE);
		addOption(options,Arg.WF_ENGINE_URL);
		addOption(options, Arg.USER_SUBFLOW_ID);
		addOption(options, Arg.LOG_DIR);
		return new GnuParser().parse(options, arguments);
	}

	private static void addOption(Options options, Arg arg) {
		Option option = arg.getOption();
		option.setRequired(true);
		options.addOption(option);
	}


}
