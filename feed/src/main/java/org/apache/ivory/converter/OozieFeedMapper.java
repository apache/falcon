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

package org.apache.ivory.converter;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.feed.LocationType;
import org.apache.ivory.entity.v0.process.Property;
import org.apache.ivory.messaging.EntityInstanceMessage;
import org.apache.ivory.oozie.coordinator.ACTION;
import org.apache.ivory.oozie.coordinator.CONFIGURATION;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.apache.ivory.oozie.coordinator.WORKFLOW;
import org.apache.ivory.oozie.workflow.SUBWORKFLOW;
import org.apache.ivory.oozie.workflow.WORKFLOWAPP;
import org.apache.ivory.util.StartupProperties;
import org.apache.ivory.workflow.engine.OozieWorkflowEngine;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

public class OozieFeedMapper extends AbstractOozieEntityMapper<Feed> {

	private static Logger LOG = Logger.getLogger(OozieFeedMapper.class);

	private static final Pattern pattern = Pattern
			.compile("ivory-common|ivory-feed|ivory-retention");

	private static final String PAR_WORKFLOW_TEMPLATE_PATH = "/config/workflow/feed-parent-workflow.xml";

	private static volatile boolean retentionUploaded = false;

	public OozieFeedMapper(Feed feed) {
		super(feed);
	}

	@Override
	protected List<COORDINATORAPP> getCoordinators(Cluster cluster)
			throws IvoryException {
		return Arrays.asList(getRetentionCoordinator(cluster));
	}

	private COORDINATORAPP getRetentionCoordinator(Cluster cluster)
			throws IvoryException {

		Feed feed = getEntity();
		String basePath = feed.getWorkflowName() + "_RETENTION";
		COORDINATORAPP retentionApp = newCOORDINATORAPP(basePath);

		retentionApp.setName(basePath + "_" + feed.getName());
		org.apache.ivory.entity.v0.feed.Cluster feedCluster = feed
				.getCluster(cluster.getName());
		retentionApp.setEnd(feedCluster.getValidity().getEnd());
		
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
		formatter.setTimeZone(TimeZone.getTimeZone(feedCluster.getValidity()
				.getTimezone()));
		retentionApp.setStart(formatter.format(new Date()));
		retentionApp.setTimezone(feedCluster.getValidity().getTimezone());
		if (feed.getFrequency().matches("hours|minutes")) {
			retentionApp.setFrequency("${coord:hours(6)}");
		} else {
			retentionApp.setFrequency("${coord:days(1)}");
		}

		retentionApp.setAction(getParentWFAction(cluster, feed));
		return retentionApp;
	}

	private ACTION getParentWFAction(Cluster cluster, Feed feed)
			throws IvoryException {
		ACTION retentionAction = new ACTION();
		WORKFLOW parentWorkflow = new WORKFLOW();
		try {
			//
			createRetentionWorkflow(cluster);
			//parent workflow
			parentWorkflow.setAppPath(getHDFSPath(getParentWorkflowPath().toString()));
			
			CONFIGURATION conf = new CONFIGURATION();			

			org.apache.ivory.entity.v0.feed.Cluster feedCluster = feed
					.getCluster(cluster.getName());

			String feedPathMask = feed.getLocations().get(LocationType.DATA)
					.getPath();
			conf.getProperty().add(
					createCoordProperty("feedDataPath",
							feedPathMask.replaceAll("\\$\\{", "\\?\\{")));
			conf.getProperty().add(
					createCoordProperty("timeZone", feedCluster.getValidity()
							.getTimezone()));
			conf.getProperty().add(
					createCoordProperty("frequency", feed.getFrequency()));
			conf.getProperty().add(
					createCoordProperty("limit", feedCluster.getRetention()
							.getLimit()));
			conf.getProperty().add(
					createCoordProperty("logDir", getHDFSPath(getParentWorkflowPath().getParent().toString())));
			
			 String queueName=getUserDefinedProps().get("queueName");
		     conf.getProperty().add(createCoordProperty("queueName",queueName==null||queueName.equals("")?"default":queueName));
			
			//Parent wf properties
			conf.getProperty().add(createCoordProperty(EntityInstanceMessage.ARG.ENTITY_TOPIC_NAME.NAME(), feed.getName()));
			conf.getProperty().add(createCoordProperty(EntityInstanceMessage.ARG.FEED_NAME.NAME(), feed.getName()));
			//these instancepaths are read from file in parentworflow
			conf.getProperty().add(createCoordProperty(EntityInstanceMessage.ARG.FEED_INSTANCE_PATH.NAME(), feed.getStagingPath()));
			conf.getProperty().add(createCoordProperty(EntityInstanceMessage.ARG.NOMINAL_TIME.NAME(), NOMINAL_TIME_EL));
			conf.getProperty().add(createCoordProperty(EntityInstanceMessage.ARG.TIME_STAMP.NAME(), ACTUAL_TIME_EL));
			conf.getProperty().add(createCoordProperty(EntityInstanceMessage.ARG.BROKER_URL.NAME(),ClusterHelper.getMessageBrokerUrl(cluster)));
	        String brokerImplClass=getUserDefinedProps().get(EntityInstanceMessage.ARG.BROKER_IMPL_CLASS.NAME());
	        conf.getProperty().add(createCoordProperty(EntityInstanceMessage.ARG.BROKER_IMPL_CLASS.NAME(),brokerImplClass==null||brokerImplClass.equals("")?DEFAULT_BROKER_IMPL_CLASS:brokerImplClass));
	        conf.getProperty().add(createCoordProperty(EntityInstanceMessage.ARG.ENTITY_TYPE.NAME(),feed.getEntityType().name()));
	        conf.getProperty().add(createCoordProperty(EntityInstanceMessage.ARG.OPERATION.NAME(),EntityInstanceMessage.entityOperation.DELETE.name()));
			conf.getProperty().add(createCoordProperty(OozieClient.LIBPATH, getHDFSPath(getRetentionWorkflowPath(cluster)+"/lib")));
			
			//user wf (sub-flow) confs, add all user defined props to coordinator
			for (Entry<String, String> entry : getUserDefinedProps().entrySet())
				conf.getProperty().add(
						createCoordProperty(entry.getKey(), entry.getValue()));

			parentWorkflow.setConfiguration(conf);
			retentionAction.setWorkflow(parentWorkflow);
			return retentionAction;
		} catch (Exception e) {
			throw new IvoryException("Unable to create parent/retention workflow", e);
		}
	}

	private Path createRetentionWorkflow(Cluster cluster) throws IOException {
		Path outPath = new Path(getRetentionWorkflowPath(cluster));
		if (!retentionUploaded) {
			InputStream in = getClass().getResourceAsStream(
					"/retention-workflow.xml");
			FileSystem fs = FileSystem.get(ClusterHelper
					.getConfiguration(cluster));
			OutputStream out = fs.create(new Path(outPath, "workflow.xml"));
			IOUtils.copyBytes(in, out, 4096, true);
			if (LOG.isDebugEnabled()) {
				debug(outPath, fs);
			}
			String localLibPath = getLocalLibLocation();
			Path libLoc = new Path(outPath, "lib");
			fs.mkdirs(libLoc);
			for (File file : new File(localLibPath).listFiles()) {
				if (pattern.matcher(file.getName()).find()) {
					LOG.debug("Copying " + file.getAbsolutePath() + " to "
							+ libLoc);
					fs.copyFromLocalFile(new Path(file.getAbsolutePath()),
							libLoc);
				}
			}
			retentionUploaded = true;
		}
		return outPath;
	}

	private String getLocalLibLocation() {
		String localLibPath = StartupProperties.get().getProperty(
				"system.lib.location");
		if (localLibPath == null || localLibPath.isEmpty()
				|| !new File(localLibPath).exists()) {
			LOG.error("Unable to copy libs: Invalid location " + localLibPath);
			throw new IllegalStateException("Invalid lib location "
					+ localLibPath);
		}
		return localLibPath;
	}

	private void debug(Path outPath, FileSystem fs) throws IOException {
		ByteArrayOutputStream writer = new ByteArrayOutputStream();
		InputStream xmlData = fs.open(new Path(outPath, "workflow.xml"));
		IOUtils.copyBytes(xmlData, writer, 4096, true);
		LOG.debug("Workflow xml copied to " + outPath + "/workflow.xml");
		LOG.debug(writer);
	}

	@Override
	protected WORKFLOWAPP getParentWorkflow(Cluster cluster)
			throws IvoryException {

		WORKFLOWAPP parentWorkflow = getParentWorkflowTemplate();
		// set the subflow app path to users workflow
		SUBWORKFLOW userSubFlowAction = ((org.apache.ivory.oozie.workflow.ACTION) parentWorkflow
				.getDecisionOrForkOrJoin().get(1)).getSubWorkflow();

		userSubFlowAction
				.setAppPath(getHDFSPath(getRetentionWorkflowPath(cluster)));

		// user wf (subflow) confs
		org.apache.ivory.oozie.workflow.CONFIGURATION conf = new org.apache.ivory.oozie.workflow.CONFIGURATION();
		conf.getProperty().add(createWorkflowProperty("feedDataPath",getVarName("feedDataPath")));
		conf.getProperty().add(createWorkflowProperty("timeZone", getVarName("timeZone")));
		conf.getProperty().add(createWorkflowProperty("frequency", getVarName("frequency")));
		conf.getProperty().add(createWorkflowProperty("limit", getVarName("limit")));
		conf.getProperty().add(createWorkflowProperty("queueName", getVarName("queueName")));
		conf.getProperty().add(createWorkflowProperty("logDir", getVarName("logDir")));
		conf.getProperty().add(createWorkflowProperty(EntityInstanceMessage.ARG.NOMINAL_TIME.NAME(), getVarName(EntityInstanceMessage.ARG.NOMINAL_TIME.NAME())));
		
		//user defined props, propagate to subflow using variable names
		for(String prop: getUserDefinedProps().keySet()){
			conf.getProperty().add(createWorkflowProperty(prop,getVarName(prop)));
		}		
		
		userSubFlowAction.setConfiguration(conf);

		return parentWorkflow;
	}

	@SuppressWarnings("unchecked")
	private WORKFLOWAPP getParentWorkflowTemplate() throws IvoryException {
		try {
			Unmarshaller unmarshaller = workflowJaxbContext
					.createUnmarshaller();
			JAXBElement<WORKFLOWAPP> workflowapp = (JAXBElement<WORKFLOWAPP>) unmarshaller
					.unmarshal(AbstractOozieEntityMapper.class
							.getResourceAsStream(PAR_WORKFLOW_TEMPLATE_PATH));
			return workflowapp.getValue();
		} catch (JAXBException e) {
			throw new IvoryException(e);
		}
	}
	
	private String getRetentionWorkflowPath(Cluster cluster){
		return ClusterHelper.getLocation(cluster, "staging")+
				"/ivory/system/retention";
	}
}
