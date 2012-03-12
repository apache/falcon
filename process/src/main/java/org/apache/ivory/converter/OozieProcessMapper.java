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

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.ExternalId;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.feed.LocationType;
import org.apache.ivory.entity.v0.process.Input;
import org.apache.ivory.entity.v0.process.Output;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.messaging.EntityInstanceMessage;
import org.apache.ivory.oozie.coordinator.CONFIGURATION;
import org.apache.ivory.oozie.coordinator.CONTROLS;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.apache.ivory.oozie.coordinator.DATAIN;
import org.apache.ivory.oozie.coordinator.DATAOUT;
import org.apache.ivory.oozie.coordinator.DATASETS;
import org.apache.ivory.oozie.coordinator.INPUTEVENTS;
import org.apache.ivory.oozie.coordinator.OUTPUTEVENTS;
import org.apache.ivory.oozie.coordinator.ObjectFactory;
import org.apache.ivory.oozie.coordinator.SYNCDATASET;
import org.apache.ivory.oozie.coordinator.WORKFLOW;
import org.apache.ivory.oozie.workflow.ACTION;
import org.apache.ivory.oozie.workflow.SUBWORKFLOW;
import org.apache.ivory.oozie.workflow.WORKFLOWAPP;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;

public class OozieProcessMapper extends AbstractOozieEntityMapper<Process> {

    private static final String EL_PREFIX = "elext:";
    private static Logger LOG = Logger.getLogger(OozieProcessMapper.class);    
	
	private static final String PAR_WORKFLOW_TEMPLATE_PATH="/config/workflow/process-parent-workflow.xml";    
	
	private Map<String,String> subflowProps= new HashMap<String,String>();

    public OozieProcessMapper(Process entity) {
        super(entity);
    }

    @Override
    protected List<COORDINATORAPP> getCoordinators(Cluster cluster)
            throws IvoryException {
        return Arrays.asList(createDefaultCoordinator(cluster));
    }
    
	@Override
	protected WORKFLOWAPP getParentWorkflow(Cluster cluster)
			throws IvoryException {
		Process process = getEntity();
		WORKFLOWAPP parentWorkflow = getParentWorkflowTemplate();
		// set the subflow app path to users workflow
		SUBWORKFLOW userSubFlowAction = ((ACTION) parentWorkflow
				.getDecisionOrForkOrJoin().get(1)).getSubWorkflow();
		userSubFlowAction.setAppPath(getHDFSPath(process.getWorkflow()
				.getPath()));

		//user wf (sub-flow) confs, add all user defined props to sub-flow
		org.apache.ivory.oozie.workflow.CONFIGURATION conf = new org.apache.ivory.oozie.workflow.CONFIGURATION();
		for (String propName : getUserDefinedProps().keySet())
			conf.getProperty().add(
					createWorkflowProperty(propName, getVarName(propName)));
		
		for (String propName : subflowProps.keySet())
			conf.getProperty().add(
					createWorkflowProperty(propName, getVarName(propName)));		
		userSubFlowAction.setConfiguration(conf);				
		
		return parentWorkflow;
	}

	/**
     * Creates default oozie coordinator 
     * 
     * @param cluster Cluster for which the coordiantor app need to be created
     * @return COORDINATORAPP
     * @throws IvoryException on Error
     */
    public COORDINATORAPP createDefaultCoordinator(Cluster cluster) throws IvoryException {
        Process process = getEntity();
        if (process == null)
            return null;

        String basePath = process.getWorkflowName() + "_DEFAULT";
        COORDINATORAPP coord = newCOORDINATORAPP(basePath);

        // coord attributes
        coord.setName(basePath + "_" + process.getName());
        coord.setStart(process.getValidity().getStart());
        coord.setEnd(process.getValidity().getEnd());
        coord.setTimezone(process.getValidity().getTimezone());
        coord.setFrequency("${coord:" + process.getFrequency() + "(" + process.getPeriodicity() + ")}");

        // controls
        CONTROLS controls = new CONTROLS();
        controls.setConcurrency(""+process.getConcurrency());
        controls.setExecution(process.getExecution());
        coord.setControls(controls);
        
        //Parent workflow properties
        HashMap<String, String> parentWFprops= new HashMap<String, String>();
        parentWFprops.put(EntityInstanceMessage.ARG.ENTITY_TOPIC_NAME.NAME(), process.getName());
        parentWFprops.put(EntityInstanceMessage.ARG.NOMINAL_TIME.NAME(), NOMINAL_TIME_EL);
        parentWFprops.put(EntityInstanceMessage.ARG.TIME_STAMP.NAME(), ACTUAL_TIME_EL);
        parentWFprops.put(EntityInstanceMessage.ARG.BROKER_URL.NAME(),ClusterHelper.getMessageBrokerUrl(cluster));
        String brokerImplClass=getUserDefinedProps().get(EntityInstanceMessage.ARG.BROKER_IMPL_CLASS.NAME());
        parentWFprops.put(EntityInstanceMessage.ARG.BROKER_IMPL_CLASS.NAME(),brokerImplClass==null||brokerImplClass.equals("")?DEFAULT_BROKER_IMPL_CLASS:brokerImplClass);
        parentWFprops.put(EntityInstanceMessage.ARG.ENTITY_TYPE.NAME(),process.getEntityType().name());
        parentWFprops.put(EntityInstanceMessage.ARG.OPERATION.NAME(),EntityInstanceMessage.entityOperation.GENERATE.name());
        parentWFprops.put("logDir", getHDFSPath(getParentWorkflowPath().getParent().toString()));
        //override external ID
        parentWFprops.put(OozieClient.EXTERNAL_ID, new ExternalId(process.getName(), "${coord:nominalTime()}").getId());
        
        String queueName=getUserDefinedProps().get("queueName");
        parentWFprops.put("queueName",queueName==null||queueName.equals("")?"default":queueName);
	
        // inputs
        if (process.getInputs() != null) {
            for (Input input : process.getInputs().getInput()) {
                SYNCDATASET syncdataset = createDataSet(input.getFeed(), cluster);
                if (coord.getDatasets() == null)
                    coord.setDatasets(new DATASETS());
                coord.getDatasets().getDatasetOrAsyncDataset().add(syncdataset);

                DATAIN datain = new DATAIN();
                datain.setName(input.getName());
                datain.setDataset(input.getFeed());
                datain.setStartInstance(getELExpression(input.getStartInstance()));
                datain.setEndInstance(getELExpression(input.getEndInstance()));
                if (coord.getInputEvents() == null)
                    coord.setInputEvents(new INPUTEVENTS());
                coord.getInputEvents().getDataIn().add(datain);

                if(StringUtils.isNotEmpty(input.getPartition()))
                	parentWFprops.put(input.getName(), getELExpression("dataIn('" + input.getName() + "', '" + input.getPartition() + "')"));
                else
                	parentWFprops.put(input.getName(), "${coord:dataIn('" + input.getName() + "')}");     
                
                subflowProps.put(input.getName(), getVarName(input.getName())); 
            }
        }

        // outputs
        if (process.getOutputs() != null) {
        	StringBuilder outputFeedPaths = new StringBuilder();
        	StringBuilder outputFeedNames = new StringBuilder();
            for (Output output : process.getOutputs().getOutput()) {
                SYNCDATASET syncdataset = createDataSet(output.getFeed(), cluster);
                if (coord.getDatasets() == null)
                    coord.setDatasets(new DATASETS());
                coord.getDatasets().getDatasetOrAsyncDataset().add(syncdataset);

                DATAOUT dataout = new DATAOUT();
                dataout.setName(output.getName());
                dataout.setDataset(output.getFeed());
                dataout.setInstance(getELExpression(output.getInstance()));
                if (coord.getOutputEvents() == null)
                    coord.setOutputEvents(new OUTPUTEVENTS());
                coord.getOutputEvents().getDataOut().add(dataout);
                
                outputFeedNames.append(output.getName()).append(",");
                outputFeedPaths.append("${coord:dataOut('" + output.getName() + "')}").append(",");                
               
                parentWFprops.put(output.getName(), "${coord:dataOut('" + output.getName() + "')}");
                subflowProps.put(output.getName(), getVarName(output.getName()));
            }
            //Output feed name and path for parent workflow
            parentWFprops.put(EntityInstanceMessage.ARG.FEED_NAME.NAME(), outputFeedNames.substring(0, outputFeedNames.length()-1));
            parentWFprops.put(EntityInstanceMessage.ARG.FEED_INSTANCE_PATH.NAME(), outputFeedPaths.substring(0, outputFeedNames.length()-1));
            
        }

        String libDir = getLibDirectory(process.getWorkflow().getPath(), cluster);
        if(libDir != null)
        	parentWFprops.put(OozieClient.LIBPATH, libDir);      

        //configuration
        CONFIGURATION conf = new CONFIGURATION();
        for(Entry<String, String> entry:parentWFprops.entrySet())
            conf.getProperty().add(createCoordProperty(entry.getKey(), entry.getValue()));
        
		//user wf (sub-flow) confs, add all user defined props to coordinator
		for (Entry<String, String> entry : getUserDefinedProps().entrySet())
			conf.getProperty().add(
					createCoordProperty(entry.getKey(), entry.getValue()));
            
        //action
        WORKFLOW wf = new WORKFLOW();
        //set the action to parent workflow
        wf.setAppPath(getHDFSPath(getParentWorkflowPath().toString()));
        wf.setConfiguration(conf);

        org.apache.ivory.oozie.coordinator.ACTION action = new org.apache.ivory.oozie.coordinator.ACTION();
        action.setWorkflow(wf);
        coord.setAction(action);

        try {
            if (LOG.isDebugEnabled()) {
                Marshaller marshaller = coordJaxbContext.createMarshaller();
                marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
                StringWriter writer = new StringWriter();
                marshaller.marshal(new ObjectFactory().createCoordinatorApp(coord), writer);
                LOG.debug(writer.getBuffer());
            }
        } catch (JAXBException e) {
            LOG.error("Unable to marshal coordinator app instance for debug", e);
        }

        return coord;
    }

    private String getLibDirectory(String wfpath, Cluster cluster) throws IvoryException {
        Path path = new Path(wfpath.replace("${nameNode}", ""));
        String libDir;
        try {
            FileSystem fs = FileSystem.get(ClusterHelper.getConfiguration(cluster));
            FileStatus status = fs.getFileStatus(path);
            if(status.isDir())
                libDir = path.toString() + "/lib";
            else
                libDir = path.getParent().toString() + "/lib";
            
            if(fs.exists(new Path(libDir)))
                return "${nameNode}" + libDir;
        } catch (IOException e) {
            throw new IvoryException(e);
        }
        return null;
    }

    private SYNCDATASET createDataSet(String feedName, Cluster cluster) throws IvoryException {
        Feed feed;
        try {
            feed = ConfigurationStore.get().get(EntityType.FEED, feedName);
        } catch (Exception e) {
            throw new IvoryException(e);
        }
        if (feed == null) // This should never happen as its checked in process validation
            throw new RuntimeException("Referenced feed " + feedName + " is not registered!");

        SYNCDATASET syncdataset = new SYNCDATASET();
        syncdataset.setName(feed.getName());
        syncdataset.setUriTemplate("${nameNode}" + feed.getLocations().get(LocationType.DATA).getPath());
        syncdataset.setFrequency("${coord:" + feed.getFrequency() + "(" + feed.getPeriodicity() + ")}");

        org.apache.ivory.entity.v0.feed.Cluster feedCluster =
                getCluster(feed.getClusters().getCluster(), cluster.getName());
        syncdataset.setInitialInstance(feedCluster.getValidity().getStart());
        syncdataset.setTimezone(feedCluster.getValidity().getTimezone());
        syncdataset.setDoneFlag("");
        return syncdataset;
    }

    private org.apache.ivory.entity.v0.feed.Cluster getCluster(
            List<org.apache.ivory.entity.v0.feed.Cluster> clusters,
            String clusterName) {

        if (clusters != null) {
            for (org.apache.ivory.entity.v0.feed.Cluster cluster : clusters)
                if (cluster.getName().equals(clusterName))
                    return cluster;
        }
        return null;
    }

    private String getELExpression(String expr) {
        if (expr != null) {
            expr = "${" + EL_PREFIX + expr + "}";
        }
        return expr;
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

}