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
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.cluster.Property;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.oozie.bundle.BUNDLEAPP;
import org.apache.ivory.oozie.bundle.CONFIGURATION;
import org.apache.ivory.oozie.bundle.COORDINATOR;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.apache.ivory.oozie.coordinator.ObjectFactory;
import org.apache.ivory.oozie.workflow.WORKFLOWAPP;
import org.apache.ivory.workflow.engine.OozieWorkflowEngine;
import org.apache.log4j.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import java.io.OutputStream;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public abstract class AbstractOozieEntityMapper<T extends Entity> {

	private static Logger LOG = Logger.getLogger(AbstractOozieEntityMapper.class);
	
    protected static final String NOMINAL_TIME_EL="${coord:formatTime(coord:nominalTime(), 'yyyy-MM-dd-HH-mm')}";
    protected static final String ACTUAL_TIME_EL="${coord:formatTime(coord:actualTime(), 'yyyy-MM-dd-HH-mm')}";
    protected static final String DEFAULT_BROKER_IMPL_CLASS="org.apache.activemq.ActiveMQConnectionFactory";

	protected static final JAXBContext workflowJaxbContext;
	protected static final JAXBContext coordJaxbContext;
	protected static final JAXBContext bundleJaxbContext;
	
	private Map<String, String> userDefinedProps = new HashMap<String, String>();
	org.apache.ivory.oozie.bundle.CONFIGURATION bundleConf = new
			org.apache.ivory.oozie.bundle.CONFIGURATION();

	static {
		try {
			workflowJaxbContext = JAXBContext.newInstance(WORKFLOWAPP.class);
			coordJaxbContext = JAXBContext.newInstance(COORDINATORAPP.class);
			bundleJaxbContext = JAXBContext.newInstance(BUNDLEAPP.class);
		} catch (JAXBException e) {
			throw new RuntimeException("Unable to create JAXB context", e);
		}
	}

	private final T entity;
	
	private Path workflowBasePath;

	protected AbstractOozieEntityMapper(T entity) {
		this.entity = entity;
	}

	protected T getEntity() {
		return entity;
	}

	private final Map<COORDINATORAPP, String> basePathMap = new HashMap<COORDINATORAPP, String>();

	protected COORDINATORAPP newCOORDINATORAPP(String basePath) {
		COORDINATORAPP coordinatorApp = new COORDINATORAPP();
		basePathMap.put(coordinatorApp,  basePath);
		return coordinatorApp;
	}

	protected String getCoordBasePath(COORDINATORAPP coordinatorApp) {
		if (!basePathMap.containsKey(coordinatorApp)) {
			throw new IllegalStateException("Use newCoordinatorApp method in " +
					"to create an COORDINATOR APP instance");
		}
		return basePathMap.get(coordinatorApp);
	}

	public Path convert(Cluster cluster, Path workflowBasePath) throws IvoryException {
		this.workflowBasePath=workflowBasePath;
		BUNDLEAPP bundleApp = new BUNDLEAPP();
		//all the properties are set prior to bundle and coordinators creation
		createBundleAndUserConf(cluster);
		bundleApp.setName(entity.getWorkflowName() + "_" + entity.getName());
		
		List<COORDINATORAPP> coordinators = getCoordinators(cluster);

		for (COORDINATORAPP coordinatorapp : coordinators) {
			Path coordPath = new Path(workflowBasePath,
					getCoordBasePath(coordinatorapp) + "/coordinator.xml");
			if(LOG.isDebugEnabled()) {
				debug(coordinatorapp, coordPath);
			}
			marshal(cluster, coordinatorapp, coordPath);
			COORDINATOR bundleCoord = new COORDINATOR();
			bundleCoord.setName(coordinatorapp.getName());
			bundleCoord.setAppPath("${" + OozieWorkflowEngine.NAME_NODE + "}" + coordPath);
			bundleCoord.setConfiguration(bundleConf);
			bundleApp.getCoordinator().add(bundleCoord);
		}
		Path bundlePath = new Path(workflowBasePath, entity.getWorkflowName()
				+ "/bundle.xml");
		marshal(cluster, bundleApp, bundlePath);

		Path parentWorkflowPath = getParentWorkflowPath();
		WORKFLOWAPP parentWorkflowApp = getParentWorkflow(cluster);
		marshal(cluster, parentWorkflowApp, parentWorkflowPath);

		if (LOG.isDebugEnabled()) {
			debug(bundleApp, bundlePath);
			debug(parentWorkflowApp, parentWorkflowPath);
		}
		return bundlePath;
	}

	private void debug(COORDINATORAPP coordinatorapp, Path coordPath) {
		try{
			Marshaller marshaller = coordJaxbContext.createMarshaller();
			marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			StringWriter writer = new StringWriter();
			marshaller.marshal(new ObjectFactory().
					createCoordinatorApp(coordinatorapp), writer);
			LOG.debug("Writing coordinator definition to " + coordPath);
			LOG.debug(writer.getBuffer());
		} catch(Exception e) {
			LOG.warn("Unable to marshall app object in " + coordPath, e);
		}
	}

	private void debug(BUNDLEAPP bundleApp, Path bundlePath) {
		try{
			Marshaller marshaller = bundleJaxbContext.createMarshaller();
			marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			StringWriter writer = new StringWriter();
			marshaller.marshal(new org.apache.ivory.oozie.bundle.ObjectFactory().
					createBundleApp(bundleApp), writer);
			LOG.debug("Writing bundle definition to " + bundlePath);
			LOG.debug(writer.getBuffer());
		} catch(Exception e) {
			LOG.warn("Unable to marshall app object in " + bundlePath, e);
		}
	}
	
	private void debug(WORKFLOWAPP workflowApp, Path workflowPath) {
		try{
			Marshaller marshaller = workflowJaxbContext.createMarshaller();
			marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			StringWriter writer = new StringWriter();
			marshaller.marshal(new org.apache.ivory.oozie.workflow.ObjectFactory().
					createWorkflowApp(workflowApp), writer);
			LOG.debug("Writing bundle definition to " + workflowPath);
			LOG.debug(writer.getBuffer());
		} catch(Exception e) {
			LOG.warn("Unable to marshall app object in " + workflowPath, e);
		}
	}

	protected abstract List<COORDINATORAPP> getCoordinators(Cluster cluster)
			throws IvoryException;
	
	protected abstract WORKFLOWAPP getParentWorkflow(Cluster cluster)
			throws IvoryException;

	protected void createBundleAndUserConf(Cluster cluster) {

		List<CONFIGURATION.Property> bundleProps = bundleConf.getProperty();
		bundleProps.add(createBundleProperty(OozieWorkflowEngine.NAME_NODE,
				"${" + OozieWorkflowEngine.NAME_NODE + "}"));
		bundleProps.add(createBundleProperty(OozieWorkflowEngine.JOB_TRACKER,
				"${" + OozieWorkflowEngine.JOB_TRACKER + "}"));
		bundleProps.add(createBundleProperty("entityName", entity.getName()));
		bundleProps.add(createBundleProperty("entityType", entity.
				getEntityType().name().toLowerCase()));

		for (Property property : cluster.getProperties().values()) {
			bundleProps.add(createBundleProperty(property.getName(),
					property.getValueAttribute()));
		}

		//populate map to be used by subclasses (bundle props + cluster + (feed or process)
		for (org.apache.ivory.oozie.bundle.CONFIGURATION.Property prop : bundleProps) {
			userDefinedProps.put(prop.getName(), prop.getValue());
		}		
		if (entity.getEntityType() == EntityType.PROCESS) {
			for (org.apache.ivory.entity.v0.process.Property property :
				((Process) entity).getProperties().getProperty()) {
				userDefinedProps.put(property.getName(),
						property.getValue());
			}
		} else {
			for (org.apache.ivory.entity.v0.feed.Property property :
				((Feed) entity).getProperties().values()) {
				userDefinedProps.put(property.getName(),
						property.getValue());
			}
		}

	}

	protected org.apache.ivory.oozie.coordinator.CONFIGURATION.Property
	createCoordProperty(String name, String value) {

		org.apache.ivory.oozie.coordinator.CONFIGURATION.Property prop = new
				org.apache.ivory.oozie.coordinator.CONFIGURATION.Property();
		prop.setName(name);
		prop.setValue(value);
		return prop;
	}

	protected org.apache.ivory.oozie.bundle.CONFIGURATION.Property
	createBundleProperty(String name, String value) {

		org.apache.ivory.oozie.bundle.CONFIGURATION.Property prop = new
				org.apache.ivory.oozie.bundle.CONFIGURATION.Property();
		prop.setName(name);
		prop.setValue(value);
		return prop;
	}

	protected org.apache.ivory.oozie.workflow.CONFIGURATION.Property 
	createWorkflowProperty(String name, String value) {
		
		org.apache.ivory.oozie.workflow.CONFIGURATION.Property prop = new 
				org.apache.ivory.oozie.workflow.CONFIGURATION.Property();
		prop.setName(name);
		prop.setValue(value);
		return prop;
	}

	protected void marshal(Cluster cluster, JAXBElement<?> jaxbElement,
			JAXBContext jaxbContext,
			Path outPath) throws IvoryException {
		try{
			Marshaller marshaller = jaxbContext.createMarshaller();
			marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
			FileSystem fs = outPath.getFileSystem(ClusterHelper.
					getConfiguration(cluster));
			OutputStream out = fs.create(outPath);
			try {
				marshaller.marshal(jaxbElement, out);
			} finally {
				out.close();
			}
			LOG.info("Marshalled " + jaxbElement.getDeclaredType() + " to " + outPath);
		} catch(Exception e) {
			throw new IvoryException("Unable to marshall app object", e);
		}
	}

	protected void marshal(Cluster cluster, COORDINATORAPP coord, Path outPath)
			throws IvoryException {

		marshal(cluster, new ObjectFactory().createCoordinatorApp(coord),
				coordJaxbContext, outPath);
	}

	protected void marshal(Cluster cluster, BUNDLEAPP bundle, Path outPath)
			throws IvoryException {

		marshal(cluster, new org.apache.ivory.oozie.bundle.ObjectFactory().
				createBundleApp(bundle), bundleJaxbContext, outPath);
	}

	protected void marshal(Cluster cluster, WORKFLOWAPP workflow, Path outPath)
			throws IvoryException {

		marshal(cluster, new org.apache.ivory.oozie.workflow.ObjectFactory().
				createWorkflowApp(workflow), workflowJaxbContext, outPath);
	}
	
	protected Path getParentWorkflowPath(){
		return new Path(this.workflowBasePath,
				getEntity().getWorkflowName() + "/workflow.xml");
	}

	protected void populateUserDefinedClusterProperties(Cluster cluster,
			Map<String, String> properties) {
		// user defined cluster properties
		if (cluster.getProperties() != null) {
			for (Entry<String, org.apache.ivory.entity.v0.cluster.Property> prop : cluster
					.getProperties().entrySet()) {
				properties.put(prop.getValue().getName(), prop.getValue()
						.getValue());
			}
		}
	}

	protected void populateUserDefinedFeedProperties(Feed feed,
			Map<String, String> properties) {
		// user defined feed properties
		if (feed.getProperties() != null) {
			for (Entry<String, org.apache.ivory.entity.v0.feed.Property> prop : feed
					.getProperties().entrySet()) {
				properties.put(prop.getValue().getName(), prop.getValue()
						.getValue());
			}
		}
	}

	protected void populateUserDefinedProcessProperties(Process process,
			Map<String, String> properties) {
		// user defined process properties
		if (process.getProperties() != null) {
			for (org.apache.ivory.entity.v0.process.Property prop : process
					.getProperties().getProperty()) {
				properties.put(prop.getName(), prop.getValue());
			}
		}
	}
	
    protected String getHDFSPath(String path) {
        if(path != null) {
            if(!path.startsWith("${nameNode}"))
                path = "${nameNode}" + path;
        }
        return path;
    }    
    
	protected Map<String, String> getUserDefinedProps() {
		return userDefinedProps;
	}
	
	protected String getVarName(String name) {
		return "${" + name + "}";
	}

}
