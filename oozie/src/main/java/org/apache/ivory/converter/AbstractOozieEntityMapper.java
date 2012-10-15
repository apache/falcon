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
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.ivory.IvoryException;
import org.apache.ivory.IvoryRuntimException;
import org.apache.ivory.Tag;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.ExternalId;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.cluster.Property;
import org.apache.ivory.messaging.EntityInstanceMessage.ARG;
import org.apache.ivory.oozie.bundle.BUNDLEAPP;
import org.apache.ivory.oozie.bundle.COORDINATOR;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.apache.ivory.oozie.coordinator.ObjectFactory;
import org.apache.ivory.oozie.workflow.WORKFLOWAPP;
import org.apache.ivory.service.IvoryPathFilter;
import org.apache.ivory.service.SharedLibraryHostingService;
import org.apache.ivory.util.StartupProperties;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;

public abstract class AbstractOozieEntityMapper<T extends Entity> {

    private static Logger LOG = Logger.getLogger(AbstractOozieEntityMapper.class);

    protected static final String NOMINAL_TIME_EL = "${coord:formatTime(coord:nominalTime(), 'yyyy-MM-dd-HH-mm')}";

    protected static final String ACTUAL_TIME_EL = "${coord:formatTime(coord:actualTime(), 'yyyy-MM-dd-HH-mm')}";
    protected static final Long DEFAULT_BROKER_MSG_TTL = 3 * 24 * 60L;
    protected static final String MR_QUEUE_NAME="queueName";
    protected static final String MR_JOB_PRIORITY="jobPriority";

    protected static final JAXBContext workflowJaxbContext;
    protected static final JAXBContext coordJaxbContext;
    protected static final JAXBContext bundleJaxbContext;

    protected static final IvoryPathFilter ivoryJarFilter = new IvoryPathFilter() {
        @Override
        public boolean accept(Path path) {
            if (path.getName().startsWith("ivory"))
                return true;
            return false;
        }

        @Override
        public String getJarName(Path path) {
            String name = path.getName();
            if(name.endsWith(".jar"))
                name = name.substring(0, name.indexOf(".jar"));
            return name;
        }
    };

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

    protected AbstractOozieEntityMapper(T entity) {
        this.entity = entity;
    }

    protected T getEntity() {
        return entity;
    }

    protected Path getCoordPath(Path bundlePath, String coordName) {
        Tag tag = EntityUtil.getWorkflowNameTag(coordName, getEntity());
        return new Path(bundlePath, tag.name());
    }

    protected abstract Map<String, String> getEntityProperties();

    public boolean map(Cluster cluster, Path bundlePath) throws IvoryException {
        BUNDLEAPP bundleApp = new BUNDLEAPP();
        bundleApp.setName(EntityUtil.getWorkflowName(entity).toString());
        // all the properties are set prior to bundle and coordinators creation

        List<COORDINATORAPP> coordinators = getCoordinators(cluster, bundlePath);
        if (coordinators.size() == 0) {
            return false;
        }
        for (COORDINATORAPP coordinatorapp : coordinators) {
            Path coordPath = getCoordPath(bundlePath, coordinatorapp.getName());
            String coordXmlName = marshal(cluster, coordinatorapp, coordPath, EntityUtil.getWorkflowNameSuffix(coordinatorapp.getName(), entity));
            createTempDir(cluster, coordPath);
            COORDINATOR bundleCoord = new COORDINATOR();
            bundleCoord.setName(coordinatorapp.getName());
            bundleCoord.setAppPath(getHDFSPath(coordPath) + "/" + coordXmlName);
            bundleApp.getCoordinator().add(bundleCoord);

            copySharedLibs(cluster, coordPath);
        }

        marshal(cluster, bundleApp, bundlePath);
        return true;
    }

    private void copySharedLibs(Cluster cluster, Path coordPath) throws IvoryException {
        try {
            Path libPath = new Path(coordPath, "lib");
            FileSystem fs = FileSystem.get(ClusterHelper.getConfiguration(cluster));
            if (!fs.exists(libPath))
                fs.mkdirs(libPath);

            SharedLibraryHostingService.pushLibsToHDFS(libPath.toString(), cluster, ivoryJarFilter);
        } catch (IOException e) {
            LOG.error("Failed to copy shared libs on cluster " + cluster.getName(), e);
            throw new IvoryException(e);
        }
    }

    protected abstract List<COORDINATORAPP> getCoordinators(Cluster cluster, Path bundlePath) throws IvoryException;

    protected org.apache.ivory.oozie.coordinator.CONFIGURATION getCoordConfig(Map<String, String> propMap) {
        org.apache.ivory.oozie.coordinator.CONFIGURATION conf = new org.apache.ivory.oozie.coordinator.CONFIGURATION();
        List<org.apache.ivory.oozie.coordinator.CONFIGURATION.Property> props = conf.getProperty();
        for (Entry<String, String> prop : propMap.entrySet())
            props.add(createCoordProperty(prop.getKey(), prop.getValue()));
        return conf;
    }

    protected Map<String, String> createCoordDefaultConfiguration(Cluster cluster, Path coordPath, String coordName) {
        Map<String, String> props = new HashMap<String, String>();
        props.put(ARG.entityName.getPropName(), entity.getName());
        props.put(ARG.nominalTime.getPropName(), NOMINAL_TIME_EL);
        props.put(ARG.timeStamp.getPropName(), ACTUAL_TIME_EL);
        props.put("userBrokerUrl", ClusterHelper.getMessageBrokerUrl(cluster));
        props.put("userBrokerImplClass", ClusterHelper.getMessageBrokerImplClass(cluster));
        String ivoryBrokerUrl = StartupProperties.get().getProperty(ARG.brokerUrl.getPropName(), "tcp://localhost:61616?daemon=true");
        props.put(ARG.brokerUrl.getPropName(), ivoryBrokerUrl);
        String ivoryBrokerImplClass = StartupProperties.get().getProperty(ARG.brokerImplClass.getPropName(),
                ClusterHelper.DEFAULT_BROKER_IMPL_CLASS);
        props.put(ARG.brokerImplClass.getPropName(), ivoryBrokerImplClass);
        String jmsMessageTTL = StartupProperties.get().getProperty("broker.ttlInMins", DEFAULT_BROKER_MSG_TTL.toString());
        props.put(ARG.brokerTTL.getPropName(), jmsMessageTTL);
        props.put(ARG.entityType.getPropName(), entity.getEntityType().name());
        props.put("logDir", getHDFSPath(new Path(coordPath, "../../logs")));
        props.put(OozieClient.EXTERNAL_ID, new ExternalId(entity.getName(), EntityUtil.getWorkflowNameTag(coordName, entity),
                "${coord:nominalTime()}").getId());
        props.put("workflowEngineUrl", ClusterHelper.getOozieUrl(cluster));
		try {
			if (EntityUtil.getLateProcess(entity) == null
					|| EntityUtil.getLateProcess(entity).getLateInputs() == null
					|| EntityUtil.getLateProcess(entity).getLateInputs().size() == 0) {
				props.put("shouldRecord", "false");
			} else {
				props.put("shouldRecord", "true");
			}
		} catch (IvoryException e) {
			LOG.error("Unable to get Late Process for entity:" + entity, e);
			throw new IvoryRuntimException(e);
		}
        props.put("entityName", entity.getName());
        props.put("entityType", entity.getEntityType().name().toLowerCase());
        props.put(ARG.cluster.getPropName(), cluster.getName());
        if(cluster.getProperties() != null)
            for(Property prop:cluster.getProperties().getProperties())
                props.put(prop.getName(), prop.getValue());
        
        props.put(MR_QUEUE_NAME, "default");
        props.put(MR_JOB_PRIORITY, "NORMAL");
        //props in entity override the set props.
        props.putAll(getEntityProperties());
        return props;
    }

    protected org.apache.ivory.oozie.coordinator.CONFIGURATION.Property createCoordProperty(String name, String value) {
        org.apache.ivory.oozie.coordinator.CONFIGURATION.Property prop = new org.apache.ivory.oozie.coordinator.CONFIGURATION.Property();
        prop.setName(name);
        prop.setValue(value);
        return prop;
    }

    protected org.apache.ivory.oozie.bundle.CONFIGURATION.Property createBundleProperty(String name, String value) {
        org.apache.ivory.oozie.bundle.CONFIGURATION.Property prop = new org.apache.ivory.oozie.bundle.CONFIGURATION.Property();
        prop.setName(name);
        prop.setValue(value);
        return prop;
    }

    protected void marshal(Cluster cluster, JAXBElement<?> jaxbElement, JAXBContext jaxbContext, Path outPath) throws IvoryException {
        try {
            Marshaller marshaller = jaxbContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            FileSystem fs = outPath.getFileSystem(ClusterHelper.getConfiguration(cluster));
            OutputStream out = fs.create(outPath);
            try {
                marshaller.marshal(jaxbElement, out);
            } finally {
                out.close();
            }
            if (LOG.isDebugEnabled()) {
                StringWriter writer = new StringWriter();
                marshaller.marshal(jaxbElement, writer);
                LOG.debug("Writing definition to " + outPath + " on cluster " + cluster.getName());
                LOG.debug(writer.getBuffer());
            }

            LOG.info("Marshalled " + jaxbElement.getDeclaredType() + " to " + outPath);
        } catch (Exception e) {
            throw new IvoryException("Unable to marshall app object", e);
        }
    }

    private void createTempDir(Cluster cluster, Path coordPath) throws IvoryException {
        try {
            FileSystem fs = coordPath.getFileSystem(ClusterHelper.getConfiguration(cluster));
            Path tempDir = new Path(coordPath, "../../logs");
            fs.mkdirs(tempDir);
            fs.setPermission(tempDir, new FsPermission((short) 511));
        } catch (Exception e) {
            throw new IvoryException("Unable to create temp dir in " + coordPath, e);
        }
    }

    protected String marshal(Cluster cluster, COORDINATORAPP coord, Path outPath, String name) throws IvoryException {
        if(StringUtils.isEmpty(name))
            name = "coordinator";
        name = name + ".xml";
        marshal(cluster, new ObjectFactory().createCoordinatorApp(coord), coordJaxbContext, new Path(outPath, name));
        return name;
    }

    protected void marshal(Cluster cluster, BUNDLEAPP bundle, Path outPath) throws IvoryException {

        marshal(cluster, new org.apache.ivory.oozie.bundle.ObjectFactory().createBundleApp(bundle), bundleJaxbContext, new Path(
                outPath, "bundle.xml"));
    }

    protected void marshal(Cluster cluster, WORKFLOWAPP workflow, Path outPath) throws IvoryException {

        marshal(cluster, new org.apache.ivory.oozie.workflow.ObjectFactory().createWorkflowApp(workflow), workflowJaxbContext,
                new Path(outPath, "workflow.xml"));
    }

    protected String getHDFSPath(Path path) {
        if (path != null)
            return getHDFSPath(path.toString());
        return null;
    }

    protected String getHDFSPath(String path) {
        if (StringUtils.isNotEmpty(path)) {
            if (!path.startsWith("${nameNode}"))
                path = "${nameNode}" + path;
        }
        return path;
    }

    protected WORKFLOWAPP getWorkflowTemplate(String template) throws IvoryException {
        try {
            Unmarshaller unmarshaller = workflowJaxbContext.createUnmarshaller();
            @SuppressWarnings("unchecked")
            JAXBElement<WORKFLOWAPP> jaxbElement = (JAXBElement<WORKFLOWAPP>) unmarshaller.unmarshal(this.getClass()
                    .getResourceAsStream(template));
            return jaxbElement.getValue();
        } catch (JAXBException e) {
            throw new IvoryException(e);
        }
    }

    protected COORDINATORAPP getCoordinatorTemplate(String template) throws IvoryException {
        try {
            Unmarshaller unmarshaller = coordJaxbContext.createUnmarshaller();
            @SuppressWarnings("unchecked")
            JAXBElement<COORDINATORAPP> jaxbElement = (JAXBElement<COORDINATORAPP>) unmarshaller
                    .unmarshal(AbstractOozieEntityMapper.class.getResourceAsStream(template));
            return jaxbElement.getValue();
        } catch (JAXBException e) {
            throw new IvoryException(e);
        }
    }
}
