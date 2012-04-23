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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.ExternalId;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.cluster.Property;
import org.apache.ivory.messaging.EntityInstanceMessage;
import org.apache.ivory.oozie.bundle.BUNDLEAPP;
import org.apache.ivory.oozie.bundle.CONFIGURATION;
import org.apache.ivory.oozie.bundle.COORDINATOR;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.apache.ivory.oozie.coordinator.ObjectFactory;
import org.apache.ivory.oozie.workflow.WORKFLOWAPP;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;

import javax.xml.bind.*;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public abstract class AbstractOozieEntityMapper<T extends Entity> {

    private static Logger LOG = Logger.getLogger(AbstractOozieEntityMapper.class);

    protected static final String NOMINAL_TIME_EL = "${coord:formatTime(coord:nominalTime(), 'yyyy-MM-dd-HH-mm')}";
    protected static final String LATE_NOMINAL_TIME_EL = "${coord:formatTime(coord:dateOffset(coord:nominalTime(), "
            + "#VAL#, 'MINUTE'),'yyyy-MM-dd-HH-mm')}";
    protected static final String ACTUAL_TIME_EL = "${coord:formatTime(coord:actualTime(), 'yyyy-MM-dd-HH-mm')}";
    protected static final String DEFAULT_BROKER_IMPL_CLASS = "org.apache.activemq.ActiveMQConnectionFactory";

    protected static final JAXBContext workflowJaxbContext;
    protected static final JAXBContext coordJaxbContext;
    protected static final JAXBContext bundleJaxbContext;

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
        String path = getEntity().getWorkflowNameTag(coordName);
        return new Path(bundlePath, path);
    }

    protected abstract Map<String, String> getEntityProperties();

    public void map(Cluster cluster, Path bundlePath) throws IvoryException {
        BUNDLEAPP bundleApp = new BUNDLEAPP();
        bundleApp.setName(entity.getWorkflowName());
        // all the properties are set prior to bundle and coordinators creation
        CONFIGURATION bundleConf = createBundleConf(cluster);

        List<COORDINATORAPP> coordinators = getCoordinators(cluster, bundlePath);

        for (COORDINATORAPP coordinatorapp : coordinators) {
            Path coordPath = getCoordPath(bundlePath, coordinatorapp.getName());
            marshal(cluster, coordinatorapp, coordPath);
            createTempDir(cluster, coordPath);
            COORDINATOR bundleCoord = new COORDINATOR();
            bundleCoord.setName(coordinatorapp.getName());
            bundleCoord.setAppPath(getHDFSPath(coordPath));
            bundleCoord.setConfiguration(bundleConf);
            bundleApp.getCoordinator().add(bundleCoord);
        }

        marshal(cluster, bundleApp, bundlePath);
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
        props.put(EntityInstanceMessage.ARG.PROCESS_NAME.NAME(), entity.getName());
        props.put(EntityInstanceMessage.ARG.NOMINAL_TIME.NAME(), NOMINAL_TIME_EL);
        props.put(EntityInstanceMessage.ARG.TIME_STAMP.NAME(), ACTUAL_TIME_EL);
        props.put(EntityInstanceMessage.ARG.BROKER_URL.NAME(), ClusterHelper.getMessageBrokerUrl(cluster));
        props.put(EntityInstanceMessage.ARG.BROKER_IMPL_CLASS.NAME(), DEFAULT_BROKER_IMPL_CLASS);
        props.put(EntityInstanceMessage.ARG.ENTITY_TYPE.NAME(), entity.getEntityType().name());
        props.put("logDir", getHDFSPath(new Path(coordPath, "../tmp")));
        props.put(EntityInstanceMessage.ARG.OPERATION.NAME(), EntityInstanceMessage.entityOperation.GENERATE.name());

        props.put(OozieClient.EXTERNAL_ID,
                new ExternalId(entity.getName(), entity.getWorkflowNameTag(coordName), "${coord:nominalTime()}").getId());

        props.putAll(getEntityProperties());
        return props;
    }

    protected CONFIGURATION createBundleConf(Cluster cluster) {
        CONFIGURATION conf = new CONFIGURATION();
        List<CONFIGURATION.Property> props = conf.getProperty();
        props.add(createBundleProperty("entityName", entity.getName()));
        props.add(createBundleProperty("entityType", entity.getEntityType().name().toLowerCase()));

        for (Property property : cluster.getProperties().values()) {
            props.add(createBundleProperty(property.getName(), property.getValueAttribute()));
        }
        return conf;
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
            Path tempDir = new Path(coordPath, "../tmp");
            fs.mkdirs(tempDir);
            fs.setPermission(tempDir, new FsPermission((short) 511));
        } catch (Exception e) {
            throw new IvoryException("Unable to create temp dir in " + coordPath, e);
        }
    }

    protected void marshal(Cluster cluster, COORDINATORAPP coord, Path outPath) throws IvoryException {

        marshal(cluster, new ObjectFactory().createCoordinatorApp(coord), coordJaxbContext, new Path(outPath, "coordinator.xml"));
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
}
