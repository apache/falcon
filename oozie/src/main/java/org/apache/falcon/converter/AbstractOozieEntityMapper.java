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

package org.apache.falcon.converter;

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.FalconRuntimException;
import org.apache.falcon.Tag;
import org.apache.commons.io.IOUtils;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.ExternalId;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Property;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.messaging.EntityInstanceMessage.ARG;
import org.apache.falcon.oozie.bundle.BUNDLEAPP;
import org.apache.falcon.oozie.bundle.COORDINATOR;
import org.apache.falcon.oozie.coordinator.COORDINATORAPP;
import org.apache.falcon.oozie.coordinator.ObjectFactory;
import org.apache.falcon.oozie.workflow.ACTION;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.falcon.security.SecurityUtil;
import org.apache.falcon.service.FalconPathFilter;
import org.apache.falcon.service.SharedLibraryHostingService;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;

import javax.xml.bind.*;
import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Entity mapper base class that allows an entity to be mapped to oozie bundle.
 * @param <T>
 */
public abstract class AbstractOozieEntityMapper<T extends Entity> {

    private static final Logger LOG = Logger.getLogger(AbstractOozieEntityMapper.class);

    protected static final String NOMINAL_TIME_EL = "${coord:formatTime(coord:nominalTime(), 'yyyy-MM-dd-HH-mm')}";

    protected static final String ACTUAL_TIME_EL = "${coord:formatTime(coord:actualTime(), 'yyyy-MM-dd-HH-mm')}";
    protected static final Long DEFAULT_BROKER_MSG_TTL = 3 * 24 * 60L;
    protected static final String MR_QUEUE_NAME = "queueName";
    protected static final String MR_JOB_PRIORITY = "jobPriority";

    protected static final JAXBContext WORKFLOW_JAXB_CONTEXT;
    protected static final JAXBContext COORD_JAXB_CONTEXT;
    protected static final JAXBContext BUNDLE_JAXB_CONTEXT;
    protected static final JAXBContext HIVE_ACTION_JAXB_CONTEXT;
    public static final Set<String> FALCON_ACTIONS = new HashSet<String>(Arrays.asList(new String[] { "recordsize",
        "succeeded-post-processing", "failed-post-processing", }));

    protected static final FalconPathFilter FALCON_JAR_FILTER = new FalconPathFilter() {
        @Override
        public boolean accept(Path path) {
            return path.getName().startsWith("falcon");
        }

        @Override
        public String getJarName(Path path) {
            String name = path.getName();
            if (name.endsWith(".jar")) {
                name = name.substring(0, name.indexOf(".jar"));
            }
            return name;
        }
    };

    static {
        try {
            WORKFLOW_JAXB_CONTEXT = JAXBContext.newInstance(WORKFLOWAPP.class);
            COORD_JAXB_CONTEXT = JAXBContext.newInstance(COORDINATORAPP.class);
            BUNDLE_JAXB_CONTEXT = JAXBContext.newInstance(BUNDLEAPP.class);
            HIVE_ACTION_JAXB_CONTEXT = JAXBContext.newInstance(
                    org.apache.falcon.oozie.hive.ACTION.class.getPackage().getName());
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

    public boolean map(Cluster cluster, Path bundlePath) throws FalconException {
        BUNDLEAPP bundleApp = new BUNDLEAPP();
        bundleApp.setName(EntityUtil.getWorkflowName(entity).toString());
        // all the properties are set prior to bundle and coordinators creation

        List<COORDINATORAPP> coordinators = getCoordinators(cluster, bundlePath);
        if (coordinators.size() == 0) {
            return false;
        }
        for (COORDINATORAPP coordinatorapp : coordinators) {
            Path coordPath = getCoordPath(bundlePath, coordinatorapp.getName());
            String coordXmlName = marshal(cluster, coordinatorapp, coordPath,
                    EntityUtil.getWorkflowNameSuffix(coordinatorapp.getName(), entity));
            createLogsDir(cluster, coordPath);
            COORDINATOR bundleCoord = new COORDINATOR();
            bundleCoord.setName(coordinatorapp.getName());
            bundleCoord.setAppPath(getStoragePath(coordPath) + "/" + coordXmlName);
            bundleApp.getCoordinator().add(bundleCoord);

            copySharedLibs(cluster, coordPath);
        }

        marshal(cluster, bundleApp, bundlePath);
        return true;
    }

    private void addExtensionJars(FileSystem fs, Path path, WORKFLOWAPP wf) throws IOException {
        FileStatus[] libs = null;
        try {
            libs = fs.listStatus(path);
        } catch(FileNotFoundException ignore) {
            //Ok if the libext is not configured
        }

        if (libs == null) {
            return;
        }

        for(FileStatus lib : libs) {
            if (lib.isDir()) {
                continue;
            }

            for(Object obj: wf.getDecisionOrForkOrJoin()) {
                if (!(obj instanceof ACTION)) {
                    continue;
                }
                ACTION action = (ACTION) obj;
                List<String> files = null;
                if (action.getJava() != null) {
                    files = action.getJava().getFile();
                } else if (action.getPig() != null) {
                    files = action.getPig().getFile();
                } else if (action.getMapReduce() != null) {
                    files = action.getMapReduce().getFile();
                }
                if (files != null) {
                    files.add(lib.getPath().toString());
                }
            }
        }
    }

    protected void addLibExtensionsToWorkflow(Cluster cluster, WORKFLOWAPP wf, EntityType type, String lifecycle)
        throws IOException, FalconException {
        String libext = ClusterHelper.getLocation(cluster, "working") + "/libext";
        FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(ClusterHelper.getConfiguration(cluster));
        addExtensionJars(fs, new Path(libext), wf);
        addExtensionJars(fs, new Path(libext, type.name()), wf);
        if (StringUtils.isNotEmpty(lifecycle)) {
            addExtensionJars(fs, new Path(libext, type.name() + "/" + lifecycle), wf);
        }
    }

    private void copySharedLibs(Cluster cluster, Path coordPath) throws FalconException {
        try {
            Path libPath = new Path(coordPath, "lib");
            SharedLibraryHostingService.pushLibsToHDFS(StartupProperties.get().getProperty("system.lib.location"),
                    libPath, cluster, FALCON_JAR_FILTER);
        } catch (IOException e) {
            throw new FalconException("Failed to copy shared libs on cluster " + cluster.getName(), e);
        }
    }

    protected abstract List<COORDINATORAPP> getCoordinators(Cluster cluster, Path bundlePath) throws FalconException;

    protected org.apache.falcon.oozie.coordinator.CONFIGURATION getCoordConfig(Map<String, String> propMap) {
        org.apache.falcon.oozie.coordinator.CONFIGURATION conf
            = new org.apache.falcon.oozie.coordinator.CONFIGURATION();
        List<org.apache.falcon.oozie.coordinator.CONFIGURATION.Property> props = conf.getProperty();
        for (Entry<String, String> prop : propMap.entrySet()) {
            props.add(createCoordProperty(prop.getKey(), prop.getValue()));
        }
        return conf;
    }

    protected Map<String, String> createCoordDefaultConfiguration(Cluster cluster, Path coordPath, String coordName) {
        Map<String, String> props = new HashMap<String, String>();
        props.put(ARG.entityName.getPropName(), entity.getName());
        props.put(ARG.nominalTime.getPropName(), NOMINAL_TIME_EL);
        props.put(ARG.timeStamp.getPropName(), ACTUAL_TIME_EL);
        props.put("userBrokerUrl", ClusterHelper.getMessageBrokerUrl(cluster));
        props.put("userBrokerImplClass", ClusterHelper.getMessageBrokerImplClass(cluster));
        String falconBrokerUrl = StartupProperties.get().getProperty(ARG.brokerUrl.getPropName(),
                "tcp://localhost:61616?daemon=true");
        props.put(ARG.brokerUrl.getPropName(), falconBrokerUrl);
        String falconBrokerImplClass = StartupProperties.get().getProperty(ARG.brokerImplClass.getPropName(),
                ClusterHelper.DEFAULT_BROKER_IMPL_CLASS);
        props.put(ARG.brokerImplClass.getPropName(), falconBrokerImplClass);
        String jmsMessageTTL = StartupProperties.get().getProperty("broker.ttlInMins",
                DEFAULT_BROKER_MSG_TTL.toString());
        props.put(ARG.brokerTTL.getPropName(), jmsMessageTTL);
        props.put(ARG.entityType.getPropName(), entity.getEntityType().name());
        props.put("logDir", getStoragePath(new Path(coordPath, "../../logs")));
        props.put(OozieClient.EXTERNAL_ID,
                new ExternalId(entity.getName(), EntityUtil.getWorkflowNameTag(coordName, entity),
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
        } catch (FalconException e) {
            LOG.error("Unable to get Late Process for entity:" + entity, e);
            throw new FalconRuntimException(e);
        }
        props.put("entityName", entity.getName());
        props.put("entityType", entity.getEntityType().name().toLowerCase());
        props.put(ARG.cluster.getPropName(), cluster.getName());
        if (cluster.getProperties() != null) {
            for (Property prop : cluster.getProperties().getProperties()) {
                props.put(prop.getName(), prop.getValue());
            }
        }

        props.put(MR_QUEUE_NAME, "default");
        props.put(MR_JOB_PRIORITY, "NORMAL");
        //props in entity override the set props.
        props.putAll(getEntityProperties());
        return props;
    }

    protected org.apache.falcon.oozie.coordinator.CONFIGURATION.Property createCoordProperty(String name,
                                                                                             String value) {
        org.apache.falcon.oozie.coordinator.CONFIGURATION.Property prop
            = new org.apache.falcon.oozie.coordinator.CONFIGURATION.Property();
        prop.setName(name);
        prop.setValue(value);
        return prop;
    }

    protected void marshal(Cluster cluster, JAXBElement<?> jaxbElement, JAXBContext jaxbContext, Path outPath)
        throws FalconException {
        try {
            Marshaller marshaller = jaxbContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            FileSystem fs = HadoopClientFactory.get().createFileSystem(
                    outPath.toUri(), ClusterHelper.getConfiguration(cluster));
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
            throw new FalconException("Unable to marshall app object", e);
        }
    }

    private void createLogsDir(Cluster cluster, Path coordPath) throws FalconException {
        try {
            FileSystem fs = HadoopClientFactory.get().createFileSystem(
                    coordPath.toUri(), ClusterHelper.getConfiguration(cluster));
            Path logsDir = new Path(coordPath, "../../logs");
            fs.mkdirs(logsDir);

            // logs are copied with in oozie as the user in Post Processing and hence 777 permissions
            FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
            fs.setPermission(logsDir, permission);
        } catch (Exception e) {
            throw new FalconException("Unable to create temp dir in " + coordPath, e);
        }
    }

    protected String marshal(Cluster cluster, COORDINATORAPP coord, Path outPath, String name) throws FalconException {
        if (StringUtils.isEmpty(name)) {
            name = "coordinator";
        }
        name = name + ".xml";
        marshal(cluster, new ObjectFactory().createCoordinatorApp(coord), COORD_JAXB_CONTEXT, new Path(outPath, name));
        return name;
    }

    protected void marshal(Cluster cluster, BUNDLEAPP bundle, Path outPath) throws FalconException {

        marshal(cluster, new org.apache.falcon.oozie.bundle.ObjectFactory().createBundleApp(bundle),
                BUNDLE_JAXB_CONTEXT,
                new Path(outPath, "bundle.xml"));
    }

    protected void marshal(Cluster cluster, WORKFLOWAPP workflow, Path outPath) throws FalconException {

        marshal(cluster, new org.apache.falcon.oozie.workflow.ObjectFactory().createWorkflowApp(workflow),
                WORKFLOW_JAXB_CONTEXT,
                new Path(outPath, "workflow.xml"));
    }

    protected String getStoragePath(Path path) {
        if (path != null) {
            return getStoragePath(path.toString());
        }
        return null;
    }

    protected String getStoragePath(String path) {
        if (StringUtils.isNotEmpty(path)) {
            if (new Path(path).toUri().getScheme() == null) {
                path = "${nameNode}" + path;
            }
        }
        return path;
    }

    protected WORKFLOWAPP getWorkflowTemplate(String template) throws FalconException {
        InputStream resourceAsStream = null;
        try {
            resourceAsStream = AbstractOozieEntityMapper.class.getResourceAsStream(template);
            Unmarshaller unmarshaller = WORKFLOW_JAXB_CONTEXT.createUnmarshaller();
            @SuppressWarnings("unchecked")
            JAXBElement<WORKFLOWAPP> jaxbElement = (JAXBElement<WORKFLOWAPP>) unmarshaller.unmarshal(
                    resourceAsStream);
            return jaxbElement.getValue();
        } catch (JAXBException e) {
            throw new FalconException(e);
        } finally {
            IOUtils.closeQuietly(resourceAsStream);
        }
    }

    protected COORDINATORAPP getCoordinatorTemplate(String template) throws FalconException {
        InputStream resourceAsStream = null;
        try {
            resourceAsStream = AbstractOozieEntityMapper.class.getResourceAsStream(template);
            Unmarshaller unmarshaller = COORD_JAXB_CONTEXT.createUnmarshaller();
            @SuppressWarnings("unchecked")
            JAXBElement<COORDINATORAPP> jaxbElement = (JAXBElement<COORDINATORAPP>)
                    unmarshaller.unmarshal(resourceAsStream);
            return jaxbElement.getValue();
        } catch (JAXBException e) {
            throw new FalconException(e);
        } finally {
            IOUtils.closeQuietly(resourceAsStream);
        }
    }

    protected void createHiveConf(FileSystem fs, Path confPath, String metastoreUrl,
                                  Cluster cluster, String prefix) throws IOException {
        Configuration hiveConf = new Configuration(false);
        hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, metastoreUrl);
        hiveConf.set("hive.metastore.local", "false");

        if (UserGroupInformation.isSecurityEnabled()) {
            hiveConf.set(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname,
                    ClusterHelper.getPropertyValue(cluster, SecurityUtil.HIVE_METASTORE_PRINCIPAL));
            hiveConf.set(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname, "true");
        }

        OutputStream out = null;
        try {
            out = fs.create(new Path(confPath, prefix + "hive-site.xml"));
            hiveConf.writeXml(out);
        } finally {
            IOUtils.closeQuietly(out);
        }
    }

    protected void decorateWithOozieRetries(ACTION action) {
        Properties props = RuntimeProperties.get();
        action.setRetryMax(props.getProperty("falcon.parentworkflow.retry.max", "3"));
        action.setRetryInterval(props.getProperty("falcon.parentworkflow.retry.interval.secs", "1"));
    }
}
