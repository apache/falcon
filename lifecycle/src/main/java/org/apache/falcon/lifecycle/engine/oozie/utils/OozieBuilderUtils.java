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

package org.apache.falcon.lifecycle.engine.oozie.utils;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.ExternalId;
import org.apache.falcon.entity.HiveUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.ClusterLocationType;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.oozie.coordinator.CONFIGURATION;
import org.apache.falcon.oozie.coordinator.COORDINATORAPP;
import org.apache.falcon.oozie.workflow.ACTION;
import org.apache.falcon.oozie.workflow.CREDENTIAL;
import org.apache.falcon.oozie.workflow.CREDENTIALS;
import org.apache.falcon.oozie.workflow.END;
import org.apache.falcon.oozie.workflow.KILL;
import org.apache.falcon.oozie.workflow.START;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.falcon.security.SecurityUtil;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.falcon.workflow.engine.AbstractWorkflowEngine;
import org.apache.falcon.workflow.util.OozieConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.namespace.QName;
import javax.xml.transform.stream.StreamSource;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Utility class to build oozie artificats.
 */
public final class OozieBuilderUtils {
    private static final Logger LOG = LoggerFactory.getLogger(OozieBuilderUtils.class);

    private static final String POSTPROCESS_TEMPLATE = "/action/post-process.xml";

    public static final String HIVE_CREDENTIAL_NAME = "falconHiveAuth";
    public static final String MR_QUEUE_NAME = "queueName";
    public static final String MR_JOB_PRIORITY = "jobPriority";
    private static final String NOMINAL_TIME_EL = "${coord:formatTime(coord:nominalTime(), 'yyyy-MM-dd-HH-mm')}";
    private static final String ACTUAL_TIME_EL = "${coord:formatTime(coord:actualTime(), 'yyyy-MM-dd-HH-mm')}";
    private static final Long DEFAULT_BROKER_MSG_TTL = 3 * 24 * 60L;

    private static final JAXBContext WORKFLOW_JAXB_CONTEXT;
    private static final JAXBContext ACTION_JAXB_CONTEXT;
    private static final JAXBContext COORD_JAXB_CONTEXT;
    private static final JAXBContext CONFIG_JAXB_CONTEXT;


    public static final String SUCCESS_POSTPROCESS_ACTION_NAME = "succeeded-post-processing";
    public static final String FAIL_POSTPROCESS_ACTION_NAME = "failed-post-processing";
    public static final String OK_ACTION_NAME = "end";
    public static final String FAIL_ACTION_NAME = "fail";


    public static final String ENTITY_PATH = "ENTITY_PATH";
    public static final String ENTITY_NAME = "ENTITY_NAME";
    public static final String IGNORE = "IGNORE";
    public static final String ENABLE_POSTPROCESSING = StartupProperties.get().
            getProperty("falcon.postprocessing.enable");


    static {
        try {
            WORKFLOW_JAXB_CONTEXT = JAXBContext.newInstance(WORKFLOWAPP.class);
            ACTION_JAXB_CONTEXT = JAXBContext.newInstance(org.apache.falcon.oozie.workflow.ACTION.class);
            COORD_JAXB_CONTEXT = JAXBContext.newInstance(COORDINATORAPP.class);
            CONFIG_JAXB_CONTEXT = JAXBContext.newInstance(org.apache.falcon.oozie.workflow.CONFIGURATION.class);
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to create JAXB context", e);
        }
    }

    private OozieBuilderUtils() {

    }

    public static ACTION addTransition(ACTION action, String ok, String fail) {
        // XTODOS : why return when it is changing the same object?
        action.getOk().setTo(ok);
        action.getError().setTo(fail);
        return action;
    }


    public static void decorateWorkflow(WORKFLOWAPP wf, String name, String startAction) {
        wf.setName(name);
        wf.setStart(new START());
        wf.getStart().setTo(startAction);

        wf.setEnd(new END());
        wf.getEnd().setName(OK_ACTION_NAME);

        KILL kill = new KILL();
        kill.setName(FAIL_ACTION_NAME);
        kill.setMessage("Workflow failed, error message[${wf:errorMessage(wf:lastErrorNode())}]");
        wf.getDecisionOrForkOrJoin().add(kill);
    }

    public static ACTION getSuccessPostProcessAction() throws FalconException {
        ACTION action = unmarshalAction(POSTPROCESS_TEMPLATE);
        decorateWithOozieRetries(action);
        return action;
    }

    public static ACTION getFailPostProcessAction() throws FalconException {
        ACTION action = unmarshalAction(POSTPROCESS_TEMPLATE);
        decorateWithOozieRetries(action);
        action.setName(FAIL_POSTPROCESS_ACTION_NAME);
        return action;
    }

    private static Path marshal(Cluster cluster, JAXBElement<?> jaxbElement,
                           JAXBContext jaxbContext, Path outPath) throws FalconException {
        try {
            Marshaller marshaller = jaxbContext.createMarshaller();
            marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

            if (LOG.isDebugEnabled()) {
                StringWriter writer = new StringWriter();
                marshaller.marshal(jaxbElement, writer);
                LOG.debug("Writing definition to {} on cluster {}", outPath, cluster.getName());
                LOG.debug(writer.getBuffer().toString());
            }

            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(
                    outPath.toUri(), ClusterHelper.getConfiguration(cluster));
            OutputStream out = fs.create(outPath);
            try {
                marshaller.marshal(jaxbElement, out);
            } finally {
                out.close();
            }

            LOG.info("Marshalled {} to {}", jaxbElement.getDeclaredType(), outPath);
            return outPath;
        } catch (Exception e) {
            throw new FalconException("Unable to marshall app object", e);
        }
    }

    public static Path marshalCoordinator(Cluster cluster, COORDINATORAPP coord, Path outPath) throws FalconException {
        return marshal(cluster, new org.apache.falcon.oozie.coordinator.ObjectFactory().createCoordinatorApp(coord),
                COORD_JAXB_CONTEXT, new Path(outPath, "coordinator.xml"));
    }


    public static Path marshalDefaultConfig(Cluster cluster, WORKFLOWAPP workflowapp,
               Properties properties, Path outPath) throws FalconException {
        QName workflowQName = new org.apache.falcon.oozie.workflow.ObjectFactory()
                .createWorkflowApp(workflowapp).getName();
        org.apache.falcon.oozie.workflow.CONFIGURATION config = getWorkflowConfig(properties);
        JAXBElement<org.apache.falcon.oozie.workflow.CONFIGURATION> configJaxbElement =
                new JAXBElement(new QName(workflowQName.getNamespaceURI(), "configuration", workflowQName.getPrefix()),
                        org.apache.falcon.oozie.workflow.CONFIGURATION.class, config);

        Path defaultConfigPath = new Path(outPath, "config-default.xml");
        return marshal(cluster, configJaxbElement, CONFIG_JAXB_CONTEXT, defaultConfigPath);
    }


    public static Path marshalWokflow(Cluster cluster, WORKFLOWAPP workflow, Path outPath) throws FalconException {
        return marshal(cluster, new org.apache.falcon.oozie.workflow.ObjectFactory().createWorkflowApp(workflow),
                WORKFLOW_JAXB_CONTEXT, new Path(outPath, "workflow.xml"));
    }

    public static <T> T unmarshal(String template, JAXBContext context, Class<T> cls) throws FalconException {
        InputStream resourceAsStream = null;
        try {
            resourceAsStream = OozieBuilderUtils.class.getResourceAsStream(template);
            Unmarshaller unmarshaller = context.createUnmarshaller();
            JAXBElement<T> jaxbElement = unmarshaller.unmarshal(new StreamSource(resourceAsStream), cls);
            return jaxbElement.getValue();
        } catch (JAXBException e) {
            throw new FalconException("Failed to unmarshal " + template, e);
        } finally {
            IOUtils.closeQuietly(resourceAsStream);
        }
    }

    public static ACTION unmarshalAction(String template) throws FalconException {
        return unmarshal(template, ACTION_JAXB_CONTEXT, ACTION.class);
    }

    // XTODOS Should we make them more specific to feeds??
    public static void addLibExtensionsToWorkflow(Cluster cluster, WORKFLOWAPP wf, Tag tag, EntityType type)
        throws FalconException {
        String libext = ClusterHelper.getLocation(cluster, ClusterLocationType.WORKING).getPath() + "/libext";
        FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(
                ClusterHelper.getConfiguration(cluster));
        try {
            addExtensionJars(fs, new Path(libext), wf);
            addExtensionJars(fs, new Path(libext, type.name()), wf);
            if (tag != null) {
                addExtensionJars(fs, new Path(libext, type.name() + "/" + tag.name().toLowerCase()),
                        wf);
            }
        } catch (IOException e) {
            throw new FalconException(e);
        }
    }

    /**
     *
     * @param path
     * @param name
     * @return
     */
    public static Properties getProperties(Path path, String name) {
        if (path == null) {
            return null;
        }
        Properties prop = new Properties();
        prop.setProperty(ENTITY_PATH, path.toString());
        prop.setProperty(ENTITY_NAME, name);
        return prop;
    }


    /**
     * Adds path(will be the list of directories containing jars to be added as external jars to workflow e.g.
     * for feeds libext, libext/FEED/, libext/FEED/RETENTION, libext/FEED/REPLICATION as an extension jar to the
     * workflow. e.g.
     *
     * @param fs
     * @param path
     * @param wf
     * @throws IOException
     */
    public static void addExtensionJars(FileSystem fs, Path path, WORKFLOWAPP wf) throws IOException {
        FileStatus[] libs;
        try {
            libs = fs.listStatus(path);
        } catch (FileNotFoundException ignore) {
            //Ok if the libext is not configured
            return;
        }

        for (FileStatus lib : libs) {
            if (lib.isDirectory()) {
                continue;
            }

            for (Object obj : wf.getDecisionOrForkOrJoin()) {
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


    public static void decorateWithOozieRetries(ACTION action) {
        Properties props = RuntimeProperties.get();
        action.setRetryMax(props.getProperty("falcon.parentworkflow.retry.max", "3"));
        action.setRetryInterval(props.getProperty("falcon.parentworkflow.retry.interval.secs", "1"));
    }

    // creates the default configuration which is written in config-default.xml
    public static Properties createDefaultConfiguration(Cluster cluster, Entity entity,
                WorkflowExecutionContext.EntityOperations operation)  throws FalconException {
        Properties props = new Properties();
        props.put(WorkflowExecutionArgs.ENTITY_NAME.getName(), entity.getName());
        props.put(WorkflowExecutionArgs.ENTITY_TYPE.getName(), entity.getEntityType().name());
        props.put(WorkflowExecutionArgs.CLUSTER_NAME.getName(), cluster.getName());
        props.put(WorkflowExecutionArgs.DATASOURCE_NAME.getName(), "NA");
        props.put("falconDataOperation", operation.name());

        props.put(WorkflowExecutionArgs.LOG_DIR.getName(),
                getStoragePath(EntityUtil.getLogPath(cluster, entity)));
        props.put(WorkflowExecutionArgs.WF_ENGINE_URL.getName(), ClusterHelper.getOozieUrl(cluster));

        addLateDataProperties(props, entity);
        addBrokerProperties(cluster, props);

        props.put(MR_QUEUE_NAME, "default");
        props.put(MR_JOB_PRIORITY, "NORMAL");

        //properties provided in entity override the default generated properties
        props.putAll(EntityUtil.getEntityProperties(entity));
        props.putAll(createAppProperties(cluster));
        return props;
    }


    // gets the cluster specific properties to be populated in config-default.xml
    private static Properties createAppProperties(Cluster cluster) throws FalconException {
        Properties properties = EntityUtil.getEntityProperties(cluster);
        properties.setProperty(AbstractWorkflowEngine.NAME_NODE, ClusterHelper.getStorageUrl(cluster));
        properties.setProperty(AbstractWorkflowEngine.JOB_TRACKER, ClusterHelper.getMREndPoint(cluster));
        properties.setProperty("colo.name", cluster.getColo());
        final String endpoint = ClusterHelper.getInterface(cluster, Interfacetype.WORKFLOW).getEndpoint();
        if (!OozieConstants.LOCAL_OOZIE.equals(endpoint)) {
            properties.setProperty(OozieClient.USE_SYSTEM_LIBPATH, "true");
        }
        properties.setProperty("falcon.libpath",
                ClusterHelper.getLocation(cluster, ClusterLocationType.WORKING).getPath()  + "/lib");

        return properties;
    }

    // creates hive-site.xml configuration in conf dir for the given cluster on the same cluster.
    public static void createHiveConfiguration(Cluster cluster, Path workflowPath,
                                           String prefix) throws FalconException {
        Configuration hiveConf = getHiveCredentialsAsConf(cluster);

        try {
            Configuration conf = ClusterHelper.getConfiguration(cluster);
            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(conf);

            // create hive conf to stagingDir
            Path confPath = new Path(workflowPath + "/conf");

            persistHiveConfiguration(fs, confPath, hiveConf, prefix);
        } catch (IOException e) {
            throw new FalconException("Unable to create create hive site", e);
        }
    }

    private static void persistHiveConfiguration(FileSystem fs, Path confPath, Configuration hiveConf,
                                          String prefix) throws IOException {
        OutputStream out = null;
        try {
            out = fs.create(new Path(confPath, prefix + "hive-site.xml"));
            hiveConf.writeXml(out);
        } finally {
            IOUtils.closeQuietly(out);
        }
    }

    /**
     * This is only necessary if table is involved and is secure mode.
     *
     * @param workflowApp workflow xml
     * @param cluster     cluster entity
     */
    public static void addHCatalogCredentials(WORKFLOWAPP workflowApp, Cluster cluster, String credentialName) {
        CREDENTIALS credentials = workflowApp.getCredentials();
        if (credentials == null) {
            credentials = new CREDENTIALS();
        }

        credentials.getCredential().add(createHCatalogCredential(cluster, credentialName));

        // add credential for workflow
        workflowApp.setCredentials(credentials);
    }


    /**
     * This is only necessary if table is involved and is secure mode.
     *
     * @param cluster        cluster entity
     * @param credentialName credential name
     * @return CREDENTIALS object
     */
    public static CREDENTIAL createHCatalogCredential(Cluster cluster, String credentialName) {
        final String metaStoreUrl = ClusterHelper.getRegistryEndPoint(cluster);

        CREDENTIAL credential = new CREDENTIAL();
        credential.setName(credentialName);
        credential.setType("hcat");

        credential.getProperty().add(createProperty(HiveUtil.METASTORE_URI, metaStoreUrl));
        credential.getProperty().add(createProperty(SecurityUtil.METASTORE_PRINCIPAL,
                ClusterHelper.getPropertyValue(cluster, SecurityUtil.HIVE_METASTORE_KERBEROS_PRINCIPAL)));

        return credential;
    }

    public static CREDENTIAL.Property createProperty(String name, String value) {
        CREDENTIAL.Property property = new CREDENTIAL.Property();
        property.setName(name);
        property.setValue(value);
        return property;
    }

    private static Properties getHiveCredentials(Cluster cluster) {
        String metaStoreUrl = ClusterHelper.getRegistryEndPoint(cluster);
        if (metaStoreUrl == null) {
            throw new IllegalStateException("Registry interface is not defined in cluster: " + cluster.getName());
        }

        Properties hiveCredentials = new Properties();
        hiveCredentials.put(HiveUtil.METASTOREURIS, metaStoreUrl);
        hiveCredentials.put(HiveUtil.METASTORE_UGI, "true");
        hiveCredentials.put(HiveUtil.NODE, metaStoreUrl.replace("thrift", "hcat"));
        hiveCredentials.put(HiveUtil.METASTORE_URI, metaStoreUrl);

        if (SecurityUtil.isSecurityEnabled()) {
            String principal = ClusterHelper
                    .getPropertyValue(cluster, SecurityUtil.HIVE_METASTORE_KERBEROS_PRINCIPAL);
            hiveCredentials.put(SecurityUtil.METASTORE_PRINCIPAL, principal);
            hiveCredentials.put(SecurityUtil.HIVE_METASTORE_KERBEROS_PRINCIPAL, principal);
            hiveCredentials.put(SecurityUtil.METASTORE_USE_THRIFT_SASL, "true");
        }

        return hiveCredentials;
    }

    private static Configuration getHiveCredentialsAsConf(Cluster cluster) {
        Properties hiveCredentials = getHiveCredentials(cluster);

        Configuration hiveConf = new Configuration(false);
        for (Map.Entry<Object, Object> entry : hiveCredentials.entrySet()) {
            hiveConf.set((String)entry.getKey(), (String)entry.getValue());
        }

        return hiveConf;
    }

    public static Path getBuildPath(Path buildPath, Tag tag) {
        return new Path(buildPath, tag.name());
    }

    protected static String getStoragePath(Path path) {
        if (path != null) {
            return getStoragePath(path.toString());
        }
        return null;
    }

    public static String getStoragePath(String path) {
        if (StringUtils.isNotEmpty(path)) {
            if (new Path(path).toUri().getScheme() == null && !path.startsWith("${nameNode}")) {
                path = "${nameNode}" + path;
            }
        }
        return path;
    }

    // default configuration for coordinator
    public static Properties createCoordDefaultConfiguration(String coordName, Entity entity)
        throws FalconException {

        Properties props = new Properties();
        props.put(WorkflowExecutionArgs.NOMINAL_TIME.getName(), NOMINAL_TIME_EL);
        props.put(WorkflowExecutionArgs.TIMESTAMP.getName(), ACTUAL_TIME_EL);
        props.put(OozieClient.EXTERNAL_ID,
                new ExternalId(entity.getName(), EntityUtil.getWorkflowNameTag(coordName, entity),
                        "${coord:nominalTime()}").getId());
        props.put(WorkflowExecutionArgs.USER_JMS_NOTIFICATION_ENABLED.getName(), "true");
        props.put(WorkflowExecutionArgs.SYSTEM_JMS_NOTIFICATION_ENABLED.getName(),
                RuntimeProperties.get().getProperty("falcon.jms.notification.enabled", "true"));
        //props in entity override the set props.
        props.putAll(EntityUtil.getEntityProperties(entity));
        return props;
    }

    private static void addLateDataProperties(Properties props, Entity entity) throws FalconException {
        if (EntityUtil.getLateProcess(entity) == null
                || EntityUtil.getLateProcess(entity).getLateInputs() == null
                || EntityUtil.getLateProcess(entity).getLateInputs().size() == 0) {
            props.put("shouldRecord", "false");
        } else {
            props.put("shouldRecord", "true");
        }
    }

    private static void addBrokerProperties(Cluster cluster, Properties props) {
        props.put(WorkflowExecutionArgs.USER_BRKR_URL.getName(),
                ClusterHelper.getMessageBrokerUrl(cluster));
        props.put(WorkflowExecutionArgs.USER_BRKR_IMPL_CLASS.getName(),
                ClusterHelper.getMessageBrokerImplClass(cluster));

        String falconBrokerUrl = StartupProperties.get().getProperty(
                "broker.url", "tcp://localhost:61616?daemon=true");
        props.put(WorkflowExecutionArgs.BRKR_URL.getName(), falconBrokerUrl);

        String falconBrokerImplClass = StartupProperties.get().getProperty(
                "broker.impl.class", ClusterHelper.DEFAULT_BROKER_IMPL_CLASS);
        props.put(WorkflowExecutionArgs.BRKR_IMPL_CLASS.getName(), falconBrokerImplClass);

        String jmsMessageTTL = StartupProperties.get().getProperty("broker.ttlInMins",
                DEFAULT_BROKER_MSG_TTL.toString());
        props.put(WorkflowExecutionArgs.BRKR_TTL.getName(), jmsMessageTTL);
    }


    private static org.apache.falcon.oozie.workflow.CONFIGURATION getWorkflowConfig(Properties props) {
        org.apache.falcon.oozie.workflow.CONFIGURATION conf = new org.apache.falcon.oozie.workflow.CONFIGURATION();
        for (Map.Entry<Object, Object> prop : props.entrySet()) {
            org.apache.falcon.oozie.workflow.CONFIGURATION.Property confProp =
                    new org.apache.falcon.oozie.workflow.CONFIGURATION.Property();
            confProp.setName((String) prop.getKey());
            confProp.setValue((String) prop.getValue());
            conf.getProperty().add(confProp);
        }
        return conf;
    }

    public static CONFIGURATION getCoordinatorConfig(Properties props) {
        CONFIGURATION conf = new CONFIGURATION();
        for (Map.Entry<Object, Object> prop : props.entrySet()) {
            CONFIGURATION.Property confProp = new CONFIGURATION.Property();
            confProp.setName((String) prop.getKey());
            confProp.setValue((String) prop.getValue());
            conf.getProperty().add(confProp);
        }
        return conf;
    }
}
