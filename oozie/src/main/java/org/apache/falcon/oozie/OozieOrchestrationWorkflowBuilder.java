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

package org.apache.falcon.oozie;

import org.apache.commons.io.IOUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.oozie.feed.FeedReplicationWorkflowBuilder;
import org.apache.falcon.oozie.feed.FeedRetentionWorkflowBuilder;
import org.apache.falcon.oozie.process.HiveProcessWorkflowBuilder;
import org.apache.falcon.oozie.process.OozieProcessWorkflowBuilder;
import org.apache.falcon.oozie.process.PigProcessWorkflowBuilder;
import org.apache.falcon.oozie.workflow.ACTION;
import org.apache.falcon.oozie.workflow.CREDENTIAL;
import org.apache.falcon.oozie.workflow.CREDENTIALS;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.falcon.security.SecurityUtil;
import org.apache.falcon.util.OozieUtils;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Base class for building orchestration workflow in oozie.
 * @param <T>
 */
public abstract class OozieOrchestrationWorkflowBuilder<T extends Entity> extends OozieEntityBuilder<T> {
    protected static final String HIVE_CREDENTIAL_NAME = "falconHiveAuth";
    public static final Set<String> FALCON_ACTIONS = new HashSet<String>(
        Arrays.asList(new String[]{"recordsize", "succeeded-post-processing", "failed-post-processing", }));
    private final Tag lifecycle;

    public OozieOrchestrationWorkflowBuilder(T entity, Tag lifecycle) {
        super(entity);
        this.lifecycle = lifecycle;
    }

    public static final OozieOrchestrationWorkflowBuilder get(Entity entity, Tag lifecycle) {
        switch(entity.getEntityType()) {
        case FEED:
            Feed feed = (Feed) entity;
            switch (lifecycle) {
            case RETENTION:
                return new FeedRetentionWorkflowBuilder(feed);

            case REPLICATION:
                return new FeedReplicationWorkflowBuilder(feed);

            default:
                throw new IllegalArgumentException("Unhandled type " + entity.getEntityType() + ", lifecycle "
                    + lifecycle);
            }

        case PROCESS:
            Process process = (Process) entity;
            switch(process.getWorkflow().getEngine()) {
            case PIG:
                return new PigProcessWorkflowBuilder(process);

            case OOZIE:
                return new OozieProcessWorkflowBuilder(process);

            case HIVE:
                return new HiveProcessWorkflowBuilder(process);

            default:
                break;
            }

        default:
        }

        throw new IllegalArgumentException("Unhandled type " + entity.getEntityType() + ", lifecycle " + lifecycle);
    }

    protected void marshal(Cluster cluster, WORKFLOWAPP workflow, Path outPath) throws FalconException {
        marshal(cluster, new org.apache.falcon.oozie.workflow.ObjectFactory().createWorkflowApp(workflow),
            OozieUtils.WORKFLOW_JAXB_CONTEXT, new Path(outPath, "workflow.xml"));
    }

    protected WORKFLOWAPP getWorkflow(String template) throws FalconException {
        InputStream resourceAsStream = null;
        try {
            resourceAsStream = OozieOrchestrationWorkflowBuilder.class.getResourceAsStream(template);
            Unmarshaller unmarshaller = OozieUtils.WORKFLOW_JAXB_CONTEXT.createUnmarshaller();
            @SuppressWarnings("unchecked")
            JAXBElement<WORKFLOWAPP> jaxbElement = (JAXBElement<WORKFLOWAPP>) unmarshaller.unmarshal(resourceAsStream);
            return jaxbElement.getValue();
        } catch (JAXBException e) {
            throw new FalconException(e);
        } finally {
            IOUtils.closeQuietly(resourceAsStream);
        }
    }

    protected void addLibExtensionsToWorkflow(Cluster cluster, WORKFLOWAPP wf, Tag tag)
        throws FalconException {
        String libext = ClusterHelper.getLocation(cluster, "working") + "/libext";
        FileSystem fs = HadoopClientFactory.get().createFileSystem(ClusterHelper.getConfiguration(cluster));
        try {
            addExtensionJars(fs, new Path(libext), wf);
            addExtensionJars(fs, new Path(libext, entity.getEntityType().name()), wf);
            if (tag != null) {
                addExtensionJars(fs,
                    new Path(libext, entity.getEntityType().name() + "/" + tag.name().toLowerCase()), wf);
            }
        } catch(IOException e) {
            throw new FalconException(e);
        }
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

    // creates hive-site.xml configuration in conf dir for the given cluster on the same cluster.
    protected void createHiveConfiguration(Cluster cluster, Path workflowPath, String prefix) throws FalconException {
        Configuration hiveConf = getHiveCredentialsAsConf(cluster);

        try {
            Configuration conf = ClusterHelper.getConfiguration(cluster);
            FileSystem fs = HadoopClientFactory.get().createFileSystem(conf);

            // create hive conf to stagingDir
            Path confPath = new Path(workflowPath + "/conf");

            persistHiveConfiguration(fs, confPath, hiveConf, prefix);
        } catch (IOException e) {
            throw new FalconException("Unable to create create hive site", e);
        }
    }

    private void persistHiveConfiguration(FileSystem fs, Path confPath, Configuration hiveConf,
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
    protected void addHCatalogCredentials(WORKFLOWAPP workflowApp, Cluster cluster, String credentialName) {
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
     * @param workflowApp workflow xml
     * @param cluster     cluster entity
     */
    protected void addHCatalogCredentials(WORKFLOWAPP workflowApp, Cluster cluster,
        String credentialName, Set<String> actions) {
        addHCatalogCredentials(workflowApp, cluster, credentialName);

        // add credential to each action
        for (Object object : workflowApp.getDecisionOrForkOrJoin()) {
            if (!(object instanceof ACTION)) {
                continue;
            }

            ACTION action = (ACTION) object;
            String actionName = action.getName();
            if (actions.contains(actionName)) {
                action.setCred(credentialName);
            }
        }
    }

    /**
     * This is only necessary if table is involved and is secure mode.
     *
     * @param cluster        cluster entity
     * @param credentialName credential name
     * @return CREDENTIALS object
     */
    private CREDENTIAL createHCatalogCredential(Cluster cluster, String credentialName) {
        final String metaStoreUrl = ClusterHelper.getRegistryEndPoint(cluster);

        CREDENTIAL credential = new CREDENTIAL();
        credential.setName(credentialName);
        credential.setType("hcat");

        credential.getProperty().add(createProperty("hcat.metastore.uri", metaStoreUrl));
        credential.getProperty().add(createProperty("hcat.metastore.principal",
            ClusterHelper.getPropertyValue(cluster, SecurityUtil.HIVE_METASTORE_PRINCIPAL)));

        return credential;
    }

    private CREDENTIAL.Property createProperty(String name, String value) {
        CREDENTIAL.Property property = new CREDENTIAL.Property();
        property.setName(name);
        property.setValue(value);
        return property;
    }

    protected void addOozieRetries(WORKFLOWAPP workflow) {
        for (Object object : workflow.getDecisionOrForkOrJoin()) {
            if (!(object instanceof org.apache.falcon.oozie.workflow.ACTION)) {
                continue;
            }
            org.apache.falcon.oozie.workflow.ACTION action = (org.apache.falcon.oozie.workflow.ACTION) object;
            String actionName = action.getName();
            if (FALCON_ACTIONS.contains(actionName)) {
                decorateWithOozieRetries(action);
            }
        }
    }

    protected void decorateWithOozieRetries(ACTION action) {
        Properties props = RuntimeProperties.get();
        action.setRetryMax(props.getProperty("falcon.parentworkflow.retry.max", "3"));
        action.setRetryInterval(props.getProperty("falcon.parentworkflow.retry.interval.secs", "1"));
    }
}
