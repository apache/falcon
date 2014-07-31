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
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.oozie.feed.FSReplicationWorkflowBuilder;
import org.apache.falcon.oozie.feed.FeedRetentionWorkflowBuilder;
import org.apache.falcon.oozie.feed.HCatReplicationWorkflowBuilder;
import org.apache.falcon.oozie.process.HiveProcessWorkflowBuilder;
import org.apache.falcon.oozie.process.OozieProcessWorkflowBuilder;
import org.apache.falcon.oozie.process.PigProcessWorkflowBuilder;
import org.apache.falcon.oozie.workflow.ACTION;
import org.apache.falcon.oozie.workflow.CREDENTIAL;
import org.apache.falcon.oozie.workflow.CREDENTIALS;
import org.apache.falcon.oozie.workflow.END;
import org.apache.falcon.oozie.workflow.KILL;
import org.apache.falcon.oozie.workflow.START;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.falcon.security.SecurityUtil;
import org.apache.falcon.util.OozieUtils;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
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

    protected static final String USER_ACTION_NAME = "user-action";
    protected static final String PREPROCESS_ACTION_NAME = "pre-processing";
    protected static final String SUCCESS_POSTPROCESS_ACTION_NAME = "succeeded-post-processing";
    protected static final String FAIL_POSTPROCESS_ACTION_NAME = "failed-post-processing";
    protected static final String OK_ACTION_NAME = "end";
    protected static final String FAIL_ACTION_NAME = "fail";


    private static final String POSTPROCESS_TEMPLATE = "/action/post-process.xml";
    private static final String PREPROCESS_TEMPLATE = "/action/pre-process.xml";

    public static final Set<String> FALCON_ACTIONS = new HashSet<String>(
        Arrays.asList(new String[]{PREPROCESS_ACTION_NAME, SUCCESS_POSTPROCESS_ACTION_NAME,
            FAIL_POSTPROCESS_ACTION_NAME, }));

    private final Tag lifecycle;

    public OozieOrchestrationWorkflowBuilder(T entity, Tag lifecycle) {
        super(entity);
        this.lifecycle = lifecycle;
    }

    public static final OozieOrchestrationWorkflowBuilder get(Entity entity, Cluster cluster, Tag lifecycle)
        throws FalconException {
        switch(entity.getEntityType()) {
        case FEED:
            Feed feed = (Feed) entity;
            switch (lifecycle) {
            case RETENTION:
                return new FeedRetentionWorkflowBuilder(feed);

            case REPLICATION:
                boolean isTable = EntityUtil.isTableStorageType(cluster, feed);
                if (isTable) {
                    return new HCatReplicationWorkflowBuilder(feed);
                } else {
                    return new FSReplicationWorkflowBuilder(feed);
                }

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

    protected void addTransition(ACTION action, String ok, String fail) {
        action.getOk().setTo(ok);
        action.getError().setTo(fail);
    }

    protected void decorateWorkflow(WORKFLOWAPP wf, String name, String startAction) {
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

    protected ACTION getSuccessPostProcessAction() throws FalconException {
        ACTION action = unmarshalAction(POSTPROCESS_TEMPLATE);
        decorateWithOozieRetries(action);
        return action;
    }

    protected ACTION getFailPostProcessAction() throws FalconException {
        ACTION action = unmarshalAction(POSTPROCESS_TEMPLATE);
        decorateWithOozieRetries(action);
        action.setName(FAIL_POSTPROCESS_ACTION_NAME);
        return action;
    }

    protected ACTION getPreProcessingAction(boolean isTableStorageType, Tag tag) throws FalconException {
        ACTION action = unmarshalAction(PREPROCESS_TEMPLATE);
        decorateWithOozieRetries(action);
        if (isTableStorageType) {
            // adds hive-site.xml in actions classpath
            action.getJava().setJobXml("${wf:appPath()}/conf/hive-site.xml");
        }

        List<String> args = action.getJava().getArg();
        args.add("-out");
        if (tag == Tag.REPLICATION) {
            args.add("${logDir}/latedata/${nominalTime}/${srcClusterName}");
        } else {
            args.add("${logDir}/latedata/${nominalTime}");
        }
        return action;
    }

    protected Path marshal(Cluster cluster, WORKFLOWAPP workflow, Path outPath) throws FalconException {
        return marshal(cluster, new org.apache.falcon.oozie.workflow.ObjectFactory().createWorkflowApp(workflow),
            OozieUtils.WORKFLOW_JAXB_CONTEXT, new Path(outPath, "workflow.xml"));
    }

    protected WORKFLOWAPP unmarshal(String template) throws FalconException {
        return unmarshal(template, OozieUtils.WORKFLOW_JAXB_CONTEXT, WORKFLOWAPP.class);
    }

    protected ACTION unmarshalAction(String template) throws FalconException {
        return unmarshal(template, OozieUtils.ACTION_JAXB_CONTEXT, ACTION.class);
    }

    protected boolean shouldPreProcess() throws FalconException {
        if (EntityUtil.getLateProcess(entity) == null
            || EntityUtil.getLateProcess(entity).getLateInputs() == null
            || EntityUtil.getLateProcess(entity).getLateInputs().size() == 0) {
            return false;
        }
        return true;
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

    protected void decorateWithOozieRetries(ACTION action) {
        Properties props = RuntimeProperties.get();
        action.setRetryMax(props.getProperty("falcon.parentworkflow.retry.max", "3"));
        action.setRetryInterval(props.getProperty("falcon.parentworkflow.retry.interval.secs", "1"));
    }
}
