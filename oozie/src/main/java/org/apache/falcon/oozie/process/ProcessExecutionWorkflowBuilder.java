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

package org.apache.falcon.oozie.process;

import org.apache.falcon.FalconException;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.CatalogStorage;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.oozie.OozieOrchestrationWorkflowBuilder;
import org.apache.falcon.oozie.workflow.ACTION;
import org.apache.falcon.oozie.workflow.CONFIGURATION;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

/**
 * Base class for building orchestration workflow for process.
 */
public abstract class ProcessExecutionWorkflowBuilder extends OozieOrchestrationWorkflowBuilder<Process> {
    private static final String DEFAULT_WF_TEMPLATE = "/workflow/process-parent-workflow.xml";
    private static final Set<String> FALCON_PROCESS_HIVE_ACTIONS = new HashSet<String>(
        Arrays.asList(new String[]{"recordsize", "user-oozie-workflow", "user-pig-job", "user-hive-job", }));

    protected ProcessExecutionWorkflowBuilder(Process entity) {
        super(entity, Tag.DEFAULT);
    }

    @Override public Properties build(Cluster cluster, Path buildPath) throws FalconException {
        WORKFLOWAPP wfApp = unmarshal(DEFAULT_WF_TEMPLATE);
        String wfName = EntityUtil.getWorkflowName(Tag.DEFAULT, entity).toString();
        wfApp.setName(wfName);

        addLibExtensionsToWorkflow(cluster, wfApp, null);

        final boolean isTableStorageType = isTableStorageType(cluster);
        if (isTableStorageType) {
            setupHiveCredentials(cluster, buildPath, wfApp);
        }

        for (Object object : wfApp.getDecisionOrForkOrJoin()) {
            if (!(object instanceof ACTION)) {
                continue;
            }

            ACTION action = (ACTION) object;
            String actionName = action.getName();
            if (FALCON_ACTIONS.contains(actionName)) {
                decorateWithOozieRetries(action);
                if (isTableStorageType && actionName.equals("recordsize")) {
                    // adds hive-site.xml in actions classpath
                    action.getJava().setJobXml("${wf:appPath()}/conf/hive-site.xml");
                }
            }

            decorateAction(action, cluster, buildPath);
        }

        //Create parent workflow
        Path marshalPath = marshal(cluster, wfApp, buildPath);
        return getProperties(marshalPath, wfName);
    }

    protected abstract void decorateAction(ACTION action, Cluster cluster, Path buildPath) throws FalconException;

    private void setupHiveCredentials(Cluster cluster, Path buildPath, WORKFLOWAPP wfApp) throws FalconException {
        // create hive-site.xml file so actions can use it in the classpath
        createHiveConfiguration(cluster, buildPath, ""); // DO NOT ADD PREFIX!!!

        if (isSecurityEnabled) {
            // add hcatalog credentials for secure mode and add a reference to each action
            addHCatalogCredentials(wfApp, cluster, HIVE_CREDENTIAL_NAME, FALCON_PROCESS_HIVE_ACTIONS);
        }
    }

    protected void addInputFeedsAsParams(List<String> paramList, Cluster cluster) throws FalconException {
        if (entity.getInputs() == null) {
            return;
        }

        for (Input input : entity.getInputs().getInputs()) {
            Feed feed = EntityUtil.getEntity(EntityType.FEED, input.getFeed());
            Storage storage = FeedHelper.createStorage(cluster, feed);

            final String inputName = input.getName();
            if (storage.getType() == Storage.TYPE.FILESYSTEM) {
                paramList.add(inputName + "=${" + inputName + "}"); // no prefix for backwards compatibility
            } else if (storage.getType() == Storage.TYPE.TABLE) {
                final String paramName = "falcon_" + inputName; // prefix 'falcon' for new params
                Properties props = new Properties();
                propagateCommonCatalogTableProperties((CatalogStorage) storage, props, paramName);
                for (Object key : props.keySet()) {
                    paramList.add(key + "=${wf:conf('" + key + "')}");
                }

                paramList.add(paramName + "_filter=${wf:conf('"
                    + paramName + "_partition_filter_" + entity.getWorkflow().getEngine().name().toLowerCase() + "')}");
            }
        }
    }

    protected void addOutputFeedsAsParams(List<String> paramList, Cluster cluster) throws FalconException {
        if (entity.getOutputs() == null) {
            return;
        }

        for (Output output : entity.getOutputs().getOutputs()) {
            Feed feed = EntityUtil.getEntity(EntityType.FEED, output.getFeed());
            Storage storage = FeedHelper.createStorage(cluster, feed);

            if (storage.getType() == Storage.TYPE.FILESYSTEM) {
                final String outputName = output.getName();  // no prefix for backwards compatibility
                paramList.add(outputName + "=${" + outputName + "}");
            } else if (storage.getType() == Storage.TYPE.TABLE) {
                Properties props = new Properties();
                propagateCatalogTableProperties(output, (CatalogStorage) storage, props); // prefix is auto added
                for (Object key : props.keySet()) {
                    paramList.add(key + "=${wf:conf('" + key + "')}");
                }

                final String paramName = "falcon_" + output.getName(); // prefix 'falcon' for new params
                paramList.add(paramName + "_partitions=${wf:conf('"
                    + paramName + "_partitions_" + entity.getWorkflow().getEngine().name().toLowerCase() + "')}");
            }
        }
    }

    protected void propagateEntityProperties(CONFIGURATION conf, List<String> paramList) {
        Properties entityProperties = getEntityProperties(entity);

        // Propagate user defined properties to job configuration
        final List<org.apache.falcon.oozie.workflow.CONFIGURATION.Property> configuration = conf.getProperty();

        // Propagate user defined properties to pig script as macros
        // passed as parameters -p name=value that can be accessed as $name
        for (Entry<Object, Object> entry: entityProperties.entrySet()) {
            org.apache.falcon.oozie.workflow.CONFIGURATION.Property configProperty =
                new org.apache.falcon.oozie.workflow.CONFIGURATION.Property();
            configProperty.setName((String) entry.getKey());
            configProperty.setValue((String) entry.getValue());
            configuration.add(configProperty);

            paramList.add(entry.getKey() + "=" + entry.getValue());
        }
    }

    protected List<String> getPrepareDeleteOutputPathList() throws FalconException {
        final List<String> deleteList = new ArrayList<String>();
        if (entity.getOutputs() == null) {
            return deleteList;
        }

        for (Output output : entity.getOutputs().getOutputs()) {
            Feed feed = EntityUtil.getEntity(EntityType.FEED, output.getFeed());

            if (FeedHelper.getStorageType(feed) == Storage.TYPE.TABLE) {
                continue; // prepare delete only applies to FileSystem storage
            }

            deleteList.add("${wf:conf('" + output.getName() + "')}");
        }

        return deleteList;
    }

    protected void addArchiveForCustomJars(Cluster cluster, List<String> archiveList,
        Path libPath) throws FalconException {
        if (libPath == null) {
            return;
        }

        try {
            final FileSystem fs = libPath.getFileSystem(ClusterHelper.getConfiguration(cluster));
            if (fs.isFile(libPath)) {  // File, not a Dir
                archiveList.add(libPath.toString());
                return;
            }

            // lib path is a directory, add each file under the lib dir to archive
            final FileStatus[] fileStatuses = fs.listStatus(libPath, new PathFilter() {
                @Override
                public boolean accept(Path path) {
                    try {
                        return fs.isFile(path) && path.getName().endsWith(".jar");
                    } catch (IOException ignore) {
                        return false;
                    }
                }
            });

            for (FileStatus fileStatus : fileStatuses) {
                archiveList.add(fileStatus.getPath().toString());
            }
        } catch (IOException e) {
            throw new FalconException("Error adding archive for custom jars under: " + libPath, e);
        }
    }
}
