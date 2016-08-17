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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.CatalogStorage;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.ProcessHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Workflow;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.oozie.OozieOrchestrationWorkflowBuilder;
import org.apache.falcon.oozie.workflow.ACTION;
import org.apache.falcon.oozie.workflow.CONFIGURATION;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.oozie.client.OozieClient;

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

    private static final Set<String> FALCON_PROCESS_HIVE_ACTIONS = new HashSet<String>(
        Arrays.asList(new String[]{PREPROCESS_ACTION_NAME, USER_ACTION_NAME, }));

    protected ProcessExecutionWorkflowBuilder(Process entity) {
        super(entity, LifeCycle.EXECUTION);
    }

    @Override public Properties build(Cluster cluster, Path buildPath) throws FalconException {
        WORKFLOWAPP wfApp = new WORKFLOWAPP();
        String wfName = EntityUtil.getWorkflowName(Tag.DEFAULT, entity).toString();

        String startAction = USER_ACTION_NAME;
        final boolean isTableStorageType = EntityUtil.isTableStorageType(cluster, entity);

        //Add pre-processing action
        if (shouldPreProcess()) {
            ACTION preProcessAction = getPreProcessingAction(isTableStorageType, Tag.DEFAULT);
            addTransition(preProcessAction, USER_ACTION_NAME, FAIL_POSTPROCESS_ACTION_NAME);
            wfApp.getDecisionOrForkOrJoin().add(preProcessAction);
            startAction = PREPROCESS_ACTION_NAME;
        }
        //Add user action
        ACTION userAction = getUserAction(cluster, buildPath);

        addPostProcessing(wfApp, userAction);

        decorateWorkflow(wfApp, wfName, startAction);

        addLibExtensionsToWorkflow(cluster, wfApp, null);

        if (isTableStorageType) {
            setupHiveCredentials(cluster, buildPath, wfApp);
        }

        marshal(cluster, wfApp, buildPath);
        Properties props =  createDefaultConfiguration(cluster);
        props.putAll(getProperties(buildPath, wfName));
        props.putAll(getWorkflowProperties());
        props.setProperty(OozieClient.APP_PATH, buildPath.toString());

        //Add libpath
        Path libPath = new Path(buildPath, "lib");
        copySharedLibs(cluster, libPath);
        props.put(OozieClient.LIBPATH, libPath.toString());

        Workflow processWorkflow = ((Process)(entity)).getWorkflow();
        propagateUserWorkflowProperties(processWorkflow, props);

        // Write out the config to config-default.xml
        marshal(cluster, wfApp, getConfig(props), buildPath);

        return props;
    }

    private Properties getWorkflowProperties() {
        Properties props = new Properties();
        props.setProperty("srcClusterName", "NA");
        props.setProperty("availabilityFlag", "NA");
        props.setProperty(WorkflowExecutionArgs.DATASOURCE_NAME.getName(), "NA");
        return props;
    }

    protected abstract ACTION getUserAction(Cluster cluster, Path buildPath) throws FalconException;

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
        Properties entityProperties = EntityUtil.getEntityProperties(entity);

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

            if (paramList != null) {
                paramList.add(entry.getKey() + "=" + entry.getValue());
            }
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

    protected void addArchiveForCustomJars(Cluster cluster, List<String> archiveList, String lib)
        throws FalconException {
        if (StringUtils.isBlank(lib)) {
            return;
        }

        String[] libPaths = lib.split(EntityUtil.WF_LIB_SEPARATOR);
        for (String path : libPaths) {
            Path libPath = new Path(path);
            try {
                final FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(
                        ClusterHelper.getConfiguration(cluster));
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

    private void propagateUserWorkflowProperties(Workflow processWorkflow, Properties props) {
        props.put("userWorkflowName", ProcessHelper.getProcessWorkflowName(
                processWorkflow.getName(), entity.getName()));
        props.put("userWorkflowVersion", processWorkflow.getVersion());
        props.put("userWorkflowEngine", processWorkflow.getEngine().value());
    }

    @Override
    protected WorkflowExecutionContext.EntityOperations getOperation() {
        return WorkflowExecutionContext.EntityOperations.GENERATE;
    }
}
