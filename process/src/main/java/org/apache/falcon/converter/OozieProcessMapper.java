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
import org.apache.falcon.Tag;
import org.apache.falcon.entity.CatalogStorage;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.ProcessHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.process.EngineType;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.LateInput;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Property;
import org.apache.falcon.entity.v0.process.Workflow;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.messaging.EntityInstanceMessage.ARG;
import org.apache.falcon.oozie.coordinator.CONTROLS;
import org.apache.falcon.oozie.coordinator.COORDINATORAPP;
import org.apache.falcon.oozie.coordinator.DATAIN;
import org.apache.falcon.oozie.coordinator.DATAOUT;
import org.apache.falcon.oozie.coordinator.DATASETS;
import org.apache.falcon.oozie.coordinator.INPUTEVENTS;
import org.apache.falcon.oozie.coordinator.OUTPUTEVENTS;
import org.apache.falcon.oozie.coordinator.SYNCDATASET;
import org.apache.falcon.oozie.coordinator.WORKFLOW;
import org.apache.falcon.oozie.workflow.ACTION;
import org.apache.falcon.oozie.workflow.DELETE;
import org.apache.falcon.oozie.workflow.PIG;
import org.apache.falcon.oozie.workflow.PREPARE;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This class maps the Falcon entities into Oozie artifacts.
 */
public class OozieProcessMapper extends AbstractOozieEntityMapper<Process> {

    private static final String DEFAULT_WF_TEMPLATE = "/config/workflow/process-parent-workflow.xml";
    private static final int THIRTY_MINUTES = 30 * 60 * 1000;

    public OozieProcessMapper(Process entity) {
        super(entity);
    }

    @Override
    protected List<COORDINATORAPP> getCoordinators(Cluster cluster, Path bundlePath) throws FalconException {
        List<COORDINATORAPP> apps = new ArrayList<COORDINATORAPP>();
        apps.add(createDefaultCoordinator(cluster, bundlePath));

        return apps;
    }

    /**
     * Creates default oozie coordinator.
     *
     * @param cluster    - Cluster for which the coordiantor app need to be created
     * @param bundlePath - bundle path
     * @return COORDINATORAPP
     * @throws FalconException on Error
     */
    public COORDINATORAPP createDefaultCoordinator(Cluster cluster, Path bundlePath) throws FalconException {
        Process process = getEntity();
        if (process == null) {
            return null;
        }

        COORDINATORAPP coord = new COORDINATORAPP();
        String coordName = EntityUtil.getWorkflowName(Tag.DEFAULT, process).toString();
        Path coordPath = getCoordPath(bundlePath, coordName);

        // coord attributes
        initializeCoordAttributes(cluster, process, coord, coordName);

        CONTROLS controls = initializeControls(process); // controls
        coord.setControls(controls);

        // Configuration
        Map<String, String> props = createCoordDefaultConfiguration(cluster, coordPath, coordName);

        initializeInputPaths(cluster, process, coord, props); // inputs
        initializeOutputPaths(cluster, process, coord, props);  // outputs
        propagateStorageType(process, props);  // falconFeedStorageType

        Workflow processWorkflow = process.getWorkflow();
        props.put("userWorkflowEngine", processWorkflow.getEngine().value());

        // create parent wf
        createWorkflow(cluster, process, processWorkflow, coordName, coordPath);

        WORKFLOW wf = new WORKFLOW();
        wf.setAppPath(getStoragePath(coordPath.toString()));
        wf.setConfiguration(getCoordConfig(props));

        // set coord action to parent wf
        org.apache.falcon.oozie.coordinator.ACTION action = new org.apache.falcon.oozie.coordinator.ACTION();
        action.setWorkflow(wf);
        coord.setAction(action);

        return coord;
    }

    private void initializeCoordAttributes(Cluster cluster, Process process, COORDINATORAPP coord, String coordName) {
        coord.setName(coordName);
        org.apache.falcon.entity.v0.process.Cluster processCluster =
                ProcessHelper.getCluster(process, cluster.getName());
        coord.setStart(SchemaHelper.formatDateUTC(processCluster.getValidity().getStart()));
        coord.setEnd(SchemaHelper.formatDateUTC(processCluster.getValidity().getEnd()));
        coord.setTimezone(process.getTimezone().getID());
        coord.setFrequency("${coord:" + process.getFrequency().toString() + "}");
    }

    private CONTROLS initializeControls(Process process)
        throws FalconException {
        CONTROLS controls = new CONTROLS();
        controls.setConcurrency(String.valueOf(process.getParallel()));
        controls.setExecution(process.getOrder().name());

        Frequency timeout = process.getTimeout();
        long frequencyInMillis = ExpressionHelper.get().evaluate(process.getFrequency().toString(), Long.class);
        long timeoutInMillis;
        if (timeout != null) {
            timeoutInMillis = ExpressionHelper.get().
                    evaluate(process.getTimeout().toString(), Long.class);
        } else {
            timeoutInMillis = frequencyInMillis * 6;
            if (timeoutInMillis < THIRTY_MINUTES) {
                timeoutInMillis = THIRTY_MINUTES;
            }
        }
        controls.setTimeout(String.valueOf(timeoutInMillis / (1000 * 60)));

        if (timeoutInMillis / frequencyInMillis * 2 > 0) {
            controls.setThrottle(String.valueOf(timeoutInMillis / frequencyInMillis * 2));
        }

        return controls;
    }

    private void initializeInputPaths(Cluster cluster, Process process, COORDINATORAPP coord,
                                      Map<String, String> props) throws FalconException {
        if (process.getInputs() == null) {
            return;
        }

        List<String> inputFeeds = new ArrayList<String>();
        List<String> inputPaths = new ArrayList<String>();
        for (Input input : process.getInputs().getInputs()) {
            Feed feed = EntityUtil.getEntity(EntityType.FEED, input.getFeed());
            Storage storage = FeedHelper.createStorage(cluster, feed);

            if (!input.isOptional()) {
                if (coord.getDatasets() == null) {
                    coord.setDatasets(new DATASETS());
                }
                if (coord.getInputEvents() == null) {
                    coord.setInputEvents(new INPUTEVENTS());
                }

                SYNCDATASET syncdataset = createDataSet(feed, cluster, storage, input.getName(), LocationType.DATA);
                coord.getDatasets().getDatasetOrAsyncDataset().add(syncdataset);

                DATAIN datain = createDataIn(input);
                coord.getInputEvents().getDataIn().add(datain);
            }

            String inputExpr = null;
            if (storage.getType() == Storage.TYPE.FILESYSTEM) {
                inputExpr = getELExpression("dataIn('" + input.getName() + "', '" + input.getPartition() + "')");
                props.put(input.getName(), inputExpr);
            } else if (storage.getType() == Storage.TYPE.TABLE) {
                inputExpr = "${coord:dataIn('" + input.getName() + "')}";
                propagateCatalogTableProperties(input, (CatalogStorage) storage, props);
            }

            inputFeeds.add(input.getName());
            inputPaths.add(inputExpr);
        }

        // populate late data handler - should-record action
        props.put("falconInputFeeds", join(inputFeeds.iterator(), '#'));
        props.put("falconInPaths", join(inputPaths.iterator(), '#'));
    }

    private void initializeOutputPaths(Cluster cluster, Process process, COORDINATORAPP coord,
                                       Map<String, String> props) throws FalconException {
        if (process.getOutputs() == null) {
            return;
        }

        if (coord.getDatasets() == null) {
            coord.setDatasets(new DATASETS());
        }

        if (coord.getOutputEvents() == null) {
            coord.setOutputEvents(new OUTPUTEVENTS());
        }

        List<String> outputFeeds = new ArrayList<String>();
        List<String> outputPaths = new ArrayList<String>();
        for (Output output : process.getOutputs().getOutputs()) {
            Feed feed = EntityUtil.getEntity(EntityType.FEED, output.getFeed());
            Storage storage = FeedHelper.createStorage(cluster, feed);

            SYNCDATASET syncdataset = createDataSet(feed, cluster, storage, output.getName(), LocationType.DATA);
            coord.getDatasets().getDatasetOrAsyncDataset().add(syncdataset);

            DATAOUT dataout = createDataOut(output);
            coord.getOutputEvents().getDataOut().add(dataout);

            String outputExpr = "${coord:dataOut('" + output.getName() + "')}";
            outputFeeds.add(output.getName());
            outputPaths.add(outputExpr);

            if (storage.getType() == Storage.TYPE.FILESYSTEM) {
                props.put(output.getName(), outputExpr);

                propagateFileSystemProperties(output, feed, cluster, coord, storage, props);
            } else if (storage.getType() == Storage.TYPE.TABLE) {
                propagateCatalogTableProperties(output, (CatalogStorage) storage, props);
            }
        }

        // Output feed name and path for parent workflow
        props.put(ARG.feedNames.getPropName(), join(outputFeeds.iterator(), ','));
        props.put(ARG.feedInstancePaths.getPropName(), join(outputPaths.iterator(), ','));
    }

    private void propagateStorageType(Process process, Map<String, String> props) throws FalconException {
        Storage.TYPE feedStorageType = Storage.TYPE.FILESYSTEM; // defaults to FS.

        if (process.getLateProcess() != null) {
            Map<String, String> feeds = new HashMap<String, String>();
            if (process.getInputs() != null) {
                for (Input in : process.getInputs().getInputs()) {
                    feeds.put(in.getName(), in.getFeed());
                }
            }

            for (LateInput lp : process.getLateProcess().getLateInputs()) {
                Feed feed = ConfigurationStore.get().get(EntityType.FEED, feeds.get(lp.getInput()));
                if (FeedHelper.getStorageType(feed) == Storage.TYPE.TABLE) {
                    feedStorageType = Storage.TYPE.TABLE;
                    break;  // break if one of 'em is a table as late data wont apply
                }
            }
        }

        // this is currently only used for late data handling.
        props.put("falconFeedStorageType", feedStorageType.name());
    }

    private SYNCDATASET createDataSet(Feed feed, Cluster cluster, Storage storage,
                                      String datasetName, LocationType locationType) throws FalconException {

        SYNCDATASET syncdataset = new SYNCDATASET();
        syncdataset.setName(datasetName);
        syncdataset.setFrequency("${coord:" + feed.getFrequency().toString() + "}");

        String uriTemplate = storage.getUriTemplate(locationType);
        if (storage.getType() == Storage.TYPE.TABLE) {
            uriTemplate = uriTemplate.replace("thrift", "hcat"); // Oozie requires this!!!
        }
        syncdataset.setUriTemplate(uriTemplate);

        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(feed, cluster.getName());
        syncdataset.setInitialInstance(SchemaHelper.formatDateUTC(feedCluster.getValidity().getStart()));
        syncdataset.setTimezone(feed.getTimezone().getID());

        if (feed.getAvailabilityFlag() == null) {
            syncdataset.setDoneFlag("");
        } else {
            syncdataset.setDoneFlag(feed.getAvailabilityFlag());
        }

        return syncdataset;
    }

    private DATAOUT createDataOut(Output output) {
        DATAOUT dataout = new DATAOUT();
        dataout.setName(output.getName());
        dataout.setDataset(output.getName());
        dataout.setInstance(getELExpression(output.getInstance()));
        return dataout;
    }

    private DATAIN createDataIn(Input input) {
        DATAIN datain = new DATAIN();
        datain.setName(input.getName());
        datain.setDataset(input.getName());
        datain.setStartInstance(getELExpression(input.getStart()));
        datain.setEndInstance(getELExpression(input.getEnd()));
        return datain;
    }

    private void propagateFileSystemProperties(Output output, Feed feed, Cluster cluster, COORDINATORAPP coord,
                                               Storage storage, Map<String, String> props)
        throws FalconException {

        // stats and meta paths
        createOutputEvent(output, feed, cluster, LocationType.STATS, coord, props, storage);
        createOutputEvent(output, feed, cluster, LocationType.META, coord, props, storage);
        createOutputEvent(output, feed, cluster, LocationType.TMP, coord, props, storage);
    }

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    private void createOutputEvent(Output output, Feed feed, Cluster cluster, LocationType locType,
                                   COORDINATORAPP coord, Map<String, String> props, Storage storage)
        throws FalconException {

        String name = output.getName();
        String type = locType.name().toLowerCase();

        SYNCDATASET dataset = createDataSet(feed, cluster, storage, name + type, locType);
        coord.getDatasets().getDatasetOrAsyncDataset().add(dataset);

        DATAOUT dataout = new DATAOUT();
        dataout.setName(name + type);
        dataout.setDataset(name + type);
        dataout.setInstance(getELExpression(output.getInstance()));

        OUTPUTEVENTS outputEvents = coord.getOutputEvents();
        if (outputEvents == null) {
            outputEvents = new OUTPUTEVENTS();
            coord.setOutputEvents(outputEvents);
        }
        outputEvents.getDataOut().add(dataout);

        String outputExpr = "${coord:dataOut('" + name + type + "')}";
        props.put(name + "." + type, outputExpr);
    }
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    private void propagateCommonCatalogTableProperties(CatalogStorage tableStorage,
                                                       Map<String, String> props, String prefix) {
        props.put(prefix + "_storage_type", tableStorage.getType().name());
        props.put(prefix + "_catalog_url", tableStorage.getCatalogUrl());
        props.put(prefix + "_database", tableStorage.getDatabase());
        props.put(prefix + "_table", tableStorage.getTable());
    }

    private void propagateCatalogTableProperties(Input input, CatalogStorage tableStorage,
                                                 Map<String, String> props) {
        String prefix = "falcon_" + input.getName();

        propagateCommonCatalogTableProperties(tableStorage, props, prefix);

        props.put(prefix + "_partition_filter_pig", "${coord:dataInPartitionFilter('input', 'pig')}");
        props.put(prefix + "_partition_filter_hive", "${coord:dataInPartitionFilter('input', 'hive')}");
        props.put(prefix + "_partition_filter_java", "${coord:dataInPartitionFilter('input', 'java')}");
    }

    private void propagateCatalogTableProperties(Output output, CatalogStorage tableStorage,
                                                 Map<String, String> props) {
        String prefix = "falcon_" + output.getName();

        propagateCommonCatalogTableProperties(tableStorage, props, prefix);

        props.put(prefix + "_dataout_partitions", "${coord:dataOutPartitions('output')}");
    }

    private String join(Iterator<String> itr, char sep) {
        String joinedStr = StringUtils.join(itr, sep);
        if (joinedStr.isEmpty()) {
            joinedStr = "null";
        }
        return joinedStr;
    }

    private String getELExpression(String expr) {
        if (expr != null) {
            expr = "${" + expr + "}";
        }
        return expr;
    }

    @Override
    protected Map<String, String> getEntityProperties() {
        Process process = getEntity();
        Map<String, String> props = new HashMap<String, String>();
        if (process.getProperties() != null) {
            for (Property prop : process.getProperties().getProperties()) {
                props.put(prop.getName(), prop.getValue());
            }
        }
        return props;
    }

    protected void createWorkflow(Cluster cluster, Process process, Workflow processWorkflow,
                                  String wfName, Path wfPath) throws FalconException {
        WORKFLOWAPP wfApp = getWorkflowTemplate(DEFAULT_WF_TEMPLATE);
        wfApp.setName(wfName);
        try {
            addLibExtensionsToWorkflow(cluster, wfApp, EntityType.PROCESS, null);
        } catch (IOException e) {
            throw new FalconException("Failed to add library extensions for the workflow", e);
        }

        EngineType engineType = processWorkflow.getEngine();
        for (Object object : wfApp.getDecisionOrForkOrJoin()) {
            if (!(object instanceof ACTION)) {
                continue;
            }

            String storagePath = getStoragePath(getEntity().getWorkflow().getPath());
            ACTION action = (ACTION) object;
            String actionName = action.getName();
            if (engineType == EngineType.OOZIE && actionName.equals("user-oozie-workflow")) {
                action.getSubWorkflow().setAppPath(storagePath);
            } else if (engineType == EngineType.PIG && actionName.equals("user-pig-job")) {
                decoratePIGAction(cluster, process, processWorkflow, storagePath, action.getPig(), wfPath);
            }
        }

        marshal(cluster, wfApp, wfPath);
    }

    private void decoratePIGAction(Cluster cluster, Process process, Workflow processWorkflow,
                                   String storagePath, PIG pigAction, Path wfPath) throws FalconException {
        pigAction.setScript(storagePath);

        addPrepareDeleteOutputPath(cluster, process, pigAction);

        addInputFeedsAsParams(pigAction, process, cluster);
        addOutputFeedsAsParams(pigAction, process, cluster);

        propagateProcessProperties(pigAction, process);

        setupHiveConfiguration(cluster, process, wfPath);

        addArchiveForCustomJars(cluster, processWorkflow, pigAction);
    }

    private void addPrepareDeleteOutputPath(Cluster cluster, Process process,
                                            PIG pigAction) throws FalconException {
        final PREPARE prepare = new PREPARE();
        final List<DELETE> deleteList = prepare.getDelete();
        for (Output output : process.getOutputs().getOutputs()) {
            Feed feed = EntityUtil.getEntity(EntityType.FEED, output.getFeed());
            Storage storage = FeedHelper.createStorage(cluster, feed);

            if (storage.getType() == Storage.TYPE.TABLE) {
                continue; // prepare delete only applies to FileSystem storage
            }

            final DELETE delete = new DELETE();
            delete.setPath("${wf:conf('" + output.getName() + "')}");
            deleteList.add(delete);
        }

        if (!deleteList.isEmpty()) {
            pigAction.setPrepare(prepare);
        }
    }

    private void addInputFeedsAsParams(PIG pigAction, Process process,
                                       Cluster cluster) throws FalconException {
        final List<String> paramList = pigAction.getParam();
        for (Input input : process.getInputs().getInputs()) {
            Feed feed = EntityUtil.getEntity(EntityType.FEED, input.getFeed());
            Storage storage = FeedHelper.createStorage(cluster, feed);

            final String inputName = "falcon_" + input.getName();
            if (storage.getType() == Storage.TYPE.FILESYSTEM) {
                paramList.add(inputName + "=${" + inputName + "}");
            } else if (storage.getType() == Storage.TYPE.TABLE) {
                Map<String, String> props = new HashMap<String, String>();
                propagateCommonCatalogTableProperties((CatalogStorage) storage, props, inputName);
                for (String key : props.keySet()) {
                    paramList.add(key + "=${wf:conf('" + key + "')}");
                }
                // finally add the pig filter for this input feed
                paramList.add(inputName + "_filter=${wf:conf('" + inputName + "_partition_filter_pig')}");
            }
        }
    }

    private void addOutputFeedsAsParams(PIG pigAction, Process process,
                                        Cluster cluster) throws FalconException {
        final List<String> paramList = pigAction.getParam();
        for (Output output : process.getOutputs().getOutputs()) {
            Feed feed = EntityUtil.getEntity(EntityType.FEED, output.getFeed());
            Storage storage = FeedHelper.createStorage(cluster, feed);

            if (storage.getType() == Storage.TYPE.FILESYSTEM) {
                final String outputName = "falcon_" + output.getName();
                paramList.add(outputName + "=${" + outputName + "}");
            } else if (storage.getType() == Storage.TYPE.TABLE) {
                Map<String, String> props = new HashMap<String, String>();
                propagateCatalogTableProperties(output, (CatalogStorage) storage, props);
                for (String key : props.keySet()) {
                    paramList.add(key + "=${wf:conf('" + key + "')}");
                }
            }
        }
    }

    private void propagateProcessProperties(PIG pigAction, Process process) {
        org.apache.falcon.entity.v0.process.Properties processProperties = process.getProperties();
        if (processProperties == null) {
            return;
        }

        // Propagate user defined properties to job configuration
        final List<org.apache.falcon.oozie.workflow.CONFIGURATION.Property> configuration =
                pigAction.getConfiguration().getProperty();
        for (org.apache.falcon.entity.v0.process.Property property : processProperties.getProperties()) {
            org.apache.falcon.oozie.workflow.CONFIGURATION.Property configProperty =
                    new org.apache.falcon.oozie.workflow.CONFIGURATION.Property();
            configProperty.setName(property.getName());
            configProperty.setValue(property.getValue());
            configuration.add(configProperty);
        }

        // Propagate user defined properties to pig script as macros
        // passed as parameters -p name=value that can be accessed as $name
        final List<String> paramList = pigAction.getParam();
        for (org.apache.falcon.entity.v0.process.Property property : processProperties.getProperties()) {
            paramList.add(property.getName() + "=" + property.getValue());
        }
    }

    // adds hive-site.xml in pig classpath
    private void setupHiveConfiguration(Cluster cluster, Process process,
                                        Path wfPath) throws FalconException {
        Input input = process.getInputs().getInputs().get(0); // at least one input should exist
        Feed feed = EntityUtil.getEntity(EntityType.FEED, input.getFeed());
        Storage storage = FeedHelper.createStorage(cluster, feed);
        if (storage.getType() == Storage.TYPE.FILESYSTEM) {
            return;
        }

        try {
            FileSystem fs = FileSystem.get(ClusterHelper.getConfiguration(cluster));
            Path confPath = new Path(wfPath, "conf");
            createHiveConf(fs, confPath, ((CatalogStorage) storage).getCatalogUrl(), ""); // DO NOT ADD PREFIX!!!
        } catch (IOException e) {
            throw new FalconException(e);
        }
    }

    private void addArchiveForCustomJars(Cluster cluster, Workflow processWorkflow,
                                         PIG pigAction) throws FalconException {
        String processWorkflowLib = processWorkflow.getLib();
        if (processWorkflowLib == null) {
            return;
        }

        Path libPath = new Path(processWorkflowLib);
        try {
            final FileSystem fs = libPath.getFileSystem(ClusterHelper.getConfiguration(cluster));
            if (fs.isFile(libPath)) {  // File, not a Dir
                pigAction.getArchive().add(processWorkflowLib);
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
                pigAction.getArchive().add(fileStatus.getPath().toString());
            }
        } catch (IOException e) {
            throw new FalconException("Error adding archive for custom jars under: " + libPath, e);
        }
    }
}
