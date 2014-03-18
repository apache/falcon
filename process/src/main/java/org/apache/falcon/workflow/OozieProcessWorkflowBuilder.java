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

package org.apache.falcon.workflow;

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.CatalogStorage;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.ProcessHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.process.EngineType;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Property;
import org.apache.falcon.entity.v0.process.Workflow;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.hadoop.HadoopClientFactory;
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
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.update.UpdateHelper;
import org.apache.falcon.util.OozieUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorJob.Timeunit;
import org.apache.oozie.client.OozieClient;

import javax.xml.bind.JAXBElement;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Oozie workflow builder for falcon entities.
 */
public class OozieProcessWorkflowBuilder extends OozieWorkflowBuilder<Process> {
    private static final Logger LOG = Logger.getLogger(OozieProcessWorkflowBuilder.class);

    public OozieProcessWorkflowBuilder(Process entity) {
        super(entity);
    }

    @Override
    public Map<String, Properties> newWorkflowSchedule(String... clusters) throws FalconException {
        Map<String, Properties> propertiesMap = new HashMap<String, Properties>();

        for (String clusterName : clusters) {
            org.apache.falcon.entity.v0.process.Cluster processCluster = ProcessHelper.getCluster(entity, clusterName);
            if (processCluster.getValidity().getStart().compareTo(processCluster.getValidity().getEnd()) >= 0) {
                LOG.info("process validity start <= end for cluster " + clusterName + ". Skipping schedule");
                break;
            }

            Cluster cluster = CONFIG_STORE.get(EntityType.CLUSTER, processCluster.getName());
            Path bundlePath = EntityUtil.getNewStagingPath(cluster, entity);
            map(cluster, bundlePath);
            Properties properties = createAppProperties(clusterName, bundlePath, CurrentUser.getUser());

            //Add libpath
            String libPath = entity.getWorkflow().getLib();
            if (!StringUtils.isEmpty(libPath)) {
                String path = libPath.replace("${nameNode}", "");
                properties.put(OozieClient.LIBPATH, "${nameNode}" + path);
            }

            if (entity.getInputs() != null) {
                for (Input in : entity.getInputs().getInputs()) {
                    if (in.isOptional()) {
                        addOptionalInputProperties(properties, in, clusterName);
                    }
                }
            }
            propertiesMap.put(clusterName, properties);
        }
        return propertiesMap;
    }

    private void addOptionalInputProperties(Properties properties, Input in, String clusterName)
        throws FalconException {
        Feed feed = EntityUtil.getEntity(EntityType.FEED, in.getFeed());
        org.apache.falcon.entity.v0.feed.Cluster cluster = FeedHelper.getCluster(feed, clusterName);
        String inName = in.getName();
        properties.put(inName + ".frequency", String.valueOf(feed.getFrequency().getFrequency()));
        properties.put(inName + ".freq_timeunit", mapToCoordTimeUnit(feed.getFrequency().getTimeUnit()).name());
        properties.put(inName + ".timezone", feed.getTimezone().getID());
        properties.put(inName + ".end_of_duration", Timeunit.NONE.name());
        properties.put(inName + ".initial-instance", SchemaHelper.formatDateUTC(cluster.getValidity().getStart()));
        properties.put(inName + ".done-flag", "notused");

        String locPath = FeedHelper.createStorage(clusterName, feed)
                .getUriTemplate(LocationType.DATA).replace('$', '%');
        properties.put(inName + ".uri-template", locPath);

        properties.put(inName + ".start-instance", in.getStart());
        properties.put(inName + ".end-instance", in.getEnd());
    }

    private Timeunit mapToCoordTimeUnit(TimeUnit tu) {
        switch (tu) {
        case days:
            return Timeunit.DAY;

        case hours:
            return Timeunit.HOUR;

        case minutes:
            return Timeunit.MINUTE;

        case months:
            return Timeunit.MONTH;

        default:
            throw new IllegalArgumentException("Unhandled time unit " + tu);
        }
    }

    @Override
    public Date getNextStartTime(Process process, String cluster, Date now) throws FalconException {
        org.apache.falcon.entity.v0.process.Cluster processCluster = ProcessHelper.getCluster(process, cluster);
        return EntityUtil.getNextStartTime(processCluster.getValidity().getStart(),
                process.getFrequency(), process.getTimezone(), now);
    }

    @Override
    public String[] getWorkflowNames() {
        return new String[]{EntityUtil.getWorkflowName(Tag.DEFAULT, entity).toString()};
    }

    private static final String DEFAULT_WF_TEMPLATE = "/config/workflow/process-parent-workflow.xml";
    private static final int THIRTY_MINUTES = 30 * 60 * 1000;

    @Override
    public List<COORDINATORAPP> getCoordinators(Cluster cluster, Path bundlePath) throws FalconException {
        try {
            FileSystem fs = HadoopClientFactory.get().createFileSystem(ClusterHelper.getConfiguration(cluster));

            //Copy user workflow and lib to staging dir
            Map<String, String> checksums = UpdateHelper.checksumAndCopy(fs, new Path(entity.getWorkflow().getPath()),
                new Path(bundlePath, EntityUtil.PROCESS_USER_DIR));
            if (entity.getWorkflow().getLib() != null && fs.exists(new Path(entity.getWorkflow().getLib()))) {
                checksums.putAll(UpdateHelper.checksumAndCopy(fs, new Path(entity.getWorkflow().getLib()),
                    new Path(bundlePath, EntityUtil.PROCESS_USERLIB_DIR)));
            }

            writeChecksums(fs, new Path(bundlePath, EntityUtil.PROCESS_CHECKSUM_FILE), checksums);
        } catch (IOException e) {
            throw new FalconException("Failed to copy user workflow/lib", e);
        }

        List<COORDINATORAPP> apps = new ArrayList<COORDINATORAPP>();
        apps.add(createDefaultCoordinator(cluster, bundlePath));

        return apps;
    }

    private void writeChecksums(FileSystem fs, Path path, Map<String, String> checksums) throws FalconException {
        try {
            FSDataOutputStream stream = fs.create(path);
            try {
                for (Map.Entry<String, String> entry : checksums.entrySet()) {
                    stream.write((entry.getKey() + "=" + entry.getValue() + "\n").getBytes());
                }
            } finally {
                stream.close();
            }
        } catch (IOException e) {
            throw new FalconException("Failed to copy user workflow/lib", e);
        }
    }

    private Path getUserWorkflowPath(Cluster cluster, Path bundlePath) throws FalconException {
        try {
            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(ClusterHelper.getConfiguration(cluster));
            Path wfPath = new Path(entity.getWorkflow().getPath());
            if (fs.isFile(wfPath)) {
                return new Path(bundlePath, EntityUtil.PROCESS_USER_DIR + "/" + wfPath.getName());
            } else {
                return new Path(bundlePath, EntityUtil.PROCESS_USER_DIR);
            }
        } catch(IOException e) {
            throw new FalconException("Failed to get workflow path", e);
        }
    }

    private Path getUserLibPath(Cluster cluster, Path bundlePath) throws FalconException {
        try {
            if (entity.getWorkflow().getLib() == null) {
                return null;
            }
            Path libPath = new Path(entity.getWorkflow().getLib());

            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(ClusterHelper.getConfiguration(cluster));
            if (fs.isFile(libPath)) {
                return new Path(bundlePath, EntityUtil.PROCESS_USERLIB_DIR + "/" + libPath.getName());
            } else {
                return new Path(bundlePath, EntityUtil.PROCESS_USERLIB_DIR);
            }
        } catch(IOException e) {
            throw new FalconException("Failed to get user lib path", e);
        }
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
        if (entity == null) {
            return null;
        }

        COORDINATORAPP coord = new COORDINATORAPP();
        String coordName = EntityUtil.getWorkflowName(Tag.DEFAULT, entity).toString();
        Path coordPath = getCoordPath(bundlePath, coordName);

        // coord attributes
        initializeCoordAttributes(cluster, entity, coord, coordName);

        CONTROLS controls = initializeControls(entity); // controls
        coord.setControls(controls);

        // Configuration
        Map<String, String> props = createCoordDefaultConfiguration(cluster, coordPath, coordName);

        initializeInputPaths(cluster, entity, coord, props); // inputs
        initializeOutputPaths(cluster, entity, coord, props);  // outputs

        Workflow processWorkflow = entity.getWorkflow();
        propagateUserWorkflowProperties(processWorkflow, props, entity.getName());

        // create parent wf
        createWorkflow(cluster, entity, processWorkflow, coordName, coordPath);

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
            props.put("falconInputFeeds", "NONE");
            props.put("falconInPaths", "IGNORE");
            return;
        }

        List<String> inputFeeds = new ArrayList<String>();
        List<String> inputPaths = new ArrayList<String>();
        List<String> inputFeedStorageTypes = new ArrayList<String>();
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

            inputFeeds.add(feed.getName());
            inputPaths.add(inputExpr);
            inputFeedStorageTypes.add(storage.getType().name());
        }

        propagateLateDataProperties(inputFeeds, inputPaths, inputFeedStorageTypes, props);
    }

    private void propagateLateDataProperties(List<String> inputFeeds, List<String> inputPaths,
        List<String> inputFeedStorageTypes, Map<String, String> props) {
        // populate late data handler - should-record action
        props.put("falconInputFeeds", join(inputFeeds.iterator(), '#'));
        props.put("falconInPaths", join(inputPaths.iterator(), '#'));

        // storage type for each corresponding feed sent as a param to LateDataHandler
        // needed to compute usage based on storage type in LateDataHandler
        props.put("falconInputFeedStorageTypes", join(inputFeedStorageTypes.iterator(), '#'));
    }

    private void initializeOutputPaths(Cluster cluster, Process process, COORDINATORAPP coord,
        Map<String, String> props) throws FalconException {
        if (process.getOutputs() == null) {
            props.put(ARG.feedNames.getPropName(), "NONE");
            props.put(ARG.feedInstancePaths.getPropName(), "IGNORE");
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
            outputFeeds.add(feed.getName());
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

        props.put(prefix + "_partition_filter_pig",
            "${coord:dataInPartitionFilter('" + input.getName() + "', 'pig')}");
        props.put(prefix + "_partition_filter_hive",
            "${coord:dataInPartitionFilter('" + input.getName() + "', 'hive')}");
        props.put(prefix + "_partition_filter_java",
            "${coord:dataInPartitionFilter('" + input.getName() + "', 'java')}");
    }

    private void propagateCatalogTableProperties(Output output, CatalogStorage tableStorage,
        Map<String, String> props) {
        String prefix = "falcon_" + output.getName();

        propagateCommonCatalogTableProperties(tableStorage, props, prefix);

        props.put(prefix + "_dataout_partitions",
            "${coord:dataOutPartitions('" + output.getName() + "')}");
        props.put(prefix + "_dated_partition_value", "${coord:dataOutPartitionValue('"
            + output.getName() + "', '" + tableStorage.getDatedPartitionKey() + "')}");
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
        Map<String, String> props = new HashMap<String, String>();
        if (entity.getProperties() != null) {
            for (Property prop : entity.getProperties().getProperties()) {
                props.put(prop.getName(), prop.getValue());
            }
        }
        return props;
    }

    private void propagateUserWorkflowProperties(Workflow processWorkflow,
        Map<String, String> props, String processName) {
        props.put("userWorkflowName", ProcessHelper.getProcessWorkflowName(
            processWorkflow.getName(), processName));
        props.put("userWorkflowVersion", processWorkflow.getVersion());
        props.put("userWorkflowEngine", processWorkflow.getEngine().value());
    }

    protected void createWorkflow(Cluster cluster, Process process, Workflow processWorkflow,
        String wfName, Path parentWfPath) throws FalconException {
        WORKFLOWAPP wfApp = getWorkflowTemplate(DEFAULT_WF_TEMPLATE);
        wfApp.setName(wfName);
        try {
            addLibExtensionsToWorkflow(cluster, wfApp, EntityType.PROCESS, null);
        } catch (IOException e) {
            throw new FalconException("Failed to add library extensions for the workflow", e);
        }

        String userWfPath = getUserWorkflowPath(cluster, parentWfPath.getParent()).toString();
        EngineType engineType = processWorkflow.getEngine();
        for (Object object : wfApp.getDecisionOrForkOrJoin()) {
            if (!(object instanceof ACTION)) {
                continue;
            }

            ACTION action = (ACTION) object;
            String actionName = action.getName();
            if (engineType == EngineType.OOZIE && actionName.equals("user-oozie-workflow")) {
                action.getSubWorkflow().setAppPath("${nameNode}" + userWfPath);
            } else if (engineType == EngineType.PIG && actionName.equals("user-pig-job")) {
                decoratePIGAction(cluster, process, action.getPig(), parentWfPath);
            } else if (engineType == EngineType.HIVE && actionName.equals("user-hive-job")) {
                decorateHiveAction(cluster, process, action, parentWfPath);
            } else if (FALCON_ACTIONS.contains(actionName)) {
                decorateWithOozieRetries(action);
            }
        }

        //Create parent workflow
        marshal(cluster, wfApp, parentWfPath);
    }

    private void decoratePIGAction(Cluster cluster, Process process,
        PIG pigAction, Path parentWfPath) throws FalconException {
        Path userWfPath = getUserWorkflowPath(cluster, parentWfPath.getParent());
        pigAction.setScript("${nameNode}" + userWfPath.toString());

        addPrepareDeleteOutputPath(process, pigAction);

        final List<String> paramList = pigAction.getParam();
        addInputFeedsAsParams(paramList, process, cluster, EngineType.PIG.name().toLowerCase());
        addOutputFeedsAsParams(paramList, process, cluster);

        propagateProcessProperties(pigAction, process);

        Storage.TYPE storageType = getStorageType(cluster, process);
        if (Storage.TYPE.TABLE == storageType) {
            // adds hive-site.xml in pig classpath
            setupHiveConfiguration(cluster, parentWfPath, ""); // DO NOT ADD PREFIX!!!
            pigAction.getFile().add("${wf:appPath()}/conf/hive-site.xml");
        }

        addArchiveForCustomJars(cluster, pigAction.getArchive(),
            getUserLibPath(cluster, parentWfPath.getParent()));
    }

    private void decorateHiveAction(Cluster cluster, Process process, ACTION wfAction,
        Path parentWfPath) throws FalconException {

        JAXBElement<org.apache.falcon.oozie.hive.ACTION> actionJaxbElement = OozieUtils.unMarshalHiveAction(wfAction);
        org.apache.falcon.oozie.hive.ACTION hiveAction = actionJaxbElement.getValue();

        Path userWfPath = getUserWorkflowPath(cluster, parentWfPath.getParent());
        hiveAction.setScript("${nameNode}" + userWfPath.toString());

        addPrepareDeleteOutputPath(process, hiveAction);

        final List<String> paramList = hiveAction.getParam();
        addInputFeedsAsParams(paramList, process, cluster, EngineType.HIVE.name().toLowerCase());
        addOutputFeedsAsParams(paramList, process, cluster);

        propagateProcessProperties(hiveAction, process);

        setupHiveConfiguration(cluster, parentWfPath, "falcon-");

        addArchiveForCustomJars(cluster, hiveAction.getArchive(),
            getUserLibPath(cluster, parentWfPath.getParent()));

        OozieUtils.marshalHiveAction(wfAction, actionJaxbElement);
    }

    private void addPrepareDeleteOutputPath(Process process,
        PIG pigAction) throws FalconException {
        List<String> deleteOutputPathList = getPrepareDeleteOutputPathList(process);
        if (deleteOutputPathList.isEmpty()) {
            return;
        }

        final PREPARE prepare = new PREPARE();
        final List<DELETE> deleteList = prepare.getDelete();

        for (String deletePath : deleteOutputPathList) {
            final DELETE delete = new DELETE();
            delete.setPath(deletePath);
            deleteList.add(delete);
        }

        if (!deleteList.isEmpty()) {
            pigAction.setPrepare(prepare);
        }
    }

    private void addPrepareDeleteOutputPath(Process process,
        org.apache.falcon.oozie.hive.ACTION hiveAction)
        throws FalconException {

        List<String> deleteOutputPathList = getPrepareDeleteOutputPathList(process);
        if (deleteOutputPathList.isEmpty()) {
            return;
        }

        org.apache.falcon.oozie.hive.PREPARE prepare = new org.apache.falcon.oozie.hive.PREPARE();
        List<org.apache.falcon.oozie.hive.DELETE> deleteList = prepare.getDelete();

        for (String deletePath : deleteOutputPathList) {
            org.apache.falcon.oozie.hive.DELETE delete = new org.apache.falcon.oozie.hive.DELETE();
            delete.setPath(deletePath);
            deleteList.add(delete);
        }

        if (!deleteList.isEmpty()) {
            hiveAction.setPrepare(prepare);
        }
    }

    private List<String> getPrepareDeleteOutputPathList(Process process) throws FalconException {
        final List<String> deleteList = new ArrayList<String>();
        if (process.getOutputs() == null) {
            return deleteList;
        }

        for (Output output : process.getOutputs().getOutputs()) {
            Feed feed = EntityUtil.getEntity(EntityType.FEED, output.getFeed());

            if (FeedHelper.getStorageType(feed) == Storage.TYPE.TABLE) {
                continue; // prepare delete only applies to FileSystem storage
            }

            deleteList.add("${wf:conf('" + output.getName() + "')}");
        }

        return deleteList;
    }

    private void addInputFeedsAsParams(List<String> paramList, Process process, Cluster cluster,
        String engineType) throws FalconException {
        if (process.getInputs() == null) {
            return;
        }

        for (Input input : process.getInputs().getInputs()) {
            Feed feed = EntityUtil.getEntity(EntityType.FEED, input.getFeed());
            Storage storage = FeedHelper.createStorage(cluster, feed);

            final String inputName = input.getName();
            if (storage.getType() == Storage.TYPE.FILESYSTEM) {
                paramList.add(inputName + "=${" + inputName + "}"); // no prefix for backwards compatibility
            } else if (storage.getType() == Storage.TYPE.TABLE) {
                final String paramName = "falcon_" + inputName; // prefix 'falcon' for new params
                Map<String, String> props = new HashMap<String, String>();
                propagateCommonCatalogTableProperties((CatalogStorage) storage, props, paramName);
                for (String key : props.keySet()) {
                    paramList.add(key + "=${wf:conf('" + key + "')}");
                }

                paramList.add(paramName + "_filter=${wf:conf('"
                    + paramName + "_partition_filter_" + engineType + "')}");
            }
        }
    }

    private void addOutputFeedsAsParams(List<String> paramList, Process process,
        Cluster cluster) throws FalconException {
        if (process.getOutputs() == null) {
            return;
        }

        for (Output output : process.getOutputs().getOutputs()) {
            Feed feed = EntityUtil.getEntity(EntityType.FEED, output.getFeed());
            Storage storage = FeedHelper.createStorage(cluster, feed);

            if (storage.getType() == Storage.TYPE.FILESYSTEM) {
                final String outputName = output.getName();  // no prefix for backwards compatibility
                paramList.add(outputName + "=${" + outputName + "}");
            } else if (storage.getType() == Storage.TYPE.TABLE) {
                Map<String, String> props = new HashMap<String, String>();
                propagateCatalogTableProperties(output, (CatalogStorage) storage, props); // prefix is auto added
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

        // Propagate user defined properties to pig script as macros
        // passed as parameters -p name=value that can be accessed as $name
        final List<String> paramList = pigAction.getParam();

        for (org.apache.falcon.entity.v0.process.Property property : processProperties.getProperties()) {
            org.apache.falcon.oozie.workflow.CONFIGURATION.Property configProperty =
                new org.apache.falcon.oozie.workflow.CONFIGURATION.Property();
            configProperty.setName(property.getName());
            configProperty.setValue(property.getValue());
            configuration.add(configProperty);

            paramList.add(property.getName() + "=" + property.getValue());
        }
    }

    private void propagateProcessProperties(org.apache.falcon.oozie.hive.ACTION hiveAction, Process process) {
        org.apache.falcon.entity.v0.process.Properties processProperties = process.getProperties();
        if (processProperties == null) {
            return;
        }

        // Propagate user defined properties to job configuration
        final List<org.apache.falcon.oozie.hive.CONFIGURATION.Property> configuration =
            hiveAction.getConfiguration().getProperty();

        // Propagate user defined properties to pig script as macros
        // passed as parameters -p name=value that can be accessed as $name
        final List<String> paramList = hiveAction.getParam();

        for (org.apache.falcon.entity.v0.process.Property property : processProperties.getProperties()) {
            org.apache.falcon.oozie.hive.CONFIGURATION.Property configProperty =
                new org.apache.falcon.oozie.hive.CONFIGURATION.Property();
            configProperty.setName(property.getName());
            configProperty.setValue(property.getValue());
            configuration.add(configProperty);

            paramList.add(property.getName() + "=" + property.getValue());
        }
    }

    private Storage.TYPE getStorageType(Cluster cluster, Process process) throws FalconException {
        Storage.TYPE storageType = Storage.TYPE.FILESYSTEM;
        if (process.getInputs() == null) {
            return storageType;
        }

        for (Input input : process.getInputs().getInputs()) {
            Feed feed = EntityUtil.getEntity(EntityType.FEED, input.getFeed());
            storageType = FeedHelper.getStorageType(feed, cluster);
            if (Storage.TYPE.TABLE == storageType) {
                break;
            }
        }

        return storageType;
    }

    // creates hive-site.xml configuration in conf dir.
    private void setupHiveConfiguration(Cluster cluster, Path wfPath,
        String prefix) throws FalconException {
        String catalogUrl = ClusterHelper.getInterface(cluster, Interfacetype.REGISTRY).getEndpoint();
        try {
            FileSystem fs = HadoopClientFactory.get().createFileSystem(ClusterHelper.getConfiguration(cluster));
            Path confPath = new Path(wfPath, "conf");
            createHiveConf(fs, confPath, catalogUrl, cluster, prefix);
        } catch (IOException e) {
            throw new FalconException(e);
        }
    }

    private void addArchiveForCustomJars(Cluster cluster, List<String> archiveList,
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
