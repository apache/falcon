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

package org.apache.falcon.oozie.feed;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.CatalogStorage;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.process.ExecutionType;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.oozie.OozieCoordinatorBuilder;
import org.apache.falcon.oozie.OozieOrchestrationWorkflowBuilder;
import org.apache.falcon.oozie.coordinator.COORDINATORAPP;
import org.apache.falcon.oozie.coordinator.SYNCDATASET;
import org.apache.falcon.oozie.coordinator.WORKFLOW;
import org.apache.falcon.oozie.coordinator.ACTION;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 *  Builds oozie coordinator for feed replication, one per source-target cluster combination.
 */
public class FeedReplicationCoordinatorBuilder extends OozieCoordinatorBuilder<Feed> {
    private static final String REPLICATION_COORD_TEMPLATE = "/coordinator/replication-coordinator.xml";
    private static final String IMPORT_HQL = "/action/feed/falcon-table-import.hql";
    private static final String EXPORT_HQL = "/action/feed/falcon-table-export.hql";

    private static final int THIRTY_MINUTES = 30 * 60 * 1000;

    private static final String PARALLEL = "parallel";
    private static final String TIMEOUT = "timeout";
    private static final String MR_MAX_MAPS = "maxMaps";
    private static final String MR_MAP_BANDWIDTH = "mapBandwidth";
    private static final String ORDER = "order";

    public FeedReplicationCoordinatorBuilder(Feed entity) {
        super(entity, LifeCycle.REPLICATION);
    }

    @Override
    public List<Properties> buildCoords(Cluster cluster, Path buildPath) throws FalconException {
        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(entity, cluster.getName());
        if (feedCluster.getType() == ClusterType.TARGET) {
            List<Properties> props = new ArrayList<Properties>();
            for (org.apache.falcon.entity.v0.feed.Cluster srcFeedCluster : entity.getClusters().getClusters()) {

                if (srcFeedCluster.getType() == ClusterType.SOURCE) {
                    Cluster srcCluster = ConfigurationStore.get().get(EntityType.CLUSTER, srcFeedCluster.getName());
                    // workflow is serialized to a specific dir
                    Path coordPath = new Path(buildPath, Tag.REPLICATION.name() + "/" + srcCluster.getName());

                    props.add(doBuild(srcCluster, cluster, coordPath));
                }
            }
            return props;
        }
        return null;
    }

    @Override
    protected WorkflowExecutionContext.EntityOperations getOperation() {
        return WorkflowExecutionContext.EntityOperations.REPLICATE;
    }

    private Properties doBuild(Cluster srcCluster, Cluster trgCluster, Path buildPath) throws FalconException {

        // Different workflow for each source since hive credentials vary for each cluster
        OozieOrchestrationWorkflowBuilder builder = OozieOrchestrationWorkflowBuilder.get(entity, trgCluster,
            Tag.REPLICATION);
        Properties wfProps = builder.build(trgCluster, buildPath);

        long replicationDelayInMillis = getReplicationDelayInMillis(srcCluster);
        Date sourceStartDate = getStartDate(srcCluster, replicationDelayInMillis);
        Date sourceEndDate = getEndDate(srcCluster);

        Date targetStartDate = getStartDate(trgCluster, replicationDelayInMillis);
        Date targetEndDate = getEndDate(trgCluster);

        if (noOverlapExists(sourceStartDate, sourceEndDate,
            targetStartDate, targetEndDate)) {
            LOG.warn("Not creating replication coordinator, as the source cluster: {} and target cluster: {} do "
                + "not have overlapping dates", srcCluster.getName(), trgCluster.getName());
            return null;
        }

        COORDINATORAPP coord = unmarshal(REPLICATION_COORD_TEMPLATE);

        String coordName = EntityUtil.getWorkflowName(Tag.REPLICATION, Arrays.asList(srcCluster.getName()),
            entity).toString();
        String start = sourceStartDate.after(targetStartDate)
            ? SchemaHelper.formatDateUTC(sourceStartDate) : SchemaHelper.formatDateUTC(targetStartDate);
        String end = sourceEndDate.before(targetEndDate)
            ? SchemaHelper.formatDateUTC(sourceEndDate) : SchemaHelper.formatDateUTC(targetEndDate);

        initializeCoordAttributes(coord, coordName, start, end, replicationDelayInMillis);
        setCoordControls(coord);

        final Storage sourceStorage = FeedHelper.createReadOnlyStorage(srcCluster, entity);
        initializeInputDataSet(srcCluster, coord, sourceStorage);

        final Storage targetStorage = FeedHelper.createStorage(trgCluster, entity);
        initializeOutputDataSet(trgCluster, coord, targetStorage);

        ACTION replicationWorkflowAction = getReplicationWorkflowAction(
            srcCluster, trgCluster, buildPath, coordName, sourceStorage, targetStorage);
        coord.setAction(replicationWorkflowAction);

        Path marshalPath = marshal(trgCluster, coord, buildPath);
        wfProps.putAll(getProperties(marshalPath, coordName));
        return wfProps;
    }

    private ACTION getReplicationWorkflowAction(Cluster srcCluster, Cluster trgCluster, Path buildPath,
        String wfName, Storage sourceStorage, Storage targetStorage) throws FalconException {
        ACTION action = new ACTION();
        WORKFLOW workflow = new WORKFLOW();

        workflow.setAppPath(getStoragePath(buildPath));
        Properties props = createCoordDefaultConfiguration(trgCluster, wfName);
        // Override CLUSTER_NAME property to include both source and target cluster pair
        String clusterProperty = trgCluster.getName()
                + WorkflowExecutionContext.CLUSTER_NAME_SEPARATOR + srcCluster.getName();
        props.put(WorkflowExecutionArgs.CLUSTER_NAME.getName(), clusterProperty);
        props.put("srcClusterName", srcCluster.getName());
        props.put("srcClusterColo", srcCluster.getColo());
        if (props.get(MR_MAX_MAPS) == null) { // set default if user has not overridden
            props.put(MR_MAX_MAPS, getDefaultMaxMaps());
        }
        if (props.get(MR_MAP_BANDWIDTH) == null) { // set default if user has not overridden
            props.put(MR_MAP_BANDWIDTH, getDefaultMapBandwidth());
        }

        // the storage type is uniform across source and target feeds for replication
        props.put("falconFeedStorageType", sourceStorage.getType().name());

        String instancePaths = "";
        if (sourceStorage.getType() == Storage.TYPE.FILESYSTEM) {
            String pathsWithPartitions = getPathsWithPartitions(srcCluster, trgCluster);
            instancePaths = pathsWithPartitions;

            propagateFileSystemCopyProperties(pathsWithPartitions, props);

            if (entity.getAvailabilityFlag() == null) {
                props.put("availabilityFlag", "NA");
            } else {
                props.put("availabilityFlag", entity.getAvailabilityFlag());
            }
        } else if (sourceStorage.getType() == Storage.TYPE.TABLE) {
            instancePaths = "${coord:dataIn('input')}";
            final CatalogStorage sourceTableStorage = (CatalogStorage) sourceStorage;
            propagateTableStorageProperties(srcCluster, sourceTableStorage, props, "falconSource");
            final CatalogStorage targetTableStorage = (CatalogStorage) targetStorage;
            propagateTableStorageProperties(trgCluster, targetTableStorage, props, "falconTarget");
            propagateTableCopyProperties(srcCluster, sourceTableStorage, trgCluster, targetTableStorage, props);
            setupHiveConfiguration(srcCluster, trgCluster, buildPath);
            props.put("availabilityFlag", "NA");
        }

        propagateLateDataProperties(instancePaths, sourceStorage.getType().name(), props);
        props.putAll(FeedHelper.getUserWorkflowProperties(getLifecycle()));

        workflow.setConfiguration(getConfig(props));
        action.setWorkflow(workflow);

        return action;
    }

    private String getDefaultMaxMaps() {
        return RuntimeProperties.get().getProperty("falcon.replication.workflow.maxmaps", "5");
    }

    private String getDefaultMapBandwidth() {
        return RuntimeProperties.get().getProperty("falcon.replication.workflow.mapbandwidth", "100");
    }

    private String getPathsWithPartitions(Cluster srcCluster, Cluster trgCluster) throws FalconException {
        String srcPart = FeedHelper.normalizePartitionExpression(
            FeedHelper.getCluster(entity, srcCluster.getName()).getPartition());
        srcPart = FeedHelper.evaluateClusterExp(srcCluster, srcPart);

        String targetPart = FeedHelper.normalizePartitionExpression(
            FeedHelper.getCluster(entity, trgCluster.getName()).getPartition());
        targetPart = FeedHelper.evaluateClusterExp(trgCluster, targetPart);

        StringBuilder pathsWithPartitions = new StringBuilder();
        pathsWithPartitions.append("${coord:dataIn('input')}/")
            .append(FeedHelper.normalizePartitionExpression(srcPart, targetPart));

        String parts = pathsWithPartitions.toString().replaceAll("//+", "/");
        parts = StringUtils.stripEnd(parts, "/");
        return parts;
    }

    private void propagateFileSystemCopyProperties(String paths, Properties props) throws FalconException {
        props.put("sourceRelativePaths", paths);

        props.put("distcpSourcePaths", "${coord:dataIn('input')}");
        props.put("distcpTargetPaths", "${coord:dataOut('output')}");
    }

    private void propagateTableStorageProperties(Cluster cluster, CatalogStorage tableStorage,
        Properties props, String prefix) {
        props.put(prefix + "NameNode", ClusterHelper.getStorageUrl(cluster));
        props.put(prefix + "JobTracker", ClusterHelper.getMREndPoint(cluster));
        props.put(prefix + "HcatNode", tableStorage.getCatalogUrl());

        props.put(prefix + "Database", tableStorage.getDatabase());
        props.put(prefix + "Table", tableStorage.getTable());
        props.put(prefix + "Partition", "${coord:dataInPartitions('input', 'hive-export')}");
    }

    private void propagateTableCopyProperties(Cluster srcCluster, CatalogStorage sourceStorage,
        Cluster trgCluster, CatalogStorage targetStorage, Properties props) {
        // create staging dirs for copy from source & set it as distcpSourcePaths - Read interface
        String sourceStagingPath =
            FeedHelper.getStagingPath(true, srcCluster, entity, sourceStorage, Tag.REPLICATION,
                NOMINAL_TIME_EL + "/" + trgCluster.getName());
        props.put("distcpSourcePaths", sourceStagingPath);
        // create staging dirs for export at source which needs to be writable - hence write interface
        String falconSourceStagingPath =
            FeedHelper.getStagingPath(false, srcCluster, entity, sourceStorage, Tag.REPLICATION,
                NOMINAL_TIME_EL + "/" + trgCluster.getName());
        props.put("falconSourceStagingDir", falconSourceStagingPath);

        // create staging dirs for import at target & set it as distcpTargetPaths
        String targetStagingPath =
            FeedHelper.getStagingPath(false, trgCluster, entity, targetStorage, Tag.REPLICATION,
                NOMINAL_TIME_EL + "/" + trgCluster.getName());
        props.put("distcpTargetPaths", targetStagingPath);

        props.put("sourceRelativePaths", IGNORE); // this will bot be used for Table storage.
    }

    private void propagateLateDataProperties(String instancePaths,
                                             String falconFeedStorageType, Properties props) {
        // todo these pairs are the same but used in different context
        // late data handler - should-record action
        props.put(WorkflowExecutionArgs.INPUT_FEED_NAMES.getName(), entity.getName());
        props.put(WorkflowExecutionArgs.INPUT_FEED_PATHS.getName(), instancePaths);
        props.put(WorkflowExecutionArgs.INPUT_NAMES.getName(), entity.getName());

        // storage type for each corresponding feed - in this case only one feed is involved
        // needed to compute usage based on storage type in LateDataHandler
        props.put(WorkflowExecutionArgs.INPUT_STORAGE_TYPES.getName(), falconFeedStorageType);

        // falcon post processing
        props.put(WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName(), entity.getName());
        props.put(WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName(), "${coord:dataOut('output')}");
    }

    private void setupHiveConfiguration(Cluster srcCluster, Cluster trgCluster,
                                        Path buildPath) throws FalconException {
        Configuration conf = ClusterHelper.getConfiguration(trgCluster);
        FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(conf);

        try {
            // copy import export scripts to stagingDir
            Path scriptPath = new Path(buildPath, "scripts");
            copyHiveScript(fs, scriptPath, IMPORT_HQL);
            copyHiveScript(fs, scriptPath, EXPORT_HQL);

            // create hive conf to stagingDir
            Path confPath = new Path(buildPath + "/conf");
            persistHiveConfiguration(fs, confPath, srcCluster, "falcon-source-");
            persistHiveConfiguration(fs, confPath, trgCluster, "falcon-target-");
        } catch (IOException e) {
            throw new FalconException("Unable to create hive conf files", e);
        }
    }

    private void copyHiveScript(FileSystem fs, Path scriptPath,
                                String resource) throws IOException {
        OutputStream out = null;
        InputStream in = null;
        try {
            out = fs.create(new Path(scriptPath, new Path(resource).getName()));
            in = FeedReplicationCoordinatorBuilder.class.getResourceAsStream(resource);
            IOUtils.copy(in, out);
        } finally {
            IOUtils.closeQuietly(in);
            IOUtils.closeQuietly(out);
        }
    }

    protected void persistHiveConfiguration(FileSystem fs, Path confPath,
                                            Cluster cluster, String prefix) throws IOException {
        Configuration hiveConf = getHiveCredentialsAsConf(cluster);
        OutputStream out = null;
        try {
            out = fs.create(new Path(confPath, prefix + "hive-site.xml"));
            hiveConf.writeXml(out);
        } finally {
            IOUtils.closeQuietly(out);
        }
    }

    private void initializeCoordAttributes(COORDINATORAPP coord, String coordName, String start, String end,
        long delayInMillis) {
        coord.setName(coordName);
        coord.setFrequency("${coord:" + entity.getFrequency().toString() + "}");

        if (delayInMillis > 0) {
            long delayInMins = -1 * delayInMillis / (1000 * 60);
            String elExp = "${now(0," + delayInMins + ")}";

            coord.getInputEvents().getDataIn().get(0).getInstance().set(0, elExp);
            coord.getOutputEvents().getDataOut().get(0).setInstance(elExp);
        }

        coord.setStart(start);
        coord.setEnd(end);
        coord.setTimezone(entity.getTimezone().getID());
    }

    private void setCoordControls(COORDINATORAPP coord) throws FalconException {
        long frequencyInMillis = ExpressionHelper.get().evaluate(entity.getFrequency().toString(), Long.class);
        long timeoutInMillis = frequencyInMillis * 6;
        if (timeoutInMillis < THIRTY_MINUTES) {
            timeoutInMillis = THIRTY_MINUTES;
        }

        Properties props = getEntityProperties(entity);
        String timeout = props.getProperty(TIMEOUT);
        if (timeout!=null) {
            try{
                timeoutInMillis= ExpressionHelper.get().evaluate(timeout, Long.class);
            } catch (Exception ignore) {
                LOG.error("Unable to evaluate timeout:", ignore);
            }
        }
        coord.getControls().setTimeout(String.valueOf(timeoutInMillis / (1000 * 60)));
        coord.getControls().setThrottle(String.valueOf(timeoutInMillis / frequencyInMillis * 2));

        String parallelProp = props.getProperty(PARALLEL);
        int parallel = 1;
        if (parallelProp != null) {
            try {
                parallel = Integer.parseInt(parallelProp);
            } catch (NumberFormatException ignore) {
                LOG.error("Unable to parse parallel:", ignore);
            }
        }
        coord.getControls().setConcurrency(String.valueOf(parallel));

        String orderProp = props.getProperty(ORDER);
        ExecutionType order = ExecutionType.FIFO;
        if (orderProp != null) {
            try {
                order = ExecutionType.fromValue(orderProp);
            } catch (IllegalArgumentException ignore) {
                LOG.error("Unable to parse order:", ignore);
            }
        }
        coord.getControls().setExecution(order.name());
    }


    private void initializeInputDataSet(Cluster cluster, COORDINATORAPP coord, Storage storage) throws FalconException {
        SYNCDATASET inputDataset = (SYNCDATASET)coord.getDatasets().getDatasetOrAsyncDataset().get(0);

        String uriTemplate = storage.getUriTemplate(LocationType.DATA);
        if (storage.getType() == Storage.TYPE.TABLE) {
            uriTemplate = uriTemplate.replace("thrift", "hcat"); // Oozie requires this!!!
        }
        inputDataset.setUriTemplate(uriTemplate);

        setDatasetValues(inputDataset, cluster);

        if (entity.getAvailabilityFlag() == null) {
            inputDataset.setDoneFlag("");
        } else {
            inputDataset.setDoneFlag(entity.getAvailabilityFlag());
        }
    }

    private void initializeOutputDataSet(Cluster cluster, COORDINATORAPP coord,
        Storage storage) throws FalconException {
        SYNCDATASET outputDataset = (SYNCDATASET)coord.getDatasets().getDatasetOrAsyncDataset().get(1);

        String uriTemplate = storage.getUriTemplate(LocationType.DATA);
        if (storage.getType() == Storage.TYPE.TABLE) {
            uriTemplate = uriTemplate.replace("thrift", "hcat"); // Oozie requires this!!!
        }
        outputDataset.setUriTemplate(uriTemplate);

        setDatasetValues(outputDataset, cluster);
    }

    private void setDatasetValues(SYNCDATASET dataset, Cluster cluster) {
        dataset.setInitialInstance(SchemaHelper.formatDateUTC(
            FeedHelper.getCluster(entity, cluster.getName()).getValidity().getStart()));
        dataset.setTimezone(entity.getTimezone().getID());
        dataset.setFrequency("${coord:" + entity.getFrequency().toString() + "}");
    }

    private long getReplicationDelayInMillis(Cluster srcCluster) throws FalconException {
        Frequency replicationDelay = FeedHelper.getCluster(entity, srcCluster.getName()).getDelay();
        long delayInMillis=0;
        if (replicationDelay != null) {
            delayInMillis = ExpressionHelper.get().evaluate(
                replicationDelay.toString(), Long.class);
        }

        return delayInMillis;
    }

    private Date getStartDate(Cluster cluster, long replicationDelayInMillis) {
        Date startDate = FeedHelper.getCluster(entity, cluster.getName()).getValidity().getStart();
        return replicationDelayInMillis == 0 ? startDate : new Date(startDate.getTime() + replicationDelayInMillis);
    }

    private Date getEndDate(Cluster cluster) {
        return FeedHelper.getCluster(entity, cluster.getName()).getValidity().getEnd();
    }

    private boolean noOverlapExists(Date sourceStartDate, Date sourceEndDate,
        Date targetStartDate, Date targetEndDate) {
        return sourceStartDate.after(targetEndDate) || targetStartDate.after(sourceEndDate);
    }
}
