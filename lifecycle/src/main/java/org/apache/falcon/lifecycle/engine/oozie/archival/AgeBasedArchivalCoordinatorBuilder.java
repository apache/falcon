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

package org.apache.falcon.lifecycle.engine.oozie.archival;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.process.ExecutionType;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.lifecycle.engine.oozie.utils.OozieBuilderUtils;
import org.apache.falcon.oozie.coordinator.SYNCDATASET;
import org.apache.falcon.oozie.coordinator.DATAOUT;
import org.apache.falcon.oozie.coordinator.DATAIN;
import org.apache.falcon.oozie.coordinator.COORDINATORAPP;
import org.apache.falcon.oozie.coordinator.ACTION;
import org.apache.falcon.oozie.coordinator.WORKFLOW;
import org.apache.falcon.oozie.coordinator.DATASETS;
import org.apache.falcon.oozie.coordinator.CONTROLS;
import org.apache.falcon.oozie.coordinator.INPUTEVENTS;
import org.apache.falcon.oozie.coordinator.OUTPUTEVENTS;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class AgeBasedArchivalCoordinatorBuilder {

    private AgeBasedArchivalCoordinatorBuilder() {

    }

    private static final Logger LOG = LoggerFactory.getLogger(AgeBasedArchivalCoordinatorBuilder.class);
    private static final int THIRTY_MINUTES = 30 * 60 * 1000;
    private static final String PARALLEL = "parallel";
    private static final String TIMEOUT = "timeout";
    private static final String ORDER = "order";
    public static final String IN_DATASET_NAME = "input-dataset";
    public static final String OUT_DATASET_NAME = "output-dataset";
    public static final String DATAIN_NAME = "input";
    public static final String DATAOUT_NAME = "output";

    /**
     * Builds the coordinator app.
     * @param cluster - cluster to schedule retention on.
     * @param basePath - Base path to marshal coordinator app.
     * @param feed - feed for which retention is to be scheduled.
     * @param wfProp - properties passed from workflow to coordinator e.g. ENTITY_PATH
     * @return - Properties from creating the coordinator application to be used by Bundle.
     * @throws FalconException
     */
    public static Properties build(Cluster cluster, Path basePath, Feed feed, Properties wfProp)
            throws FalconException {

        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(feed, cluster.getName());

        // workflow is serialized to a specific dir
        Path coordPath = new Path(basePath, Tag.ARCHIVAL.name() + "/" + feedCluster.getName());

        COORDINATORAPP coord = new COORDINATORAPP();

        String coordName = EntityUtil.getWorkflowName(LifeCycle.ARCHIVAL.getTag(), feed).toString();

        long replicationDelayInMillis = getReplicationDelayInMillis(feed, cluster);
        Date sourceStartDate = getStartDate(feed, cluster, replicationDelayInMillis);
        Date sourceEndDate = getEndDate(feed, cluster);

        coord.setName(coordName);
        coord.setFrequency("${coord:" + feed.getFrequency().toString() + "}");
        coord.setEnd(SchemaHelper.formatDateUTC(sourceEndDate));
        coord.setStart(SchemaHelper.formatDateUTC(sourceStartDate));
        coord.setTimezone(feed.getTimezone().getID());

        if (replicationDelayInMillis > 0) {
            long delayInMins = -1 * replicationDelayInMillis / (1000 * 60);
            String elExp = "${now(0," + delayInMins + ")}";

            initializeInputOutputPath(coord, cluster, feed, elExp);
        }

        setCoordControls(feed, coord);

        final Storage sourceStorage = FeedHelper.createReadOnlyStorage(cluster, feed);
        initializeInputDataSet(feed, cluster, coord, sourceStorage);

        String archivalPath = FeedHelper.getArchivalPath(feed, cluster.getName());
        initializeOutputDataSet(feed, cluster, coord, archivalPath);

        ACTION replicationWorkflowAction = getReplicationWorkflowAction(feed, cluster, coordPath, coordName, sourceStorage);
        coord.setAction(replicationWorkflowAction);

        Path marshalPath = OozieBuilderUtils.marshalCoordinator(cluster, coord, coordPath);
        wfProp.putAll(OozieBuilderUtils.getProperties(marshalPath, coordName));

        return wfProp;

    }

    private static void initializeInputOutputPath(COORDINATORAPP coord, Cluster cluster, Feed feed, String elExp)
            throws FalconException {

        if (coord.getInputEvents() == null) {
            coord.setInputEvents(new INPUTEVENTS());
        }

        if (coord.getOutputEvents() == null) {
            coord.setOutputEvents(new OUTPUTEVENTS());
        }

        DATAIN datain = createDataIn(feed, cluster);
        coord.getInputEvents().getDataIn().add(datain);

        DATAOUT dataout = createDataOut(feed, cluster);
        coord.getOutputEvents().getDataOut().add(dataout);

        coord.getInputEvents().getDataIn().get(0).getInstance().set(0, elExp);
        coord.getOutputEvents().getDataOut().get(0).setInstance(elExp);
    }


    private static DATAIN createDataIn(Feed feed, Cluster cluster) {
        DATAIN datain = new DATAIN();
        datain.setName(DATAIN_NAME);
        datain.setDataset(IN_DATASET_NAME);
        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(feed, cluster.getName());
        datain.getInstance().add(SchemaHelper.formatDateUTC(feedCluster.getValidity().getStart()));
        return datain;
    }

    private static DATAOUT createDataOut(Feed feed, Cluster cluster) {
        DATAOUT dataout = new DATAOUT();
        dataout.setName(DATAOUT_NAME);
        dataout.setDataset(OUT_DATASET_NAME);
        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(feed, cluster.getName());
        dataout.setInstance("${coord:current(0)}");
        return dataout;
    }

    private static ACTION getReplicationWorkflowAction(Feed feed, Cluster cluster, Path buildPath, String coordName,
                                                Storage sourceStorage) throws FalconException {
        ACTION action = new ACTION();
        WORKFLOW workflow = new WORKFLOW();

        workflow.setAppPath(OozieBuilderUtils.getStoragePath(String.valueOf(buildPath)));
        Properties props = OozieBuilderUtils.createCoordDefaultConfiguration(coordName, feed);

        // Setting CLUSTER_NAME property to include source cluster
        props.put(WorkflowExecutionArgs.CLUSTER_NAME.getName(), cluster.getName());
        props.put("srcClusterName", cluster.getName());
        props.put("srcClusterColo", cluster.getColo());

        // the storage type is uniform across source and target feeds for replication
        props.put("falconFeedStorageType", sourceStorage.getType().name());

        String instancePaths = "";
        if (sourceStorage.getType() == Storage.TYPE.FILESYSTEM) {
            String pathsWithPartitions = getPathsWithPartitions(feed, cluster);
            instancePaths = pathsWithPartitions;
            propagateFileSystemCopyProperties(pathsWithPartitions, props);
        }

        propagateLateDataProperties(feed, instancePaths, sourceStorage.getType().name(), props);
        // Add the custom properties set in feed. Else, dryrun won't catch any missing props.
        props.putAll(EntityUtil.getEntityProperties(feed));
        workflow.setConfiguration(OozieBuilderUtils.getCoordinatorConfig(props));
        action.setWorkflow(workflow);

        return action;
    }

    private static String getPathsWithPartitions(Feed feed, Cluster cluster) throws FalconException {
        String srcPart = FeedHelper.normalizePartitionExpression(
                FeedHelper.getCluster(feed, cluster.getName()).getPartition());
        srcPart = FeedHelper.evaluateClusterExp(cluster, srcPart);

        StringBuilder pathsWithPartitions = new StringBuilder();
        pathsWithPartitions.append("${coord:dataIn('input')}/")
                .append(FeedHelper.normalizePartitionExpression(srcPart));

        String parts = pathsWithPartitions.toString().replaceAll("//+", "/");
        parts = StringUtils.stripEnd(parts, "/");
        return parts;
    }

    private static void propagateLateDataProperties(Feed feed, String instancePaths,
                                             String falconFeedStorageType, Properties props) {
        // todo these pairs are the same but used in different context
        // late data handler - should-record action
        props.put(WorkflowExecutionArgs.INPUT_FEED_NAMES.getName(), feed.getName());
        props.put(WorkflowExecutionArgs.INPUT_FEED_PATHS.getName(), instancePaths);
        props.put(WorkflowExecutionArgs.INPUT_NAMES.getName(), feed.getName());

        // storage type for each corresponding feed - in this case only one feed is involved
        // needed to compute usage based on storage type in LateDataHandler
        props.put(WorkflowExecutionArgs.INPUT_STORAGE_TYPES.getName(), falconFeedStorageType);

        // falcon post processing
        props.put(WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName(), feed.getName());
        props.put(WorkflowExecutionArgs.OUTPUT_NAMES.getName(), feed.getName());
        props.put(WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName(), "${coord:dataOut('output')}");
    }

    private static void propagateFileSystemCopyProperties(String paths, Properties props) throws FalconException {
        props.put("sourceRelativePaths", paths);

        props.put("distcpSourcePaths", "${coord:dataIn('input')}");
        props.put("distcpTargetPaths", "${coord:dataOut('output')}");
    }

    private static void setCoordControls(Feed feed, COORDINATORAPP coord) throws FalconException {
        // set controls
        CONTROLS controls = new CONTROLS();

        long frequencyInMillis = ExpressionHelper.get().evaluate(feed.getFrequency().toString(), Long.class);
        long timeoutInMillis = frequencyInMillis * 6;
        if (timeoutInMillis < THIRTY_MINUTES) {
            timeoutInMillis = THIRTY_MINUTES;
        }

        Properties props = EntityUtil.getEntityProperties(feed);
        String timeout = props.getProperty(TIMEOUT);
        if (timeout!=null) {
            try{
                timeoutInMillis= ExpressionHelper.get().evaluate(timeout, Long.class);
            } catch (Exception ignore) {
                LOG.error("Unable to evaluate timeout:", ignore);
            }
        }

        String parallelProp = props.getProperty(PARALLEL);
        int parallel = 1;
        if (parallelProp != null) {
            try {
                parallel = Integer.parseInt(parallelProp);
            } catch (NumberFormatException ignore) {
                LOG.error("Unable to parse parallel:", ignore);
            }
        }

        String orderProp = props.getProperty(ORDER);
        ExecutionType order = ExecutionType.FIFO;
        if (orderProp != null) {
            try {
                order = ExecutionType.fromValue(orderProp);
            } catch (IllegalArgumentException ignore) {
                LOG.error("Unable to parse order:", ignore);
            }
        }

        controls.setTimeout(String.valueOf(timeoutInMillis / (1000 * 60)));
        controls.setThrottle(String.valueOf(timeoutInMillis / frequencyInMillis * 2));
        controls.setConcurrency(String.valueOf(parallel));
        controls.setExecution(order.name());
        coord.setControls(controls);
    }

    private static void initializeInputDataSet(Feed feed, Cluster cluster, COORDINATORAPP coord, Storage storage) throws FalconException {
        if (coord.getDatasets() == null) {
            coord.setDatasets(new DATASETS());
        }

        SYNCDATASET inputDataset = new SYNCDATASET();
        inputDataset.setName(IN_DATASET_NAME);

        String uriTemplate = storage.getUriTemplate(LocationType.DATA);
        inputDataset.setUriTemplate(uriTemplate);

        setDatasetValues(feed, inputDataset, cluster);

        if (feed.getAvailabilityFlag() == null) {
            inputDataset.setDoneFlag("");
        } else {
            inputDataset.setDoneFlag(feed.getAvailabilityFlag());
        }

        coord.getDatasets().getDatasetOrAsyncDataset().add(inputDataset);
    }

    private static void initializeOutputDataSet(Feed feed, Cluster cluster, COORDINATORAPP coord,
                                         String targetPath) throws FalconException {
        if (coord.getDatasets() == null) {
            coord.setDatasets(new DATASETS());
        }

        SYNCDATASET outputDataset = new SYNCDATASET();
        outputDataset.setName(OUT_DATASET_NAME);
        outputDataset.setUriTemplate(targetPath);

        setDatasetValues(feed, outputDataset, cluster);
        coord.getDatasets().getDatasetOrAsyncDataset().add(outputDataset);
    }


    private static void setDatasetValues(Feed feed, SYNCDATASET dataset, Cluster cluster) {
        dataset.setInitialInstance(SchemaHelper.formatDateUTC(
                FeedHelper.getCluster(feed, cluster.getName()).getValidity().getStart()));
        dataset.setTimezone(feed.getTimezone().getID());
        dataset.setFrequency("${coord:" + feed.getFrequency().toString() + "}");
    }

    private static long getReplicationDelayInMillis(Feed feed, Cluster srcCluster) throws FalconException {
        Frequency replicationDelay = FeedHelper.getCluster(feed, srcCluster.getName()).getDelay();
        long delayInMillis=0;
        if (replicationDelay != null) {
            delayInMillis = ExpressionHelper.get().evaluate(
                    replicationDelay.toString(), Long.class);
        }

        return delayInMillis;
    }

    private static Date getStartDate(Feed feed, Cluster cluster, long replicationDelayInMillis) {
        Date startDate = FeedHelper.getCluster(feed, cluster.getName()).getValidity().getStart();
        return replicationDelayInMillis == 0 ? startDate : new Date(startDate.getTime() + replicationDelayInMillis);
    }

    private static Date getEndDate(Feed feed, Cluster cluster) {
        return FeedHelper.getCluster(feed, cluster.getName()).getValidity().getEnd();
    }

}
