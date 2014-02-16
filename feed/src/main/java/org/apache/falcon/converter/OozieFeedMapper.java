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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.CatalogStorage;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Property;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.messaging.EntityInstanceMessage.ARG;
import org.apache.falcon.messaging.EntityInstanceMessage.EntityOps;
import org.apache.falcon.oozie.coordinator.ACTION;
import org.apache.falcon.oozie.coordinator.COORDINATORAPP;
import org.apache.falcon.oozie.coordinator.SYNCDATASET;
import org.apache.falcon.oozie.coordinator.WORKFLOW;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

/**
 * Mapper which maps feed definition to oozie workflow definitions for
 * replication & retention.
 */
public class OozieFeedMapper extends AbstractOozieEntityMapper<Feed> {

    private static final Logger LOG = Logger.getLogger(OozieFeedMapper.class);

    private final RetentionOozieWorkflowMapper retentionMapper = new RetentionOozieWorkflowMapper();
    private final ReplicationOozieWorkflowMapper replicationMapper = new ReplicationOozieWorkflowMapper();

    public OozieFeedMapper(Feed feed) {
        super(feed);
    }

    @Override
    protected List<COORDINATORAPP> getCoordinators(Cluster cluster, Path bundlePath) throws FalconException {
        List<COORDINATORAPP> coords = new ArrayList<COORDINATORAPP>();
        COORDINATORAPP retentionCoord = getRetentionCoordinator(cluster, bundlePath);
        if (retentionCoord != null) {
            coords.add(retentionCoord);
        }
        List<COORDINATORAPP> replicationCoords = getReplicationCoordinators(cluster, bundlePath);
        coords.addAll(replicationCoords);
        return coords;
    }

    private COORDINATORAPP getRetentionCoordinator(Cluster cluster, Path bundlePath) throws FalconException {

        Feed feed = getEntity();
        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(feed, cluster.getName());

        if (feedCluster.getValidity().getEnd().before(new Date())) {
            LOG.warn("Feed Retention is not applicable as Feed's end time for cluster " + cluster.getName()
                    + " is not in the future");
            return null;
        }

        return retentionMapper.getRetentionCoordinator(cluster, bundlePath, feed, feedCluster);
    }

    private List<COORDINATORAPP> getReplicationCoordinators(Cluster targetCluster, Path bundlePath)
        throws FalconException {

        Feed feed = getEntity();
        List<COORDINATORAPP> replicationCoords = new ArrayList<COORDINATORAPP>();

        if (FeedHelper.getCluster(feed, targetCluster.getName()).getType() == ClusterType.TARGET) {
            String coordName = EntityUtil.getWorkflowName(Tag.REPLICATION, feed).toString();
            Path basePath = getCoordPath(bundlePath, coordName);
            replicationMapper.createReplicatonWorkflow(targetCluster, basePath, coordName);

            for (org.apache.falcon.entity.v0.feed.Cluster feedCluster : feed.getClusters().getClusters()) {
                if (feedCluster.getType() == ClusterType.SOURCE) {
                    COORDINATORAPP coord = replicationMapper.createAndGetCoord(feed,
                            (Cluster) ConfigurationStore.get().get(EntityType.CLUSTER, feedCluster.getName()),
                            targetCluster, bundlePath);

                    if (coord != null) {
                        replicationCoords.add(coord);
                    }
                }
            }
        }

        return replicationCoords;
    }

    @Override
    protected Map<String, String> getEntityProperties() {
        Feed feed = getEntity();
        Map<String, String> props = new HashMap<String, String>();
        if (feed.getProperties() != null) {
            for (Property prop : feed.getProperties().getProperties()) {
                props.put(prop.getName(), prop.getValue());
            }
        }
        return props;
    }

    private final class RetentionOozieWorkflowMapper {

        private static final String RETENTION_WF_TEMPLATE = "/config/workflow/retention-workflow.xml";

        private COORDINATORAPP getRetentionCoordinator(Cluster cluster, Path bundlePath, Feed feed,
                                                       org.apache.falcon.entity.v0.feed.Cluster feedCluster)
            throws FalconException {

            COORDINATORAPP retentionApp = new COORDINATORAPP();
            String coordName = EntityUtil.getWorkflowName(Tag.RETENTION, feed).toString();
            retentionApp.setName(coordName);
            retentionApp.setEnd(SchemaHelper.formatDateUTC(feedCluster.getValidity().getEnd()));
            retentionApp.setStart(SchemaHelper.formatDateUTC(new Date()));
            retentionApp.setTimezone(feed.getTimezone().getID());
            TimeUnit timeUnit = feed.getFrequency().getTimeUnit();
            if (timeUnit == TimeUnit.hours || timeUnit == TimeUnit.minutes) {
                retentionApp.setFrequency("${coord:hours(6)}");
            } else {
                retentionApp.setFrequency("${coord:days(1)}");
            }

            Path wfPath = getCoordPath(bundlePath, coordName);
            retentionApp.setAction(getRetentionWorkflowAction(cluster, wfPath, coordName));
            return retentionApp;
        }

        private ACTION getRetentionWorkflowAction(Cluster cluster, Path wfPath, String wfName)
            throws FalconException {
            Feed feed = getEntity();
            ACTION retentionAction = new ACTION();
            WORKFLOW retentionWorkflow = new WORKFLOW();
            createRetentionWorkflow(cluster, wfPath, wfName);
            retentionWorkflow.setAppPath(getStoragePath(wfPath.toString()));

            Map<String, String> props = createCoordDefaultConfiguration(cluster, wfPath, wfName);
            props.put("timeZone", feed.getTimezone().getID());
            props.put("frequency", feed.getFrequency().getTimeUnit().name());

            final Storage storage = FeedHelper.createStorage(cluster, feed);
            props.put("falconFeedStorageType", storage.getType().name());

            String feedDataPath = storage.getUriTemplate();
            props.put("feedDataPath",
                    feedDataPath.replaceAll(Storage.DOLLAR_EXPR_START_REGEX, Storage.QUESTION_EXPR_START_REGEX));

            org.apache.falcon.entity.v0.feed.Cluster feedCluster =
                    FeedHelper.getCluster(feed, cluster.getName());
            props.put("limit", feedCluster.getRetention().getLimit().toString());

            props.put(ARG.operation.getPropName(), EntityOps.DELETE.name());
            props.put(ARG.feedNames.getPropName(), feed.getName());
            props.put(ARG.feedInstancePaths.getPropName(), "IGNORE");

            retentionWorkflow.setConfiguration(getCoordConfig(props));
            retentionAction.setWorkflow(retentionWorkflow);

            return retentionAction;
        }

        private void createRetentionWorkflow(Cluster cluster, Path wfPath, String wfName) throws FalconException {
            try {
                WORKFLOWAPP retWfApp = getWorkflowTemplate(RETENTION_WF_TEMPLATE);
                retWfApp.setName(wfName);
                addLibExtensionsToWorkflow(cluster, retWfApp, EntityType.FEED, "retention");
                addOozieRetries(retWfApp);
                marshal(cluster, retWfApp, wfPath);
            } catch(IOException e) {
                throw new FalconException("Unable to create retention workflow", e);
            }
        }
    }

    private class ReplicationOozieWorkflowMapper {
        private static final int THIRTY_MINUTES = 30 * 60 * 1000;

        private static final String REPLICATION_COORD_TEMPLATE = "/config/coordinator/replication-coordinator.xml";
        private static final String REPLICATION_WF_TEMPLATE = "/config/workflow/replication-workflow.xml";

        private static final String TIMEOUT = "timeout";
        private static final String PARALLEL = "parallel";

        private void createReplicatonWorkflow(Cluster cluster, Path wfPath, String wfName)
            throws FalconException {
            try {
                WORKFLOWAPP repWFapp = getWorkflowTemplate(REPLICATION_WF_TEMPLATE);
                repWFapp.setName(wfName);
                addLibExtensionsToWorkflow(cluster, repWFapp, EntityType.FEED, "replication");
                addOozieRetries(repWFapp);
                marshal(cluster, repWFapp, wfPath);
            } catch(IOException e) {
                throw new FalconException("Unable to create replication workflow", e);
            }

        }

        private COORDINATORAPP createAndGetCoord(Feed feed, Cluster srcCluster, Cluster trgCluster,
                                                 Path bundlePath) throws FalconException {
            long replicationDelayInMillis = getReplicationDelayInMillis(feed, srcCluster);
            Date sourceStartDate = getStartDate(feed, srcCluster, replicationDelayInMillis);
            Date sourceEndDate = getEndDate(feed, srcCluster);

            Date targetStartDate = getStartDate(feed, trgCluster, replicationDelayInMillis);
            Date targetEndDate = getEndDate(feed, trgCluster);

            if (noOverlapExists(sourceStartDate, sourceEndDate,
                    targetStartDate, targetEndDate)) {
                LOG.warn("Not creating replication coordinator, as the source cluster:"
                        + srcCluster.getName()
                        + " and target cluster: "
                        + trgCluster.getName()
                        + " do not have overlapping dates");
                return null;
            }

            COORDINATORAPP replicationCoord;
            try {
                replicationCoord = getCoordinatorTemplate(REPLICATION_COORD_TEMPLATE);
            } catch (FalconException e) {
                throw new FalconException("Cannot unmarshall replication coordinator template", e);
            }

            String coordName = EntityUtil.getWorkflowName(
                    Tag.REPLICATION, Arrays.asList(srcCluster.getName()), feed).toString();
            String start = sourceStartDate.after(targetStartDate)
                    ? SchemaHelper.formatDateUTC(sourceStartDate) : SchemaHelper.formatDateUTC(targetStartDate);
            String end = sourceEndDate.before(targetEndDate)
                    ? SchemaHelper.formatDateUTC(sourceEndDate) : SchemaHelper.formatDateUTC(targetEndDate);

            initializeCoordAttributes(replicationCoord, coordName, feed, start, end, replicationDelayInMillis);
            setCoordControls(feed, replicationCoord);

            final Storage sourceStorage = FeedHelper.createReadOnlyStorage(srcCluster, feed);
            initializeInputDataSet(feed, srcCluster, replicationCoord, sourceStorage);

            final Storage targetStorage = FeedHelper.createStorage(trgCluster, feed);
            initializeOutputDataSet(feed, trgCluster, replicationCoord, targetStorage);

            Path wfPath = getCoordPath(bundlePath, coordName);
            ACTION replicationWorkflowAction = getReplicationWorkflowAction(
                    srcCluster, trgCluster, wfPath, coordName, sourceStorage, targetStorage);
            replicationCoord.setAction(replicationWorkflowAction);

            return replicationCoord;
        }

        private Date getStartDate(Feed feed, Cluster cluster, long replicationDelayInMillis) {
            Date startDate = FeedHelper.getCluster(feed, cluster.getName()).getValidity().getStart();
            return replicationDelayInMillis == 0 ? startDate : new Date(startDate.getTime() + replicationDelayInMillis);
        }

        private Date getEndDate(Feed feed, Cluster cluster) {
            return FeedHelper.getCluster(feed, cluster.getName()).getValidity().getEnd();
        }

        private boolean noOverlapExists(Date sourceStartDate, Date sourceEndDate,
                                        Date targetStartDate, Date targetEndDate) {
            return sourceStartDate.after(targetEndDate) || targetStartDate.after(sourceEndDate);
        }

        private void initializeCoordAttributes(COORDINATORAPP replicationCoord, String coordName,
                                               Feed feed, String start, String end, long delayInMillis) {
            replicationCoord.setName(coordName);
            replicationCoord.setFrequency("${coord:" + feed.getFrequency().toString() + "}");

            if (delayInMillis > 0) {
                long delayInMins = -1 * delayInMillis / (1000 * 60);
                String elExp = "${now(0," + delayInMins + ")}";

                replicationCoord.getInputEvents().getDataIn().get(0).getInstance().set(0, elExp);
                replicationCoord.getOutputEvents().getDataOut().get(0).setInstance(elExp);
            }

            replicationCoord.setStart(start);
            replicationCoord.setEnd(end);
            replicationCoord.setTimezone(feed.getTimezone().getID());
        }

        private long getReplicationDelayInMillis(Feed feed, Cluster srcCluster) throws FalconException {
            Frequency replicationDelay = FeedHelper.getCluster(feed, srcCluster.getName()).getDelay();
            long delayInMillis=0;
            if (replicationDelay != null) {
                delayInMillis = ExpressionHelper.get().evaluate(
                        replicationDelay.toString(), Long.class);
            }

            return delayInMillis;
        }

        private void setCoordControls(Feed feed, COORDINATORAPP replicationCoord) throws FalconException {
            long frequencyInMillis = ExpressionHelper.get().evaluate(
                    feed.getFrequency().toString(), Long.class);
            long timeoutInMillis = frequencyInMillis * 6;
            if (timeoutInMillis < THIRTY_MINUTES) {
                timeoutInMillis = THIRTY_MINUTES;
            }

            Map<String, String> props = getEntityProperties();
            String timeout = props.get(TIMEOUT);
            if (timeout!=null) {
                try{
                    timeoutInMillis= ExpressionHelper.get().evaluate(timeout, Long.class);
                } catch (Exception ignore) {
                    LOG.error("Unable to evaluate timeout:", ignore);
                }
            }
            replicationCoord.getControls().setTimeout(String.valueOf(timeoutInMillis / (1000 * 60)));
            replicationCoord.getControls().setThrottle(String.valueOf(timeoutInMillis / frequencyInMillis * 2));

            String parallelProp = props.get(PARALLEL);
            int parallel = 1;
            if (parallelProp != null) {
                try {
                    parallel = Integer.parseInt(parallelProp);
                } catch (NumberFormatException ignore) {
                    LOG.error("Unable to parse parallel:", ignore);
                }
            }
            replicationCoord.getControls().setConcurrency(String.valueOf(parallel));
        }

        private void initializeInputDataSet(Feed feed, Cluster srcCluster, COORDINATORAPP replicationCoord,
                                            Storage sourceStorage) throws FalconException {
            SYNCDATASET inputDataset = (SYNCDATASET)
                    replicationCoord.getDatasets().getDatasetOrAsyncDataset().get(0);

            String uriTemplate = sourceStorage.getUriTemplate(LocationType.DATA);
            if (sourceStorage.getType() == Storage.TYPE.TABLE) {
                uriTemplate = uriTemplate.replace("thrift", "hcat"); // Oozie requires this!!!
            }
            inputDataset.setUriTemplate(uriTemplate);

            setDatasetValues(inputDataset, feed, srcCluster);

            if (feed.getAvailabilityFlag() == null) {
                inputDataset.setDoneFlag("");
            } else {
                inputDataset.setDoneFlag(feed.getAvailabilityFlag());
            }
        }

        private void initializeOutputDataSet(Feed feed, Cluster targetCluster, COORDINATORAPP replicationCoord,
                                             Storage targetStorage) throws FalconException {
            SYNCDATASET outputDataset = (SYNCDATASET)
                    replicationCoord.getDatasets().getDatasetOrAsyncDataset().get(1);

            String uriTemplate = targetStorage.getUriTemplate(LocationType.DATA);
            if (targetStorage.getType() == Storage.TYPE.TABLE) {
                uriTemplate = uriTemplate.replace("thrift", "hcat"); // Oozie requires this!!!
            }
            outputDataset.setUriTemplate(uriTemplate);

            setDatasetValues(outputDataset, feed, targetCluster);
        }

        private void setDatasetValues(SYNCDATASET dataset, Feed feed, Cluster cluster) {
            dataset.setInitialInstance(SchemaHelper.formatDateUTC(
                    FeedHelper.getCluster(feed, cluster.getName()).getValidity().getStart()));
            dataset.setTimezone(feed.getTimezone().getID());
            dataset.setFrequency("${coord:" + feed.getFrequency().toString() + "}");
        }

        private ACTION getReplicationWorkflowAction(Cluster srcCluster, Cluster trgCluster, Path wfPath,
                                                    String wfName, Storage sourceStorage,
                                                    Storage targetStorage) throws FalconException {
            ACTION replicationAction = new ACTION();
            WORKFLOW replicationWF = new WORKFLOW();
            try {
                replicationWF.setAppPath(getStoragePath(wfPath.toString()));
                Feed feed = getEntity();

                Map<String, String> props = createCoordDefaultConfiguration(trgCluster, wfPath, wfName);
                props.put("srcClusterName", srcCluster.getName());
                props.put("srcClusterColo", srcCluster.getColo());

                // the storage type is uniform across source and target feeds for replication
                props.put("falconFeedStorageType", sourceStorage.getType().name());

                String instancePaths = null;
                if (sourceStorage.getType() == Storage.TYPE.FILESYSTEM) {
                    String pathsWithPartitions = getPathsWithPartitions(srcCluster, trgCluster, feed);
                    instancePaths = pathsWithPartitions;

                    propagateFileSystemCopyProperties(pathsWithPartitions, props);
                } else if (sourceStorage.getType() == Storage.TYPE.TABLE) {
                    instancePaths = "${coord:dataIn('input')}";

                    final CatalogStorage sourceTableStorage = (CatalogStorage) sourceStorage;
                    propagateTableStorageProperties(srcCluster, sourceTableStorage, props, "falconSource");
                    final CatalogStorage targetTableStorage = (CatalogStorage) targetStorage;
                    propagateTableStorageProperties(trgCluster, targetTableStorage, props, "falconTarget");
                    propagateTableCopyProperties(srcCluster, sourceTableStorage,
                            trgCluster, targetTableStorage, props);
                    setupHiveConfiguration(srcCluster, sourceTableStorage, trgCluster, targetTableStorage, wfPath);
                }

                propagateLateDataProperties(feed, instancePaths, sourceStorage.getType().name(), props);

                replicationWF.setConfiguration(getCoordConfig(props));
                replicationAction.setWorkflow(replicationWF);

            } catch (Exception e) {
                throw new FalconException("Unable to create replication workflow", e);
            }

            return replicationAction;
        }

        private String getPathsWithPartitions(Cluster srcCluster, Cluster trgCluster,
                                              Feed feed) throws FalconException {
            String srcPart = FeedHelper.normalizePartitionExpression(
                    FeedHelper.getCluster(feed, srcCluster.getName()).getPartition());
            srcPart = FeedHelper.evaluateClusterExp(srcCluster, srcPart);

            String targetPart = FeedHelper.normalizePartitionExpression(
                    FeedHelper.getCluster(feed, trgCluster.getName()).getPartition());
            targetPart = FeedHelper.evaluateClusterExp(trgCluster, targetPart);

            StringBuilder pathsWithPartitions = new StringBuilder();
            pathsWithPartitions.append("${coord:dataIn('input')}/")
                    .append(FeedHelper.normalizePartitionExpression(srcPart, targetPart));

            String parts = pathsWithPartitions.toString().replaceAll("//+", "/");
            parts = StringUtils.stripEnd(parts, "/");
            return parts;
        }

        private void propagateFileSystemCopyProperties(String pathsWithPartitions,
                                                       Map<String, String> props) throws FalconException {
            props.put("sourceRelativePaths", pathsWithPartitions);

            props.put("distcpSourcePaths", "${coord:dataIn('input')}");
            props.put("distcpTargetPaths", "${coord:dataOut('output')}");
        }

        private void propagateTableStorageProperties(Cluster cluster, CatalogStorage tableStorage,
                                                     Map<String, String> props, String prefix) {
            props.put(prefix + "NameNode", ClusterHelper.getStorageUrl(cluster));
            props.put(prefix + "JobTracker", ClusterHelper.getMREndPoint(cluster));
            props.put(prefix + "HcatNode", tableStorage.getCatalogUrl());

            props.put(prefix + "Database", tableStorage.getDatabase());
            props.put(prefix + "Table", tableStorage.getTable());
            props.put(prefix + "Partition", "${coord:dataInPartitionFilter('input', 'hive')}");
        }

        private void setupHiveConfiguration(Cluster srcCluster, CatalogStorage sourceStorage,
                                            Cluster trgCluster, CatalogStorage targetStorage, Path wfPath)
            throws IOException, FalconException {
            Configuration conf = ClusterHelper.getConfiguration(trgCluster);
            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(conf);

            // copy import export scripts to stagingDir
            Path scriptPath = new Path(wfPath, "scripts");
            copyHiveScript(fs, scriptPath, "/config/workflow/", "falcon-table-export.hql");
            copyHiveScript(fs, scriptPath, "/config/workflow/", "falcon-table-import.hql");

            // create hive conf to stagingDir
            Path confPath = new Path(wfPath + "/conf");
            createHiveConf(fs, confPath, sourceStorage.getCatalogUrl(), srcCluster, "falcon-source-");
            createHiveConf(fs, confPath, targetStorage.getCatalogUrl(), trgCluster, "falcon-target-");
        }

        private void copyHiveScript(FileSystem fs, Path scriptPath,
                                    String localScriptPath, String scriptName) throws IOException {
            OutputStream out = null;
            InputStream in = null;
            try {
                out = fs.create(new Path(scriptPath, scriptName));
                in = OozieFeedMapper.class.getResourceAsStream(localScriptPath + scriptName);
                IOUtils.copy(in, out);
            } finally {
                IOUtils.closeQuietly(in);
                IOUtils.closeQuietly(out);
            }
        }

        private void propagateTableCopyProperties(Cluster srcCluster, CatalogStorage sourceStorage,
                                                  Cluster trgCluster, CatalogStorage targetStorage,
                                                  Map<String, String> props) {
            // create staging dirs for export at source & set it as distcpSourcePaths
            String sourceDatedPartitionKey = sourceStorage.getDatedPartitionKey();
            String sourceStagingDir =
                    FeedHelper.getStagingDir(srcCluster, getEntity(), sourceStorage, Tag.REPLICATION)
                    + "/" + sourceDatedPartitionKey
                    + "=${coord:dataOutPartitionValue('output', '" + sourceDatedPartitionKey + "')}";
            props.put("distcpSourcePaths", sourceStagingDir + "/" + NOMINAL_TIME_EL + "/data");

            // create staging dirs for import at target & set it as distcpTargetPaths
            String targetDatedPartitionKey = targetStorage.getDatedPartitionKey();
            String targetStagingDir =
                    FeedHelper.getStagingDir(trgCluster, getEntity(), targetStorage, Tag.REPLICATION)
                    + "/" + targetDatedPartitionKey
                    + "=${coord:dataOutPartitionValue('output', '" + targetDatedPartitionKey + "')}";
            props.put("distcpTargetPaths", targetStagingDir + "/" + NOMINAL_TIME_EL + "/data");

            props.put("sourceRelativePaths", "IGNORE"); // this will bot be used for Table storage.
        }

        private void propagateLateDataProperties(Feed feed, String instancePaths,
                                                 String falconFeedStorageType, Map<String, String> props) {
            // todo these pairs are the same but used in different context
            // late data handler - should-record action
            props.put("falconInputFeeds", feed.getName());
            props.put("falconInPaths", instancePaths);

            // storage type for each corresponding feed - in this case only one feed is involved
            // needed to compute usage based on storage type in LateDataHandler
            props.put("falconInputFeedStorageTypes", falconFeedStorageType);

            // falcon post processing
            props.put(ARG.feedNames.getPropName(), feed.getName());
            props.put(ARG.feedInstancePaths.getPropName(), instancePaths);
        }
    }

    private void addOozieRetries(WORKFLOWAPP workflow) {
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
}
