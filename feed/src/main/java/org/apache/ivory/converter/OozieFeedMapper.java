/*
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

package org.apache.ivory.converter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.Tag;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.FeedHelper;
import org.apache.ivory.entity.parser.FeedEntityParser;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.Frequency.TimeUnit;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.feed.ClusterType;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.feed.LocationType;
import org.apache.ivory.entity.v0.feed.Property;
import org.apache.ivory.messaging.EntityInstanceMessage.ARG;
import org.apache.ivory.messaging.EntityInstanceMessage.EntityOps;
import org.apache.ivory.oozie.coordinator.ACTION;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.apache.ivory.oozie.coordinator.SYNCDATASET;
import org.apache.ivory.oozie.coordinator.WORKFLOW;
import org.apache.ivory.oozie.workflow.WORKFLOWAPP;
import org.apache.log4j.Logger;

public class OozieFeedMapper extends AbstractOozieEntityMapper<Feed> {

    private static Logger LOG = Logger.getLogger(OozieFeedMapper.class);

    private static final String RETENTION_WF_TEMPLATE = "/config/workflow/retention-workflow.xml";
    private static final String REPLICATION_COORD_TEMPLATE = "/config/coordinator/replication-coordinator.xml";
    private static final String REPLICATION_WF_TEMPLATE = "/config/workflow/replication-workflow.xml";

    public OozieFeedMapper(Feed feed) {
        super(feed);
    }

    @Override
    protected List<COORDINATORAPP> getCoordinators(Cluster cluster, Path bundlePath) throws IvoryException {
        List<COORDINATORAPP> coords = new ArrayList<COORDINATORAPP>();
        COORDINATORAPP retentionCoord = getRetentionCoordinator(cluster, bundlePath);
        if (retentionCoord != null) {
            coords.add(retentionCoord);
        }
        List<COORDINATORAPP> replicationCoords = getReplicationCoordinators(cluster, bundlePath);
        coords.addAll(replicationCoords);
        return coords;
    }

    private COORDINATORAPP getRetentionCoordinator(Cluster cluster, Path bundlePath) throws IvoryException {

        Feed feed = getEntity();
        org.apache.ivory.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(feed, cluster.getName());

        if (EntityUtil.parseDateUTC(feedCluster.getValidity().getEnd()).before(new Date())) {
            LOG.warn("Feed Retention is not applicable as Feed's end time for cluster " + cluster.getName() + " is not in the future");
            return null;
        }
        COORDINATORAPP retentionApp = new COORDINATORAPP();
        String coordName = EntityUtil.getWorkflowName(Tag.RETENTION, feed).toString();
        retentionApp.setName(coordName);
        retentionApp.setEnd(feedCluster.getValidity().getEnd());
        retentionApp.setStart(EntityUtil.formatDateUTC(new Date()));
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

    private ACTION getRetentionWorkflowAction(Cluster cluster, Path wfPath, String wfName) throws IvoryException {
        Feed feed = getEntity();
        ACTION retentionAction = new ACTION();
        WORKFLOW retentionWorkflow = new WORKFLOW();
        try {
            //
            WORKFLOWAPP retWfApp = createRetentionWorkflow(cluster);
            retWfApp.setName(wfName);
            marshal(cluster, retWfApp, wfPath);
            retentionWorkflow.setAppPath(getHDFSPath(wfPath.toString()));

            Map<String, String> props = createCoordDefaultConfiguration(cluster, wfPath, wfName);

            org.apache.ivory.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(feed, cluster.getName());
            String feedPathMask = FeedHelper.getLocation(feed, LocationType.DATA).getPath();

            props.put("feedDataPath", feedPathMask.replaceAll("\\$\\{", "\\?\\{"));
            props.put("timeZone", feed.getTimezone().getID());
            props.put("frequency", feed.getFrequency().getTimeUnit().name());
            props.put("limit", feedCluster.getRetention().getLimit().toString());
            props.put(ARG.operation.getPropName(), EntityOps.DELETE.name());
            props.put(ARG.feedNames.getPropName(), feed.getName());
            props.put(ARG.feedInstancePaths.getPropName(), "IGNORE");

            retentionWorkflow.setConfiguration(getCoordConfig(props));
            retentionAction.setWorkflow(retentionWorkflow);
            return retentionAction;
        } catch (Exception e) {
            throw new IvoryException("Unable to create parent/retention workflow", e);
        }
    }

    private List<COORDINATORAPP> getReplicationCoordinators(Cluster targetCluster, Path bundlePath) throws IvoryException {
        Feed feed = getEntity();
        List<COORDINATORAPP> replicationCoords = new ArrayList<COORDINATORAPP>();
        if (FeedHelper.getCluster(feed, targetCluster.getName()).getType().equals(ClusterType.TARGET)) {
            String coordName = EntityUtil.getWorkflowName(Tag.REPLICATION, feed).toString();
            Path basePath = getCoordPath(bundlePath, coordName);
            createReplicatonWorkflow(targetCluster, basePath, coordName);
            for (org.apache.ivory.entity.v0.feed.Cluster feedCluster : feed.getClusters().getClusters()) {
                if (feedCluster.getType().equals(ClusterType.SOURCE)) {
                    COORDINATORAPP coord = createAndGetCoord(feed,
                            (Cluster) ConfigurationStore.get().get(EntityType.CLUSTER, feedCluster.getName()), targetCluster,
                            bundlePath);
                    replicationCoords.add(coord);

                }
            }

        }
        return replicationCoords;
    }

    private COORDINATORAPP createAndGetCoord(Feed feed, Cluster srcCluster, Cluster trgCluster, Path bundlePath)
            throws IvoryException {
        COORDINATORAPP replicationCoord;
        String coordName;
        try {
            replicationCoord = getCoordinatorTemplate(REPLICATION_COORD_TEMPLATE);
            coordName = EntityUtil.getWorkflowName(Tag.REPLICATION, Arrays.asList(srcCluster.getName()), feed).toString();
            replicationCoord.setName(coordName);
            replicationCoord.setFrequency("${coord:" + feed.getFrequency().toString() + "}");
            Date srcStartDate = EntityUtil.parseDateUTC(FeedHelper.getCluster(feed, srcCluster.getName()).getValidity().getStart());
            Date srcEndDate = EntityUtil.parseDateUTC(FeedHelper.getCluster(feed, srcCluster.getName()).getValidity().getEnd());
            Date trgStartDate = EntityUtil.parseDateUTC(FeedHelper.getCluster(feed, trgCluster.getName()).getValidity().getStart());
            Date trgEndDate = EntityUtil.parseDateUTC(FeedHelper.getCluster(feed, trgCluster.getName()).getValidity().getEnd());
            replicationCoord.setStart(srcStartDate.after(trgStartDate) ? EntityUtil.formatDateUTC(srcStartDate) : EntityUtil
                    .formatDateUTC(trgStartDate));
            replicationCoord.setEnd(srcEndDate.before(trgEndDate) ? EntityUtil.formatDateUTC(srcEndDate) : EntityUtil
                    .formatDateUTC(trgEndDate));
            replicationCoord.setTimezone(feed.getTimezone().getID());
            SYNCDATASET inputDataset = (SYNCDATASET) replicationCoord.getDatasets().getDatasetOrAsyncDataset().get(0);
            SYNCDATASET outputDataset = (SYNCDATASET) replicationCoord.getDatasets().getDatasetOrAsyncDataset().get(1);

			inputDataset.setUriTemplate(new Path(ClusterHelper
					.getHdfsUrl(srcCluster), FeedHelper.getLocation(feed,
					LocationType.DATA).getPath()).toString());
            outputDataset.setUriTemplate(getHDFSPath(FeedHelper.getLocation(feed, LocationType.DATA).getPath()));
            setDatasetValues(inputDataset, feed, srcCluster);
            setDatasetValues(outputDataset, feed, srcCluster);
            if (feed.getAvailabilityFlag() == null) {
                inputDataset.setDoneFlag("");
            } else {
                inputDataset.setDoneFlag(feed.getAvailabilityFlag());
            }

        } catch (IvoryException e) {
            throw new IvoryException("Cannot unmarshall replication coordinator template", e);
        }

        Path wfPath = getCoordPath(bundlePath, coordName);
        replicationCoord.setAction(getReplicationWorkflowAction(srcCluster, trgCluster, wfPath, coordName));
        return replicationCoord;
    }

    private void setDatasetValues(SYNCDATASET dataset, Feed feed, Cluster cluster) {
        dataset.setInitialInstance(FeedHelper.getCluster(feed, cluster.getName()).getValidity().getStart());
        dataset.setTimezone(feed.getTimezone().getID());
        dataset.setFrequency("${coord:" + feed.getFrequency().toString() + "}");
    }

    private ACTION getReplicationWorkflowAction(Cluster srcCluster, Cluster trgCluster, Path wfPath, String wfName) throws IvoryException {
        ACTION replicationAction = new ACTION();
        WORKFLOW replicationWF = new WORKFLOW();
        try {
            replicationWF.setAppPath(getHDFSPath(wfPath.toString()));
            Feed feed = getEntity();
            StringBuilder pathsWithPartitions = new StringBuilder();
            pathsWithPartitions.append("${coord:dataIn('input')}").append(
                    FeedHelper.getCluster(feed, srcCluster.getName()).getPartition() == null ? "" : "/"+FeedEntityParser.getPartitionExpValue(
                            srcCluster, FeedHelper.getCluster(feed, srcCluster.getName()).getPartition()));

            Map<String, String> props = createCoordDefaultConfiguration(trgCluster, wfPath, wfName);
            props.put("srcClusterName", srcCluster.getName());
            props.put("srcClusterColo", srcCluster.getColo());
            props.put(ARG.feedNames.getPropName(), feed.getName());
            props.put(ARG.feedInstancePaths.getPropName(), pathsWithPartitions.toString());
            props.put("sourceRelativePaths", pathsWithPartitions.toString());
            props.put("distcpSourcePaths", "${coord:dataOut('input')}");
            props.put("distcpTargetPaths", "${coord:dataOut('output')}");
            replicationWF.setConfiguration(getCoordConfig(props));
            replicationAction.setWorkflow(replicationWF);
        } catch (Exception e) {
            throw new IvoryException("Unable to create replication workflow", e);
        }
        return replicationAction;

    }

    private void createReplicatonWorkflow(Cluster cluster, Path wfPath, String wfName) throws IvoryException {
        WORKFLOWAPP repWFapp = getWorkflowTemplate(REPLICATION_WF_TEMPLATE);
        repWFapp.setName(wfName);
        marshal(cluster, repWFapp, wfPath);
    }

    private WORKFLOWAPP createRetentionWorkflow(Cluster cluster) throws IOException, IvoryException {
        return getWorkflowTemplate(RETENTION_WF_TEMPLATE);
    }

    @Override
    protected Map<String, String> getEntityProperties() {
        Feed feed = getEntity();
        Map<String, String> props = new HashMap<String, String>();
        if (feed.getProperties() != null) {
            for (Property prop : feed.getProperties().getProperties())
                props.put(prop.getName(), prop.getValue());
        }
        return props;
    }

}
