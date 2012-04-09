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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.feed.LocationType;
import org.apache.ivory.messaging.EntityInstanceMessage;
import org.apache.ivory.oozie.coordinator.ACTION;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.apache.ivory.oozie.coordinator.WORKFLOW;
import org.apache.ivory.oozie.workflow.WORKFLOWAPP;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;

public class OozieFeedMapper extends AbstractOozieEntityMapper<Feed> {

    private static Logger LOG = Logger.getLogger(OozieFeedMapper.class);

    private static final String RETENTION_WF_TEMPLATE = "/config/workflow/feed-parent-workflow.xml";

    public OozieFeedMapper(Feed feed) {
        super(feed);
    }

    @Override
    protected List<COORDINATORAPP> getCoordinators(Cluster cluster, Path bundlePath) throws IvoryException {
        return Arrays.asList(getRetentionCoordinator(cluster, bundlePath));
    }

    private COORDINATORAPP getRetentionCoordinator(Cluster cluster, Path bundlePath) throws IvoryException {

        Feed feed = getEntity();
        COORDINATORAPP retentionApp = new COORDINATORAPP();
        String coordName = feed.getWorkflowName("RETENTION");
        retentionApp.setName(coordName);
        org.apache.ivory.entity.v0.feed.Cluster feedCluster = feed.getCluster(cluster.getName());
        
        if(EntityUtil.parseDateUTC(feedCluster.getValidity().getEnd()).before(new Date()))
            throw new IvoryException("Feed's end time for cluster " + cluster.getName() + " should be in the future");
        
        retentionApp.setEnd(feedCluster.getValidity().getEnd());
        retentionApp.setStart(EntityUtil.formatDateUTC(new Date()));
        retentionApp.setTimezone(feedCluster.getValidity().getTimezone());
        if (feed.getFrequency().matches("hours|minutes")) {
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
            marshal(cluster, retWfApp, wfPath);
            retentionWorkflow.setAppPath(getHDFSPath(wfPath.toString()));

            Map<String, String> props = createCoordDefaultConfiguration(cluster, wfPath, wfName);

            org.apache.ivory.entity.v0.feed.Cluster feedCluster = feed.getCluster(cluster.getName());
            String feedPathMask = feed.getLocations().get(LocationType.DATA).getPath();

            props.put(OozieClient.LIBPATH, getRetentionWorkflowPath(cluster) + "/lib");
            props.put("feedDataPath", feedPathMask.replaceAll("\\$\\{", "\\?\\{"));
            props.put("timeZone", feedCluster.getValidity().getTimezone());
            props.put("frequency", feed.getFrequency());
            props.put("limit", feedCluster.getRetention().getLimit());
            props.put(EntityInstanceMessage.ARG.OPERATION.NAME(), EntityInstanceMessage.entityOperation.DELETE.name());
            props.put(EntityInstanceMessage.ARG.FEED_NAME.NAME(), feed.getName());
            props.put(EntityInstanceMessage.ARG.FEED_INSTANCE_PATH.NAME(), feed.getStagingPath());
            props.put(EntityInstanceMessage.ARG.OPERATION.NAME(), EntityInstanceMessage.entityOperation.DELETE.name());

            retentionWorkflow.setConfiguration(getCoordConfig(props));
            retentionAction.setWorkflow(retentionWorkflow);
            return retentionAction;
        } catch (Exception e) {
            throw new IvoryException("Unable to create parent/retention workflow", e);
        }
    }

    private WORKFLOWAPP createRetentionWorkflow(Cluster cluster) throws IOException, IvoryException {
        Path retentionWorkflowAppPath = new Path(getRetentionWorkflowPath(cluster));
        FileSystem fs = FileSystem.get(ClusterHelper.getConfiguration(cluster));
        Path workflowPath = new Path(retentionWorkflowAppPath, "workflow.xml");
        if (!fs.exists(workflowPath)) {
            InputStream in = getClass().getResourceAsStream("/retention-workflow.xml");
            OutputStream out = fs.create(workflowPath);
            IOUtils.copyBytes(in, out, 4096, true);
            if (LOG.isDebugEnabled()) {
                debug(retentionWorkflowAppPath, fs);
            }
            Path libLoc = new Path(retentionWorkflowAppPath, "lib");
            fs.mkdirs(libLoc);
        }

        return getWorkflowTemplate(RETENTION_WF_TEMPLATE);
    }

    private void debug(Path outPath, FileSystem fs) throws IOException {
        ByteArrayOutputStream writer = new ByteArrayOutputStream();
        InputStream xmlData = fs.open(new Path(outPath, "workflow.xml"));
        IOUtils.copyBytes(xmlData, writer, 4096, true);
        LOG.debug("Workflow xml copied to " + outPath + "/workflow.xml");
        LOG.debug(writer);
    }

    private String getRetentionWorkflowPath(Cluster cluster) {
        return ClusterHelper.getLocation(cluster, "staging") + "/ivory/system/retention";
    }

    @Override
    protected Map<String, String> getEntityProperties() {
        Feed feed = getEntity();
        Map<String, String> props = new HashMap<String, String>();
        if (feed.getProperties() != null) {
            for (org.apache.ivory.entity.v0.feed.Property prop : feed.getProperties().values())
                props.put(prop.getName(), prop.getValue());
        }
        return props;
    }
}