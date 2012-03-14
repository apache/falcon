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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.feed.LocationType;
import org.apache.ivory.messaging.EntityInstanceMessage;
import org.apache.ivory.oozie.coordinator.ACTION;
import org.apache.ivory.oozie.coordinator.CONFIGURATION;
import org.apache.ivory.oozie.coordinator.CONFIGURATION.Property;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.apache.ivory.oozie.coordinator.WORKFLOW;
import org.apache.ivory.oozie.workflow.SUBWORKFLOW;
import org.apache.ivory.oozie.workflow.WORKFLOWAPP;
import org.apache.ivory.util.StartupProperties;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;

public class OozieFeedMapper extends AbstractOozieEntityMapper<Feed> {

    private static Logger LOG = Logger.getLogger(OozieFeedMapper.class);

    private static final Pattern pattern = Pattern.compile("ivory-common|ivory-feed|ivory-retention");

    private static final String RETENTION_WF_TEMPLATE = "/config/workflow/feed-parent-workflow.xml";

    private static volatile boolean retentionUploaded = false;

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
        retentionApp.setEnd(feedCluster.getValidity().getEnd());

        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        formatter.setTimeZone(TimeZone.getTimeZone(feedCluster.getValidity().getTimezone()));
        retentionApp.setStart(formatter.format(new Date()));
        retentionApp.setTimezone(feedCluster.getValidity().getTimezone());
        if (feed.getFrequency().matches("hours|minutes")) {
            retentionApp.setFrequency("${coord:hours(6)}");
        } else {
            retentionApp.setFrequency("${coord:days(1)}");
        }

        Path wfPath = getCoordPath(bundlePath, coordName);
        retentionApp.setAction(getRetentionWorkflowAction(cluster, wfPath));
        return retentionApp;
    }

    private ACTION getRetentionWorkflowAction(Cluster cluster, Path wfPath) throws IvoryException {
        Feed feed = getEntity();
        ACTION retentionAction = new ACTION();
        WORKFLOW parentWorkflow = new WORKFLOW();
        try {
            //
            WORKFLOWAPP retWfApp = createRetentionWorkflow(cluster);
            marshal(cluster, retWfApp, wfPath);
            parentWorkflow.setAppPath(getHDFSPath(wfPath.toString()));

            CONFIGURATION conf = createCoordDefaultConfiguration(cluster, wfPath);

            org.apache.ivory.entity.v0.feed.Cluster feedCluster = feed.getCluster(cluster.getName());
            String feedPathMask = feed.getLocations().get(LocationType.DATA).getPath();

            List<org.apache.ivory.oozie.coordinator.CONFIGURATION.Property> props = conf.getProperty();

            props.add(createCoordProperty(OozieClient.LIBPATH, getRetentionWorkflowPath(cluster) + "/lib"));
            props.add(createCoordProperty("feedDataPath", feedPathMask.replaceAll("\\$\\{", "\\?\\{")));
            props.add(createCoordProperty("timeZone", feedCluster.getValidity().getTimezone()));
            props.add(createCoordProperty("frequency", feed.getFrequency()));
            props.add(createCoordProperty("limit", feedCluster.getRetention().getLimit()));
            props.add(createCoordProperty(EntityInstanceMessage.ARG.OPERATION.NAME(),
                    EntityInstanceMessage.entityOperation.DELETE.name()));
            props.add(createCoordProperty(EntityInstanceMessage.ARG.FEED_NAME.NAME(), feed.getName()));
            props.add(createCoordProperty(EntityInstanceMessage.ARG.FEED_INSTANCE_PATH.NAME(), feed.getStagingPath()));
            props.add(createCoordProperty(EntityInstanceMessage.ARG.OPERATION.NAME(),
                    EntityInstanceMessage.entityOperation.DELETE.name()));

            parentWorkflow.setConfiguration(conf);
            retentionAction.setWorkflow(parentWorkflow);
            return retentionAction;
        } catch (Exception e) {
            throw new IvoryException("Unable to create parent/retention workflow", e);
        }
    }

    private WORKFLOWAPP createRetentionWorkflow(Cluster cluster) throws IOException, IvoryException {
        // Create sub workflow
        Path subWfPath = new Path(getRetentionWorkflowPath(cluster));
        if (!retentionUploaded) {
            InputStream in = getClass().getResourceAsStream("/retention-workflow.xml");
            FileSystem fs = FileSystem.get(ClusterHelper.getConfiguration(cluster));
            OutputStream out = fs.create(new Path(subWfPath, "workflow.xml"));
            IOUtils.copyBytes(in, out, 4096, true);
            if (LOG.isDebugEnabled()) {
                debug(subWfPath, fs);
            }
            String localLibPath = getLocalLibLocation();
            Path libLoc = new Path(subWfPath, "lib");
            fs.mkdirs(libLoc);
            for (File file : new File(localLibPath).listFiles()) {
                if (pattern.matcher(file.getName()).find()) {
                    LOG.debug("Copying " + file.getAbsolutePath() + " to " + libLoc);
                    fs.copyFromLocalFile(new Path(file.getAbsolutePath()), libLoc);
                }
            }
            retentionUploaded = true;
        }

        WORKFLOWAPP parentWorkflow = getWorkflowTemplate(RETENTION_WF_TEMPLATE);
        // set the subflow app path to users workflow
        SUBWORKFLOW userSubFlowAction = ((org.apache.ivory.oozie.workflow.ACTION) parentWorkflow.getDecisionOrForkOrJoin().get(1))
                .getSubWorkflow();

        userSubFlowAction.setAppPath(getHDFSPath(subWfPath.toString()));

        return parentWorkflow;
    }

    private String getLocalLibLocation() {
        String localLibPath = StartupProperties.get().getProperty("system.lib.location");
        if (localLibPath == null || localLibPath.isEmpty() || !new File(localLibPath).exists()) {
            LOG.error("Unable to copy libs: Invalid location " + localLibPath);
            throw new IllegalStateException("Invalid lib location " + localLibPath);
        }
        return localLibPath;
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
    protected List<Property> getEntityProperties() {
        Feed feed = getEntity();
        List<CONFIGURATION.Property> props = new ArrayList<CONFIGURATION.Property>();
        if (feed.getProperties() != null) {
            for (org.apache.ivory.entity.v0.feed.Property prop : feed.getProperties().values())
                props.add(createCoordProperty(prop.getName(), prop.getValue()));
        }
        return props;
    }
}