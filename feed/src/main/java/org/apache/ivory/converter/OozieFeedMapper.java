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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.feed.LocationType;
import org.apache.ivory.oozie.coordinator.ACTION;
import org.apache.ivory.oozie.coordinator.CONFIGURATION;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.apache.ivory.oozie.coordinator.WORKFLOW;
import org.apache.ivory.util.StartupProperties;
import org.apache.ivory.workflow.engine.OozieWorkflowEngine;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class OozieFeedMapper extends AbstractOozieEntityMapper<Feed> {

    private static Logger LOG = Logger.getLogger(OozieFeedMapper.class);

    private static final Pattern pattern = Pattern.compile("ivory-common|ivory-feed|ivory-retention");

    private static volatile boolean retentionUploaded = false;

    public OozieFeedMapper(Feed feed) {
        super(feed);
    }

    @Override
    protected List<COORDINATORAPP> getCoordinators(Cluster cluster)
            throws IvoryException {
        return Arrays.asList(getRetentionCoordinator(cluster));
    }

    private COORDINATORAPP getRetentionCoordinator(Cluster cluster)
            throws IvoryException{

        Feed feed = getEntity();
        String basePath = feed.getWorkflowName() + "_RETENTION";
        COORDINATORAPP retentionApp = newCOORDINATORAPP(basePath);

        retentionApp.setName(basePath + "_" + feed.getName());
        org.apache.ivory.entity.v0.feed.Cluster feedCluster =
                feed.getCluster(cluster.getName());
        retentionApp.setEnd(feedCluster.getValidity().getEnd());

        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        formatter.setTimeZone(TimeZone.
                getTimeZone(feedCluster.getValidity().getTimezone()));
        retentionApp.setStart(formatter.format(new Date()));
        retentionApp.setTimezone(feedCluster.getValidity().getTimezone());
        if (feed.getFrequency().matches("hours|minutes")) {
            retentionApp.setFrequency("${coord:hours(6)}");
        } else {
            retentionApp.setFrequency("${coord:days(1)}");
        }

        retentionApp.setAction(getRetentionAction(cluster, feed));
        return retentionApp;
    }

    private ACTION getRetentionAction(Cluster cluster, Feed feed) throws IvoryException{
        ACTION retentionAction = new ACTION();
        WORKFLOW retentionWorkflow = new WORKFLOW();
        try {
            Path retentionWorkflowAppPath = createRetentionWorkflow(cluster);
            retentionWorkflow.setAppPath("${" + OozieWorkflowEngine.NAME_NODE + "}" +
                    retentionWorkflowAppPath.toString());

            CONFIGURATION conf = new CONFIGURATION();
            org.apache.ivory.entity.v0.feed.Cluster feedCluster =
                    feed.getCluster(cluster.getName());

            conf.getProperty().add(createCoordProperty(OozieClient.LIBPATH,
                    new Path(retentionWorkflowAppPath, "lib").toString()));
            conf.getProperty().add(createCoordProperty("queueName", "default"));
            String feedPathMask = feed.getLocations().get(LocationType.DATA).getPath();
            conf.getProperty().add(createCoordProperty("feedDataPath",
                    feedPathMask.replaceAll("\\$\\{", "\\?\\{")));
            conf.getProperty().add(createCoordProperty("timeZone",
                    feedCluster.getValidity().getTimezone()));
            conf.getProperty().add(createCoordProperty("frequency",
                    feed.getFrequency()));
            conf.getProperty().add(createCoordProperty("limit",
                    feedCluster.getRetention().getLimit()));

            retentionWorkflow.setConfiguration(conf);

            retentionAction.setWorkflow(retentionWorkflow);
            return retentionAction;
        } catch (IOException e) {
            throw new IvoryException("Unable to create retention workflow", e);
        }
    }

    private Path createRetentionWorkflow(Cluster cluster) throws IOException {
        Path outPath = new Path(ClusterHelper.getLocation(cluster, "staging"),
                "ivory/system/retention");
        if (!retentionUploaded) {
            InputStream in = getClass().getResourceAsStream("/retention-workflow.xml");
            FileSystem fs = FileSystem.get(ClusterHelper.getConfiguration(cluster));
            OutputStream out = fs.create(new Path(outPath, "workflow.xml"));
            IOUtils.copyBytes(in, out, 4096, true);
            if (LOG.isDebugEnabled()) {
                debug(outPath, fs);
            }
            String localLibPath = getLocalLibLocation();
            Path libLoc =  new Path(outPath, "lib");
            fs.mkdirs(libLoc);
            for (File file : new File(localLibPath).listFiles()) {
                if (pattern.matcher(file.getName()).find()) {
                    LOG.debug("Copying " + file.getAbsolutePath() + " to " + libLoc);
                    fs.copyFromLocalFile(new Path(file.getAbsolutePath()), libLoc);
                }
            }
            retentionUploaded = true;
        }
        return outPath;
    }

    private String getLocalLibLocation() {
        String localLibPath = StartupProperties.get().getProperty("system.lib.location");
        if (localLibPath == null || localLibPath.isEmpty() ||
                !new File(localLibPath).exists()) {
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
}
