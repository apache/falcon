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
import org.apache.ivory.oozie.coordinator.ACTION;
import org.apache.ivory.oozie.coordinator.CONFIGURATION;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.apache.ivory.oozie.coordinator.WORKFLOW;
import org.apache.ivory.workflow.engine.OozieWorkflowEngine;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

public class OozieFeedMapper extends AbstractOozieEntityMapper<Feed> {

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

        COORDINATORAPP retentionApp = new COORDINATORAPP();

        Feed feed = getEntity();
        retentionApp.setName(feed.getWorkflowName() + "_RETENTION");
        org.apache.ivory.entity.v0.feed.Cluster feedCluster =
                feed.getCluster(cluster.getName());
        retentionApp.setEnd(feedCluster.getValidity().getEnd());
        retentionApp.setStart(feedCluster.getValidity().getStart());
        retentionApp.setTimezone(feedCluster.getValidity().getTimezone());
        if (feed.getFrequency().matches("hours|minutes")) {
            retentionApp.setFrequency("${coord:hours(6)}");
        } else {
            retentionApp.setFrequency("${coord:days(1)}");
        }

        retentionApp.setAction(getRetentionAction(cluster));
        return retentionApp;
    }

    private ACTION getRetentionAction(Cluster cluster) throws IvoryException{
        ACTION retentionAction = new ACTION();
        WORKFLOW retentionWorkflow = new WORKFLOW();
        try {
            Path retentionWorkflowAppPath = createRetentionWorkflow(cluster);
            retentionWorkflow.setAppPath("${" + OozieWorkflowEngine.NAME_NODE + "}" +
                    retentionWorkflowAppPath.toString());
            retentionWorkflow.setConfiguration(new CONFIGURATION());
            retentionAction.setWorkflow(retentionWorkflow);
            return retentionAction;
        } catch (IOException e) {
            throw new IvoryException("Unable to create retention workflow", e);
        }
    }

    private Path createRetentionWorkflow(Cluster cluster) throws IOException {
        Path outPath = new Path(ClusterHelper.getLocation(cluster, "staging"),
                "ivory/system/retention-workflow.xml");
        if (!retentionUploaded) {
            InputStream in = getClass().getResourceAsStream("/retention-workflow.xml");
            FileSystem fs = FileSystem.get(ClusterHelper.getConfiguration(cluster));
            OutputStream out = fs.create(outPath);
            IOUtils.copyBytes(in, out, 4096, true);
            retentionUploaded = true;
        }
        return outPath;
    }
}
