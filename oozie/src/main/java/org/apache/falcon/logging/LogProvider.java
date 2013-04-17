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
package org.apache.falcon.logging;

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.resource.InstancesResult.Instance;
import org.apache.falcon.resource.InstancesResult.InstanceAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClientException;
import org.mortbay.log.Log;

import java.io.IOException;

public final class LogProvider {
    private static final Logger LOG = Logger.getLogger(LogProvider.class);

    public Instance populateLogUrls(Entity entity, Instance instance,
                                    String runId) throws FalconException {

        Cluster clusterObj = (Cluster) ConfigurationStore.get().get(
                EntityType.CLUSTER, instance.cluster);
        String resolvedRunId = "-";
        try {
            FileSystem fs = FileSystem.get(
                    new Path(ClusterHelper.getStorageUrl(clusterObj)).toUri(),
                    new Configuration());
            resolvedRunId = getResolvedRunId(fs, clusterObj, entity, instance,
                    runId);
            // if runId param is not resolved, i.e job is killed or not started
            // or running
            if (resolvedRunId.equals("-")
                    && StringUtils.isEmpty(instance.logFile)) {
                instance.logFile = "-";
                return instance;
            }
            return populateActionLogUrls(fs, clusterObj, entity, instance,
                    resolvedRunId);
        } catch (IOException e) {
            LOG.warn("Exception while getting FS in LogProvider", e);
        } catch (Exception e) {
            LOG.warn("Exception in LogProvider while getting resolving run id",
                    e);
        }
        return instance;
    }

    public String getResolvedRunId(FileSystem fs, Cluster cluster,
                                   Entity entity, Instance instance, String runId)
            throws FalconException, IOException {
        if (StringUtils.isEmpty(runId)) {
            Path jobPath = new Path(ClusterHelper.getStorageUrl(cluster),
                    EntityUtil.getLogPath(cluster, entity) + "/job-"
                            + EntityUtil.fromUTCtoURIDate(instance.instance) + "/*");

            FileStatus[] runs = fs.globStatus(jobPath);
            if (runs.length > 0) {
                // this is the latest run, dirs are sorted in increasing
                // order of runs
                return runs[runs.length - 1].getPath().getName();
            } else {
                LOG.warn("No run dirs are available in logs dir:" + jobPath);
                return "-";
            }
        } else {
            Path jobPath = new Path(ClusterHelper.getStorageUrl(cluster),
                    EntityUtil.getLogPath(cluster, entity) + "/job-"
                            + EntityUtil.fromUTCtoURIDate(instance.instance) + "/"
                            + getFormatedRunId(runId));
            if (fs.exists(jobPath)) {
                return getFormatedRunId(runId);
            } else {
                Log.warn("No run dirs are available in logs dir:" + jobPath);
                return "-";
            }
        }

    }

    private Instance populateActionLogUrls(FileSystem fs, Cluster cluster,
                                           Entity entity, Instance instance, String formatedRunId)
            throws FalconException, OozieClientException, IOException {

        Path actionPaths = new Path(ClusterHelper.getStorageUrl(cluster),
                EntityUtil.getLogPath(cluster, entity) + "/job-"
                        + EntityUtil.fromUTCtoURIDate(instance.instance) + "/"
                        + formatedRunId + "/*");
        FileStatus[] actions = fs.globStatus(actionPaths);
        InstanceAction[] instanceActions = new InstanceAction[actions.length - 1];
        instance.actions = instanceActions;
        int i = 0;
        for (FileStatus file : actions) {
            Path filePath = file.getPath();
            String dfsBrowserUrl = getDFSbrowserUrl(
                    ClusterHelper.getStorageUrl(cluster),
                    EntityUtil.getLogPath(cluster, entity) + "/job-"
                            + EntityUtil.fromUTCtoURIDate(instance.instance) + "/"
                            + formatedRunId, file.getPath().getName());
            if (filePath.getName().equals("oozie.log")) {
                instance.logFile = dfsBrowserUrl;
                continue;
            }

            InstanceAction instanceAction = new InstanceAction(
                    getActionName(filePath.getName()),
                    getActionStatus(filePath.getName()), dfsBrowserUrl);
            instanceActions[i++] = instanceAction;
        }

        return instance;

    }

    private String getActionName(String fileName) {
        return fileName.replaceAll("_SUCCEEDED.log", "").replaceAll(
                "_FAILED.log", "");
    }

    private String getActionStatus(String fileName) {
        return fileName.contains("_SUCCEEDED.log") ? "SUCCEEDED" : "FAILED";
    }

    private String getDFSbrowserUrl(String hdfsPath, String logPath,
                                    String fileName) throws FalconException {
        String scheme = new Path(hdfsPath).toUri().getScheme();
        String httpUrl = hdfsPath.replaceAll(scheme + "://", "http://").replaceAll(
                ":[0-9]+", ":50070");
        return new Path(httpUrl, "/data/" + logPath + "/" + fileName).toString();
    }

    private String getFormatedRunId(String runId) {
        return String.format("%03d", Integer.parseInt(runId));
    }
}
