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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.resource.InstancesResult.Instance;
import org.apache.falcon.resource.InstancesResult.InstanceAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Get oozie action execution logs corresponding to a run as saved by the log mover.
 */
public final class LogProvider {
    private static final Logger LOG = LoggerFactory.getLogger(LogProvider.class);

    public Instance populateLogUrls(Entity entity, Instance instance,
                                    String runId) throws FalconException {

        Cluster clusterObj = ConfigurationStore.get().get(
                EntityType.CLUSTER, instance.cluster);
        try {
            Configuration conf = ClusterHelper.getConfiguration(clusterObj);
            // fs on behalf of the end user.
            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(conf);
            String resolvedRunId = getResolvedRunId(fs, clusterObj, entity, instance, runId);
            // if runId param is not resolved, i.e job is killed or not started or running
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

            //Assumption here is - Each wf run will have a directory
            //the corresponding job logs are stored within the respective dir
            Path maxRunPath = findMaxRunIdPath(fs, jobPath);
            if (maxRunPath != null) {
                return maxRunPath.getName();
            } else {
                LOG.warn("No run dirs are available in logs dir: {}", jobPath);
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
                LOG.warn("No run dirs are available in logs dir:" + jobPath);
                return "-";
            }
        }

    }

    private Instance populateActionLogUrls(FileSystem fs, Cluster cluster,
                                           Entity entity, Instance instance, String formattedRunId)
        throws FalconException, OozieClientException, IOException {

        Path actionPaths = new Path(ClusterHelper.getStorageUrl(cluster),
                EntityUtil.getLogPath(cluster, entity) + "/job-"
                        + EntityUtil.fromUTCtoURIDate(instance.instance) + "/"
                        + formattedRunId + "/*");
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
                            + formattedRunId, file.getPath().getName());
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
        return fileName.matches("(.*)SUCCEEDED(.*).log") ? "SUCCEEDED" : "FAILED";
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

    private Path findMaxRunIdPath(FileSystem fs, Path jobLogPath) throws IOException{
        // In case of multiple runs, dirs are sorted in increasing
        // order of runs. If runId is not specified, return the max runId (whose dir exists)
        Path maxRunIdPath = null;
        for (FileStatus fileStatus : fs.globStatus(jobLogPath)) {
            if (fs.isDirectory(fileStatus.getPath())) {
                maxRunIdPath = fileStatus.getPath();
            }
        }
        return maxRunIdPath;
    }
}
