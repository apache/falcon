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

package org.apache.falcon.service;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.util.DeploymentUtil;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

public class SharedLibraryHostingService implements ConfigurationChangeListener {
    private static Logger LOG = Logger.getLogger(SharedLibraryHostingService.class);

    private static final String[] LIBS = StartupProperties.get().getProperty("shared.libs").split(",");

    private static final FalconPathFilter nonFalconJarFilter = new FalconPathFilter() {
        @Override
        public boolean accept(Path path) {
            for (String jarName : LIBS) {
                if (path.getName().startsWith(jarName)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String getJarName(Path path) {
            for (String jarName : LIBS) {
                if (path.getName().startsWith(jarName)) {
                    return jarName;
                }
            }
            throw new IllegalArgumentException(path + " is not accepted!");
        }
    };

    private void addLibsTo(Cluster cluster) throws FalconException {
        String libLocation = ClusterHelper.getLocation(cluster, "working") + "/lib";
        try {
            pushLibsToHDFS(libLocation, cluster, nonFalconJarFilter);
        } catch (IOException e) {
            LOG.error("Failed to copy shared libs to cluster " + cluster.getName(), e);
        }
    }

    public static void pushLibsToHDFS(String path, Cluster cluster, FalconPathFilter pathFilter)
            throws IOException, FalconException {
        String localPaths = StartupProperties.get().getProperty("system.lib.location");
        assert localPaths != null && !localPaths.isEmpty() : "Invalid value for system.lib.location";
        if (!new File(localPaths).isDirectory()) {
            throw new FalconException(
                    localPaths + " configured for system.lib.location doesn't contain any valid libs");
        }

        Configuration conf = ClusterHelper.getConfiguration(cluster);
        conf.setInt("ipc.client.connect.max.retries", 10);
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (Exception e) {
            throw new FalconException("Unable to connect to HDFS: "
                    + ClusterHelper.getStorageUrl(cluster));
        }
        Path clusterPath = new Path(path);
        if (!fs.exists(clusterPath)) {
            fs.mkdirs(clusterPath);
        }

        for (File localFile : new File(localPaths).listFiles()) {
            Path localPath = new Path(localFile.getAbsolutePath());
            if (!pathFilter.accept(localPath)) {
                continue;
            }

            Path clusterFile = new Path(path, pathFilter.getJarName(localPath) + ".jar");
            if (fs.exists(clusterFile)) {
                FileStatus fstat = fs.getFileStatus(clusterFile);
                if (fstat.getLen() == localFile.length()) {
                    continue;
                }
            }
            fs.copyFromLocalFile(false, true, new Path(localFile.getAbsolutePath()), clusterFile);
            LOG.info("Copied " + localFile.getAbsolutePath() + " to " + path + " in " + fs.getUri());
        }
    }

    @Override
    public void onAdd(Entity entity) throws FalconException {
        if (entity.getEntityType() != EntityType.CLUSTER) {
            return;
        }

        Cluster cluster = (Cluster) entity;
        String currentColo = DeploymentUtil.getCurrentColo();
        if (DeploymentUtil.isEmbeddedMode() || currentColo.equals(cluster.getColo())) {
            addLibsTo(cluster);
        }
    }

    @Override
    public void onRemove(Entity entity) throws FalconException {
        // Do Nothing
    }

    @Override
    public void onChange(Entity oldEntity, Entity newEntity) throws FalconException {
        if (oldEntity.getEntityType() != EntityType.CLUSTER) {
            return;
        }
        Cluster oldCluster = (Cluster) oldEntity;
        Cluster newCluster = (Cluster) newEntity;
        if (!ClusterHelper.getInterface(oldCluster, Interfacetype.WRITE).getEndpoint()
                .equals(ClusterHelper.getInterface(newCluster, Interfacetype.WRITE).getEndpoint())
                || !ClusterHelper.getInterface(oldCluster, Interfacetype.WORKFLOW).getEndpoint()
                .equals(ClusterHelper.getInterface(newCluster, Interfacetype.WORKFLOW).getEndpoint())) {
            addLibsTo(newCluster);
        }
    }
}
