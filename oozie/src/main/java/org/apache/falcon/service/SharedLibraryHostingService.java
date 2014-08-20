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

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * Host shared libraries in oozie shared lib dir upon creation or modification of cluster.
 */
public class SharedLibraryHostingService implements ConfigurationChangeListener {
    private static final Logger LOG = LoggerFactory.getLogger(SharedLibraryHostingService.class);

    private static final String[] LIBS = StartupProperties.get().getProperty("shared.libs").split(",");

    private static final FalconPathFilter NON_FALCON_JAR_FILTER = new FalconPathFilter() {
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
        Path lib = new Path(ClusterHelper.getLocation(cluster, "working"), "lib");
        Path libext = new Path(ClusterHelper.getLocation(cluster, "working"), "libext");
        try {
            Properties properties = StartupProperties.get();
            pushLibsToHDFS(properties.getProperty("system.lib.location"), lib, cluster, NON_FALCON_JAR_FILTER);
            pushLibsToHDFS(properties.getProperty("libext.paths"), libext, cluster, null);
            pushLibsToHDFS(properties.getProperty("libext.feed.paths"),
                    new Path(libext, EntityType.FEED.name()) , cluster, null);
            pushLibsToHDFS(properties.getProperty("libext.feed.replication.paths"),
                    new Path(libext, EntityType.FEED.name() + "/replication"), cluster, null);
            pushLibsToHDFS(properties.getProperty("libext.feed.retention.paths"),
                    new Path(libext, EntityType.FEED.name() + "/retention"), cluster, null);
            pushLibsToHDFS(properties.getProperty("libext.process.paths"),
                    new Path(libext, EntityType.PROCESS.name()) , cluster, null);
        } catch (IOException e) {
            throw new FalconException("Failed to copy shared libs to cluster" + cluster.getName(), e);
        }
    }

    public static void pushLibsToHDFS(String src, Path target, Cluster cluster, FalconPathFilter pathFilter)
        throws IOException, FalconException {
        if (StringUtils.isEmpty(src)) {
            return;
        }

        LOG.debug("Copying libs from {}", src);
        FileSystem fs;
        try {
            fs = getFileSystem(cluster);
            fs.getConf().set("dfs.umaskmode", "022");  // drwxr-xr-x
        } catch (Exception e) {
            throw new FalconException("Unable to connect to HDFS: "
                    + ClusterHelper.getStorageUrl(cluster), e);
        }
        if (!fs.exists(target) && !fs.mkdirs(target)) {
            throw new FalconException("mkdir " + target + " failed");
        }

        for(String srcPaths : src.split(",")) {
            File srcFile = new File(srcPaths);
            File[] srcFiles = new File[] { srcFile };
            if (srcFile.isDirectory()) {
                srcFiles = srcFile.listFiles();
            }

            for (File file : srcFiles) {
                Path path = new Path(file.getAbsolutePath());
                String jarName = StringUtils.removeEnd(path.getName(), ".jar");
                if (pathFilter != null) {
                    if (!pathFilter.accept(path)) {
                        continue;
                    }
                    jarName = pathFilter.getJarName(path);
                }

                Path targetFile = new Path(target, jarName + ".jar");
                if (fs.exists(targetFile)) {
                    FileStatus fstat = fs.getFileStatus(targetFile);
                    if (fstat.getLen() == file.length()) {
                        continue;
                    }
                }
                fs.copyFromLocalFile(false, true, new Path(file.getAbsolutePath()), targetFile);
                LOG.info("Copied {} to {} in {}", file.getAbsolutePath(), targetFile.toString(), fs.getUri());
            }
        }
    }

    // the dir is owned by Falcon but world-readable
    private static FileSystem getFileSystem(Cluster cluster)
        throws FalconException, IOException {
        Configuration conf = ClusterHelper.getConfiguration(cluster);
        conf.setInt("ipc.client.connect.max.retries", 10);

        return HadoopClientFactory.get().createFileSystem(conf);
    }

    @Override
    public void onAdd(Entity entity) throws FalconException {
        if (entity.getEntityType() != EntityType.CLUSTER) {
            return;
        }

        Cluster cluster = (Cluster) entity;
        if (!EntityUtil.responsibleFor(cluster.getColo())) {
            return;
        }

        addLibsTo(cluster);
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

    @Override
    public void onReload(Entity entity) throws FalconException {
        try {
            onAdd(entity);
        } catch (FalconException e) {
            throw new FalconException(e);
        }
    }
}
