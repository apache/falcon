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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.ClusterLocationType;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.extensions.store.ExtensionStore;
import org.apache.falcon.extensions.ExtensionService;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * Host shared libraries in oozie shared lib dir upon creation or modification of cluster.
 */
public class SharedLibraryHostingService implements ConfigurationChangeListener {
    private static final Logger LOG = LoggerFactory.getLogger(SharedLibraryHostingService.class);

    private static final String[] LIBS = StartupProperties.get().getProperty("shared.libs").split(",");

    private static class FalconLibPath implements FalconPathFilter {
        private String[] shareLibs;

        FalconLibPath(String[] libList) {
            this.shareLibs = Arrays.copyOf(libList, libList.length);
        }

        @Override
        public boolean accept(Path path) {
            for (String jarName : shareLibs) {
                if (path.getName().startsWith(jarName)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String getJarName(Path path) {
            for (String jarName : shareLibs) {
                if (path.getName().startsWith(jarName)) {
                    return jarName;
                }
            }
            throw new IllegalArgumentException(path + " is not accepted!");
        }
    }


    private void pushExtensionArtifactsToCluster(final Cluster cluster,
                                                 final FileSystem clusterFs) throws FalconException {
        if (!Services.get().isRegistered(ExtensionService.SERVICE_NAME)) {
            LOG.info("ExtensionService not registered, return");
            return;
        }

        ExtensionStore store = ExtensionStore.get();
        if (!store.isExtensionStoreInitialized()) {
            LOG.info("Extension store not initialized by Extension service. Make sure Extension service is added in "
                    + "start up properties");
            return;
        }

        final String filterPath = "/apps/falcon/extensions/mirroring/";
        Path extensionStorePath = store.getExtensionStorePath();
        LOG.info("extensionStorePath :{}", extensionStorePath);
        FileSystem falconFileSystem =
                HadoopClientFactory.get().createFalconFileSystem(extensionStorePath.toUri());
        String nameNode = StringUtils.removeEnd(falconFileSystem.getConf().get(HadoopClientFactory
                .FS_DEFAULT_NAME_KEY), File.separator);


        String clusterStorageUrl = StringUtils.removeEnd(ClusterHelper.getStorageUrl(cluster), File.separator);

        // If default fs for Falcon server is same as cluster fs abort copy
        if (nameNode.equalsIgnoreCase(clusterStorageUrl)) {
            LOG.info("clusterStorageUrl :{} same return", clusterStorageUrl);
            return;
        }

        try {
            RemoteIterator<LocatedFileStatus> fileStatusListIterator =
                    falconFileSystem.listFiles(extensionStorePath, true);

            while (fileStatusListIterator.hasNext()) {
                LocatedFileStatus srcfileStatus = fileStatusListIterator.next();
                Path filePath = Path.getPathWithoutSchemeAndAuthority(srcfileStatus.getPath());

                if (filePath != null && filePath.toString().startsWith(filterPath)) {
                    /* HiveDR uses filter path as store path in DRStatusStore, so skip it. Copy only the extension
                     artifacts */
                    continue;
                }

                if (srcfileStatus.isDirectory()) {
                    if (!clusterFs.exists(filePath)) {
                        HadoopClientFactory.mkdirs(clusterFs, filePath, srcfileStatus.getPermission());
                    }
                } else {
                    if (clusterFs.exists(filePath)) {
                        FileStatus targetfstat = clusterFs.getFileStatus(filePath);
                        if (targetfstat.getLen() == srcfileStatus.getLen()) {
                            continue;
                        }
                    }

                    Path parentPath = filePath.getParent();
                    if (!clusterFs.exists(parentPath)) {
                        FsPermission dirPerm = falconFileSystem.getFileStatus(parentPath).getPermission();
                        HadoopClientFactory.mkdirs(clusterFs, parentPath, dirPerm);
                    }

                    FileUtil.copy(falconFileSystem, srcfileStatus, clusterFs, filePath, false, true,
                            falconFileSystem.getConf());
                    FileUtil.chmod(clusterFs.makeQualified(filePath).toString(),
                            srcfileStatus.getPermission().toString());
                }
            }
        } catch (IOException | InterruptedException e) {
            throw new FalconException("Failed to copy extension artifacts to cluster" + cluster.getName(), e);
        }
    }

    private void addLibsTo(Cluster cluster, FileSystem fs) throws FalconException {
        try {
            Path lib = new Path(ClusterHelper.getLocation(cluster, ClusterLocationType.WORKING).getPath(),
                    "lib");
            Path libext = new Path(ClusterHelper.getLocation(cluster, ClusterLocationType.WORKING).getPath(),
                    "libext");
            FalconPathFilter nonFalconJarFilter = new FalconLibPath(LIBS);
            Properties properties = StartupProperties.get();
            pushLibsToHDFS(fs, properties.getProperty("system.lib.location"), lib,
                    nonFalconJarFilter);
            pushLibsToHDFS(fs, properties.getProperty("libext.paths"), libext, null);
            pushLibsToHDFS(fs, properties.getProperty("libext.feed.paths"),
                    new Path(libext, EntityType.FEED.name()), null);
            pushLibsToHDFS(fs, properties.getProperty("libext.feed.replication.paths"),
                    new Path(libext, EntityType.FEED.name() + "/replication"), null);
            pushLibsToHDFS(fs, properties.getProperty("libext.feed.retention.paths"),
                    new Path(libext, EntityType.FEED.name() + "/retention"), null);
            pushLibsToHDFS(fs, properties.getProperty("libext.process.paths"),
                    new Path(libext, EntityType.PROCESS.name()), null);
        } catch (IOException e) {
            throw new FalconException("Failed to copy shared libs to cluster" + cluster.getName(), e);
        }
    }

    @SuppressWarnings("ConstantConditions")
    public static void pushLibsToHDFS(FileSystem fs, String src, Path target,
                                      FalconPathFilter pathFilter) throws IOException, FalconException {
        if (StringUtils.isEmpty(src)) {
            return;
        }
        LOG.debug("Copying libs from {}", src);
        createTargetPath(fs, target);

        for (String srcPaths : src.split(",")) {
            File srcFile = new File(srcPaths);
            File[] srcFiles = new File[]{srcFile};
            if (srcFiles != null) {
                if (srcFile.isDirectory()) {
                    srcFiles = srcFile.listFiles();
                }
            }

            if (srcFiles != null) {
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
                    fs.setPermission(targetFile, HadoopClientFactory.READ_EXECUTE_PERMISSION);
                    LOG.info("Copied {} to {} in {}",
                            file.getAbsolutePath(), targetFile.toString(), fs.getUri());
                }
            }
        }
    }

    private static void createTargetPath(FileSystem fs,
                                         Path target) throws IOException, FalconException {
        // the dir and files MUST be readable by all users
        if (!fs.exists(target)
                && !FileSystem.mkdirs(fs, target, HadoopClientFactory.READ_EXECUTE_PERMISSION)) {
            throw new FalconException("mkdir " + target + " failed");
        }
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

        addLibsTo(cluster, getFilesystem(cluster));
        pushExtensionArtifactsToCluster(cluster, getFilesystem(cluster));
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
            addLibsTo(newCluster, getFilesystem(newCluster));
            pushExtensionArtifactsToCluster(newCluster, getFilesystem(newCluster));
        }
    }

    @Override
    public void onReload(Entity entity) throws FalconException {
        try {
            onAdd(entity);
        } catch (FalconException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private FileSystem getFilesystem(final Cluster cluster) throws FalconException {
        FileSystem fs;
        try {
            LOG.info("Initializing FS: {} for cluster: {}", ClusterHelper.getStorageUrl(cluster), cluster.getName());
            fs = HadoopClientFactory.get().createFalconFileSystem(ClusterHelper.getConfiguration(cluster));
            fs.getStatus();
            return fs;
        } catch (Exception e) {
            throw new FalconException("Failed to initialize FS for cluster : " + cluster.getName(), e);
        }
    }
}
