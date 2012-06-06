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

package org.apache.ivory.service;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.cluster.Interfacetype;
import org.apache.ivory.expression.ExpressionHelper;
import org.apache.ivory.util.StartupProperties;
import org.apache.ivory.workflow.engine.OozieClientFactory;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CustomOozieClient;
import org.apache.oozie.client.OozieClient;

public class SharedLibraryHostingService implements ConfigurationChangeListener {
    private static Logger LOG = Logger.getLogger(SharedLibraryHostingService.class);

    private static final String SYS_LIB_PATH = "oozie.service.WorkflowAppService.system.libpath";

    private static final PathFilter nonIvoryJarFilter = new PathFilter() {
        @Override
        public boolean accept(Path path) {
            if(!path.getName().startsWith("ivory"))
                return true;
            return false;
        }
    };

    private void addLibsTo(Cluster cluster) throws IvoryException {
        OozieClient oozieClient = OozieClientFactory.get(cluster);
        if (oozieClient instanceof CustomOozieClient) {
            CustomOozieClient customClient = (CustomOozieClient) oozieClient;
            try {
                String path = getSystemLibPath(customClient);
                pushLibsToHDFS(path, cluster, nonIvoryJarFilter);
            } catch (Exception e) {
                LOG.error("Unable to load shared libraries to " + cluster.getName(), e);
            }
        } else {
            LOG.warn("Not loading shared libraries to " + cluster.getName());
        }
    }

    private String getSystemLibPath(CustomOozieClient customClient) throws Exception {
        Properties allProps = customClient.getProperties();
        allProps.putAll(customClient.getConfiguration());
        return ExpressionHelper.substitute(allProps.getProperty(SYS_LIB_PATH), allProps);
    }

    public static void pushLibsToHDFS(String path, Cluster cluster, PathFilter pathFilter) throws IOException {
        Configuration conf = ClusterHelper.getConfiguration(cluster);
        FileSystem fs = FileSystem.get(conf);
        String localPaths = StartupProperties.get().getProperty("system.lib.location");
        assert localPaths != null && !localPaths.isEmpty() : "Invalid value for system.lib.location";
        if (!new File(localPaths).isDirectory()) {
            LOG.warn(localPaths + " configured for system.lib.location doesn't contain any valid libs");
            return;
        }
        for (File localFile : new File(localPaths).listFiles()) {
            Path clusterFile = new Path(path, localFile.getName());
            if (!pathFilter.accept(clusterFile))
                continue;

            if (fs.exists(clusterFile)) {
                FileStatus fstat = fs.getFileStatus(clusterFile);
                if (fstat.getLen() == localFile.length() && fstat.getModificationTime() == localFile.lastModified())
                    continue;
            }
            fs.copyFromLocalFile(false, true, new Path(localFile.getAbsolutePath()), clusterFile);
            fs.setTimes(clusterFile, localFile.lastModified(), System.currentTimeMillis());
            LOG.info("Copied " + localFile.getAbsolutePath() + " to " + path + " in " + fs.getUri());
        }
    }

    @Override
    public void onAdd(Entity entity) throws IvoryException {
        if (entity.getEntityType() != EntityType.CLUSTER)
            return;
        Cluster cluster = (Cluster) entity;
        addLibsTo(cluster);
    }

    @Override
    public void onRemove(Entity entity) throws IvoryException {
        // Do Nothing
    }

    @Override
    public void onChange(Entity oldEntity, Entity newEntity) throws IvoryException {
        if (oldEntity.getEntityType() != EntityType.CLUSTER)
            return;
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
