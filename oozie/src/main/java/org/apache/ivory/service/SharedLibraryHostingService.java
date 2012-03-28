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

import org.apache.commons.el.ExpressionEvaluatorImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.store.ConfigurationStore;
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

import javax.servlet.jsp.el.ExpressionEvaluator;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Properties;

public class SharedLibraryHostingService implements IvoryService, ConfigurationChangeListener {

    private static Logger LOG = Logger.getLogger(SharedLibraryHostingService.class);

    private final ConfigurationStore store = ConfigurationStore.get();

    private static final String SYS_LIB_PATH =
            "oozie.service.WorkflowAppService.system.libpath";

    @Override
    public String getName() {
        return "workflow-libs";
    }

    @Override
    public void init() throws IvoryException {
        store.registerListener(this);
        Collection<String> clusterNames = store.getEntities(EntityType.CLUSTER);
        for (String clusterName : clusterNames) {
            Cluster cluster = store.get(EntityType.CLUSTER, clusterName);
            addLibsTo(cluster);
        }
    }

    private void addLibsTo(Cluster cluster) throws IvoryException {
        OozieClient oozieClient = OozieClientFactory.get(cluster);
        if (oozieClient instanceof CustomOozieClient) {
            CustomOozieClient customClient = (CustomOozieClient) oozieClient;
            try {
                String path = getSystemLibPath(customClient);
                pushLibsToHDFS(path, cluster);
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

    private void pushLibsToHDFS(String path, Cluster cluster) throws IOException {
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
            if (fs.exists(clusterFile)) {
                FileStatus fstat = fs.getFileStatus(clusterFile);
                if (fstat.getLen() == localFile.length() &&
                        fstat.getModificationTime() == localFile.lastModified()) continue;
            }
            fs.copyFromLocalFile(false, true, new Path(localFile.getAbsolutePath()), clusterFile);
            fs.setTimes(clusterFile,  localFile.lastModified(), System.currentTimeMillis());
            LOG.info("Copied " + localFile.getAbsolutePath() + " to " + path + " in " + fs.getUri());
        }
    }

    @Override
    public void destroy() throws IvoryException {
        //Do Nothing
    }

    @Override
    public void onAdd(Entity entity) throws IvoryException {
        if (entity.getEntityType() != EntityType.CLUSTER) return;
        Cluster cluster = (Cluster) entity;
        addLibsTo(cluster);
    }

    @Override
    public void onRemove(Entity entity) throws IvoryException {
        //Do Nothing
    }

    @Override
    public void onChange(Entity oldEntity, Entity newEntity) throws IvoryException {
        if (oldEntity.getEntityType() != EntityType.CLUSTER) return;
        Cluster oldCluster = (Cluster) oldEntity;
        Cluster newCluster = (Cluster) newEntity;
        if (!oldCluster.getInterfaces().get(Interfacetype.WRITE).getEndpoint().
                equals(newCluster.getInterfaces().get(Interfacetype.WRITE).getEndpoint()) ||
                !oldCluster.getInterfaces().get(Interfacetype.WORKFLOW).getEndpoint().
                        equals(newCluster.getInterfaces().get(Interfacetype.WORKFLOW).getEndpoint())) {
            addLibsTo(newCluster);
        }
    }
}
