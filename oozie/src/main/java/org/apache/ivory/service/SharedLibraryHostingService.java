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
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
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

public class SharedLibraryHostingService implements IvoryService {

    private static Logger LOG = Logger.getLogger(SharedLibraryHostingService.class);

    private final ConfigurationStore store = ConfigurationStore.get();
    private static final ExpressionEvaluator EVALUATOR = new ExpressionEvaluatorImpl();
    private static final ExpressionHelper resolver = ExpressionHelper.get();

    private static final String SYS_LIB_PATH =
            "oozie.service.WorkflowAppService.system.libpath";

    @Override
    public void init() throws IvoryException {
        Collection<String> clusterNames = store.getEntities(EntityType.CLUSTER);
        for (String clusterName : clusterNames) {
            Cluster cluster = store.get(EntityType.CLUSTER, clusterName);
            OozieClient oozieClient = OozieClientFactory.get(cluster);
            if (oozieClient instanceof CustomOozieClient) {
                CustomOozieClient customClient = (CustomOozieClient) oozieClient;
                try {
                    String path = getSystemLibPath(customClient);
                    pushLibsToHDFS(path, cluster);
                    System.out.println(path);
                } catch (Exception e) {
                    LOG.error("Unable to load shared libraries to " + clusterName, e);
                }
            } else {
                LOG.warn("Not loading shared libraries to " + clusterName);
            }
        }
    }

    private String getSystemLibPath(CustomOozieClient customClient) throws Exception {
        Properties configuration = customClient.getConfiguration();
        Properties sysProperties = customClient.getProperties();
        resolver.setPropertiesForVariable(sysProperties);
        return (String) EVALUATOR.evaluate(configuration.
                getProperty(SYS_LIB_PATH), String.class, resolver, resolver);
    }

    private void pushLibsToHDFS(String path, Cluster cluster) throws IOException {
        Configuration conf = ClusterHelper.getConfiguration(cluster);
        FileSystem fs = FileSystem.get(conf);
        String localPaths = StartupProperties.get().getProperty("system.lib.location");
        assert localPaths != null && !localPaths.isEmpty() : "Invalid value for system.lib.location";
        assert new File(localPaths).isDirectory() : localPaths + " is not a valid directory";
        for (File localFile : new File(localPaths).listFiles()) {
            Path clusterFile = new Path(path, localFile.getName());
            if (fs.exists(clusterFile)) {
                FileStatus fstat = fs.getFileStatus(clusterFile);
                if (fstat.getLen() == localFile.length() &&
                        fstat.getModificationTime() == localFile.lastModified()) continue;
            }
            LOG.info("Copied " + localFile.getAbsolutePath() + " to " + path + " in " + fs.getUri());
            fs.copyFromLocalFile(false, true, new Path(localFile.getAbsolutePath()), clusterFile);
            fs.setTimes(clusterFile,  localFile.lastModified(), System.currentTimeMillis());
        }
    }

    @Override
    public void destroy() throws IvoryException {
        //Do Nothing
    }
}
