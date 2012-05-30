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

package org.apache.ivory.workflow;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.security.CurrentUser;
import org.apache.ivory.workflow.engine.OozieWorkflowEngine;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;

public abstract class OozieWorkflowBuilder<T extends Entity> extends WorkflowBuilder<T> {

    private static Logger LOG = Logger.getLogger(OozieWorkflowBuilder.class);

    protected static final ConfigurationStore configStore = ConfigurationStore.get();

    
    protected Map<String, Properties> createAppProperties(Map<String, Path> pathMap) throws IvoryException {

        Map<String, Properties> propsMap = new HashMap<String, Properties>();
        for (String clusterName:pathMap.keySet()) {
            Path path = pathMap.get(clusterName);
            Cluster cluster = (Cluster) EntityUtil.getEntity(EntityType.CLUSTER, clusterName);
            Properties properties = new Properties();
            properties.setProperty(OozieWorkflowEngine.NAME_NODE,
                    ClusterHelper.getHdfsUrl(cluster));
            properties.setProperty(OozieWorkflowEngine.JOB_TRACKER,
                    ClusterHelper.getMREndPoint(cluster));
            properties.setProperty(OozieClient.BUNDLE_APP_PATH,
                    "${" + OozieWorkflowEngine.NAME_NODE + "}" + path.toString());

            properties.setProperty(OozieClient.USER_NAME, CurrentUser.getUser());
            properties.setProperty(OozieClient.USE_SYSTEM_LIBPATH, "true");
            propsMap.put(clusterName, properties);
            LOG.info("Cluster: " + cluster.getName() + ", PROPS: " + properties);
        }
        return propsMap;
    }
    
    public abstract Date getNextStartTime(T entity, String cluster, Date now) throws IvoryException;
}
