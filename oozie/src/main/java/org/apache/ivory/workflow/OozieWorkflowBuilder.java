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

import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.security.CurrentUser;
import org.apache.ivory.workflow.engine.OozieWorkflowEngine;
import org.apache.oozie.client.OozieClient;

import java.util.*;

public abstract class OozieWorkflowBuilder extends WorkflowBuilder {

    protected static final ConfigurationStore configStore = ConfigurationStore.get();

    protected Map<String, Object> createAppProperties(Cluster cluster,
                                                      Path path) throws IvoryException {

        Properties properties = new Properties();
        properties.setProperty(OozieWorkflowEngine.NAME_NODE,
                ClusterHelper.getHdfsUrl(cluster));
        properties.setProperty(OozieWorkflowEngine.JOB_TRACKER,
                ClusterHelper.getMREndPoint(cluster));
        properties.setProperty(OozieClient.BUNDLE_APP_PATH,
                "${" + OozieWorkflowEngine.NAME_NODE + "}" + path.toString());

        properties.setProperty(OozieClient.USER_NAME, CurrentUser.getUser());

        Map<String, Object> map = new HashMap<String, Object>();
        List<Properties> propList = new ArrayList<Properties>();
        propList.add(properties);
        map.put(PROPS, propList);
        List<Cluster> clList = new ArrayList<Cluster>();
        clList.add(cluster);
        map.put(CLUSTERS, clList);
        return map;
    }
}
