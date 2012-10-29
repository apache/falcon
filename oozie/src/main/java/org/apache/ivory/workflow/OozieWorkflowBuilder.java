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
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.cluster.Property;
import org.apache.ivory.security.CurrentUser;
import org.apache.ivory.workflow.engine.OozieWorkflowEngine;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;

public abstract class OozieWorkflowBuilder<T extends Entity> extends WorkflowBuilder<T> {
    
    private static Logger LOG = Logger.getLogger(OozieWorkflowBuilder.class);
    protected static final ConfigurationStore configStore = ConfigurationStore.get();
    
    protected Properties createAppProperties(String clusterName, Path bundlePath) throws IvoryException {

        Cluster cluster = EntityUtil.getEntity(EntityType.CLUSTER, clusterName);
        Properties properties = new Properties();
		if (cluster.getProperties() != null) {
			addClusterProperties(properties, cluster.getProperties()
					.getProperties());
		}
        properties.setProperty(OozieWorkflowEngine.NAME_NODE,
                ClusterHelper.getHdfsUrl(cluster));
        properties.setProperty(OozieWorkflowEngine.JOB_TRACKER,
                ClusterHelper.getMREndPoint(cluster));
        properties.setProperty(OozieClient.BUNDLE_APP_PATH,
                "${" + OozieWorkflowEngine.NAME_NODE + "}" + bundlePath.toString());
        properties.setProperty("colo.name", cluster.getColo());
        
        properties.setProperty(OozieClient.USER_NAME, CurrentUser.getUser());
        properties.setProperty(OozieClient.USE_SYSTEM_LIBPATH, "true");
        properties.setProperty("ivory.libpath", ClusterHelper.getLocation(cluster, "working") + "/lib");
        LOG.info("Cluster: " + cluster.getName() + ", PROPS: " + properties);
        return properties;
    }
    
	private void addClusterProperties(Properties properties,
			List<Property> clusterProperties) {
		for (Property prop : clusterProperties) {
			properties.setProperty(prop.getName(), prop.getValue());
		}
	}

	public abstract Date getNextStartTime(T entity, String cluster, Date now) throws IvoryException;
}
