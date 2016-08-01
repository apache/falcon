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

package org.apache.falcon.oozie.feed;

import org.apache.falcon.FalconException;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Property;
import org.apache.falcon.oozie.OozieOrchestrationWorkflowBuilder;
import org.apache.falcon.oozie.workflow.ACTION;
import org.apache.falcon.oozie.workflow.CONFIGURATION;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Properties;

/**
 * Builds feed replication workflow, one per source-target cluster combination.
 */
public abstract class FeedReplicationWorkflowBuilder extends OozieOrchestrationWorkflowBuilder<Feed> {
    protected static final String REPLICATION_ACTION_TEMPLATE = "/action/feed/replication-action.xml";
    protected static final String REPLICATION_ACTION_NAME = "replication";
    private static final String MR_MAX_MAPS = "maxMaps";
    private static final String MR_MAP_BANDWIDTH = "mapBandwidth";
    private static final String REPLICATION_JOB_COUNTER = "job.counter";
    private static final String TDE_ENCRYPTION_ENABLED = "tdeEncryptionEnabled";

    public FeedReplicationWorkflowBuilder(Feed entity) {
        super(entity, LifeCycle.REPLICATION);
    }

    public boolean isCounterEnabled() throws FalconException {
        if (entity.getProperties() != null) {
            List<Property> propertyList = entity.getProperties().getProperties();
            for (Property prop : propertyList) {
                if (prop.getName().equals(REPLICATION_JOB_COUNTER) && "true" .equalsIgnoreCase(prop.getValue())) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Properties build(Cluster cluster, Path buildPath) throws FalconException {
        Cluster srcCluster = ConfigurationStore.get().get(EntityType.CLUSTER, buildPath.getName());

        WORKFLOWAPP workflow = getWorkflow(srcCluster, cluster);
        String wfName = EntityUtil.getWorkflowName(Tag.REPLICATION, entity).toString();
        workflow.setName(wfName);

        addLibExtensionsToWorkflow(cluster, workflow, Tag.REPLICATION);

        marshal(cluster, workflow, buildPath);
        Properties props = getProperties(buildPath, wfName);
        props.putAll(createDefaultConfiguration(cluster));

        props.putAll(getWorkflowProperties(entity));
        props.putAll(FeedHelper.getUserWorkflowProperties(getLifecycle()));
        // Write out the config to config-default.xml
        marshal(cluster, workflow, getConfig(props), buildPath);
        return props;
    }

    protected Properties getWorkflowProperties(Feed feed) throws FalconException {
        Properties props = FeedHelper.getFeedProperties(feed);
        if (props.getProperty(MR_MAX_MAPS) == null) { // set default if user has not overridden
            props.put(MR_MAX_MAPS, getDefaultMaxMaps());
        }
        if (props.getProperty(MR_MAP_BANDWIDTH) == null) { // set default if user has not overridden
            props.put(MR_MAP_BANDWIDTH, getDefaultMapBandwidth());
        }

        return props;
    }

    protected ACTION addHDFSServersConfig(ACTION action, Cluster sourceCluster, Cluster targetCluster) {
        if (isSecurityEnabled) {
            // this is to ensure that the delegation tokens are checked out for both clusters
            CONFIGURATION.Property property = new CONFIGURATION.Property();
            property.setName("oozie.launcher.mapreduce.job.hdfs-servers");
            property.setValue(ClusterHelper.getReadOnlyStorageUrl(sourceCluster)
                    + "," + ClusterHelper.getStorageUrl(targetCluster));
            action.getJava().getConfiguration().getProperty().add(property);
        }
        return action;
    }

    protected ACTION enableCounters(ACTION action) throws FalconException {
        if (isCounterEnabled()) {
            List<String> args = action.getJava().getArg();
            args.add("-counterLogDir");
            args.add("${logDir}/job-${nominalTime}/${srcClusterName == 'NA' ? '' : srcClusterName}");
        }
        return action;
    }

    protected ACTION enableTDE(ACTION action) throws FalconException {
        if (isTDEEnabled()) {
            List<String> args = action.getJava().getArg();
            args.add("-tdeEncryptionEnabled");
            args.add("true");
        }
        return action;
    }

    protected abstract WORKFLOWAPP getWorkflow(Cluster src, Cluster target) throws FalconException;

    @Override
    protected WorkflowExecutionContext.EntityOperations getOperation() {
        return WorkflowExecutionContext.EntityOperations.REPLICATE;
    }

    private String getDefaultMaxMaps() {
        return RuntimeProperties.get().getProperty("falcon.replication.workflow.maxmaps", "5");
    }

    private String getDefaultMapBandwidth() {
        return RuntimeProperties.get().getProperty("falcon.replication.workflow.mapbandwidth", "100");
    }

    private boolean isTDEEnabled() {
        String tdeEncryptionEnabled = FeedHelper.getPropertyValue(entity, TDE_ENCRYPTION_ENABLED);
        return "true" .equalsIgnoreCase(tdeEncryptionEnabled);
    }
}
