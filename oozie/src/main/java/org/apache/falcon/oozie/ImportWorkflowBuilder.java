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

package org.apache.falcon.oozie;

import org.apache.falcon.FalconException;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.hadoop.fs.Path;
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

/**
 * Builds oozie workflow for Datasource import.
 */

public abstract class ImportWorkflowBuilder extends OozieOrchestrationWorkflowBuilder<Feed> {

    public ImportWorkflowBuilder(Feed feed) {

        super(feed, LifeCycle.IMPORT);
    }

    @Override public Properties build(Cluster cluster, Path buildPath) throws FalconException {

        WORKFLOWAPP workflow = new WORKFLOWAPP();
        String wfName = EntityUtil.getWorkflowName(Tag.IMPORT, entity).toString();
        workflow.setName(wfName);
        Properties p = getWorkflow(cluster, workflow, buildPath);
        marshal(cluster, workflow, buildPath);

        Properties props = FeedHelper.getFeedProperties(entity);
        if (props == null) {
            props = new Properties();
        }
        props.putAll(getProperties(buildPath, wfName));
        if (createDefaultConfiguration(cluster) != null) {
            props.putAll(createDefaultConfiguration(cluster));
        }
        if (FeedHelper.getUserWorkflowProperties(getLifecycle()) != null) {
            props.putAll(FeedHelper.getUserWorkflowProperties(getLifecycle()));
        }
        props.put(WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName(), entity.getName());
        props.put(WorkflowExecutionArgs.OUTPUT_NAMES.getName(), entity.getName());
        props.put(WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName(),
                String.format("${coord:dataOut('%s')}", FeedImportCoordinatorBuilder.IMPORT_DATAOUT_NAME));
        props.put(WorkflowExecutionArgs.INPUT_FEED_NAMES.getName(), NONE);
        props.put(WorkflowExecutionArgs.INPUT_FEED_PATHS.getName(), NONE);
        props.setProperty("srcClusterName", "NA");
        props.put(WorkflowExecutionArgs.CLUSTER_NAME.getName(), cluster.getName());

        if (StringUtils.isEmpty(FeedHelper.getImportDatasourceName(
            FeedHelper.getCluster(entity, cluster.getName())))) {
            throw new FalconException("Datasource name is null or empty");
        }

        props.put(WorkflowExecutionArgs.DATASOURCE_NAME.getName(),
            FeedHelper.getImportDatasourceName(FeedHelper.getCluster(entity, cluster.getName())));
        props.putAll(p);
        return props;
    }

    protected abstract Properties getWorkflow(Cluster cluster, WORKFLOWAPP workflow, Path buildPath)
        throws FalconException;
}
