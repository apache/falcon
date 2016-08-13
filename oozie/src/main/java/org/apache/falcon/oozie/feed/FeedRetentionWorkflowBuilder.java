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
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.HiveUtil;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.oozie.OozieOrchestrationWorkflowBuilder;
import org.apache.falcon.oozie.workflow.ACTION;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.hadoop.fs.Path;

import java.util.Properties;

/**
 * Builds feed retention workflow.
 */
public class FeedRetentionWorkflowBuilder extends OozieOrchestrationWorkflowBuilder<Feed> {
    private static final String EVICTION_ACTION_TEMPLATE = "/action/feed/eviction-action.xml";

    private static final String EVICTION_ACTION_NAME = "eviction";

    public FeedRetentionWorkflowBuilder(Feed entity) {
        super(entity, LifeCycle.EVICTION);
    }

    @Override public Properties build(Cluster cluster, Path buildPath) throws FalconException {
        WORKFLOWAPP workflow = new WORKFLOWAPP();
        String wfName = EntityUtil.getWorkflowName(Tag.RETENTION, entity).toString();
        //Add eviction action
        ACTION eviction = unmarshalAction(EVICTION_ACTION_TEMPLATE);

        addPostProcessing(workflow, eviction);
        decorateWorkflow(workflow, wfName, EVICTION_ACTION_NAME);
        addLibExtensionsToWorkflow(cluster, workflow, Tag.RETENTION);

        Properties props = getProperties(buildPath, wfName);
        props.putAll(getWorkflowProperties(cluster));
        props.putAll(createDefaultConfiguration(cluster));
        props.putAll(FeedHelper.getUserWorkflowProperties(getLifecycle()));

        if (EntityUtil.isTableStorageType(cluster, entity)) {
            setupHiveCredentials(cluster, buildPath, workflow);
            // todo: kludge send source hcat creds for coord dependency check to pass
            props.putAll(HiveUtil.getHiveCredentials(cluster));
        }

        marshal(cluster, workflow, buildPath);

        // Write out the config to config-default.xml
        marshal(cluster, workflow, getConfig(props), buildPath);
        return props;
    }

    private Properties getWorkflowProperties(Cluster cluster) throws FalconException {
        Properties props = new Properties();
        props.setProperty("srcClusterName", "NA");
        props.setProperty("availabilityFlag", "NA");

        props.put("timeZone", entity.getTimezone().getID());
        props.put("frequency", entity.getFrequency().getTimeUnit().name());

        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(entity, cluster.getName());
        final Storage storage = FeedHelper.createStorage(cluster, entity);
        props.put("falconFeedStorageType", storage.getType().name());

        String feedDataPath = storage.getUriTemplate();
        props.put("feedDataPath",
                feedDataPath.replaceAll(Storage.DOLLAR_EXPR_START_REGEX, Storage.QUESTION_EXPR_START_REGEX));

        props.put("limit", feedCluster.getRetention().getLimit().toString());

        props.put(WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName(), entity.getName());
        props.put(WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName(), IGNORE);

        props.put("falconInputFeeds", entity.getName());
        props.put("falconInPaths", IGNORE);
        props.put(WorkflowExecutionArgs.DATASOURCE_NAME.getName(), "NA");
        return props;
    }

    private void setupHiveCredentials(Cluster cluster, Path wfPath,
        WORKFLOWAPP workflowApp) throws FalconException {
        if (isSecurityEnabled) {
            // add hcatalog credentials for secure mode and add a reference to each action
            addHCatalogCredentials(workflowApp, cluster, HIVE_CREDENTIAL_NAME);
        }

        // create hive-site.xml file so actions can use it in the classpath
        createHiveConfiguration(cluster, wfPath, ""); // no prefix since only one hive instance

        for (Object object : workflowApp.getDecisionOrForkOrJoin()) {
            if (!(object instanceof org.apache.falcon.oozie.workflow.ACTION)) {
                continue;
            }

            org.apache.falcon.oozie.workflow.ACTION action =
                (org.apache.falcon.oozie.workflow.ACTION) object;
            String actionName = action.getName();
            if (EVICTION_ACTION_NAME.equals(actionName)) {
                // add reference to hive-site conf to each action
                action.getJava().setJobXml("${wf:appPath()}/conf/hive-site.xml");

                if (isSecurityEnabled) {
                    // add a reference to credential in the action
                    action.setCred(HIVE_CREDENTIAL_NAME);
                }
            }
        }
    }

    @Override
    protected WorkflowExecutionContext.EntityOperations getOperation() {
        return WorkflowExecutionContext.EntityOperations.DELETE;
    }
}
