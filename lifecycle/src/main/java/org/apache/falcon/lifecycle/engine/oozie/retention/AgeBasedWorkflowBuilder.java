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

package org.apache.falcon.lifecycle.engine.oozie.retention;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.HiveUtil;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.RetentionStage;
import org.apache.falcon.lifecycle.engine.oozie.utils.OozieBuilderUtils;
import org.apache.falcon.lifecycle.retention.AgeBasedDelete;
import org.apache.falcon.oozie.workflow.ACTION;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.falcon.security.SecurityUtil;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.hadoop.fs.Path;

import java.util.Properties;

/**
 * Workflow Builder for AgeBasedDelete.
 */
public final class AgeBasedWorkflowBuilder {
    private static final String EVICTION_ACTION_TEMPLATE = "/action/feed/eviction-action.xml";
    private static final String EVICTION_ACTION_NAME = "eviction";

    private AgeBasedWorkflowBuilder(){

    }

    public static Properties build(Cluster cluster, Path basePath, Feed feed) throws FalconException {
        Path buildPath = OozieBuilderUtils.getBuildPath(basePath, LifeCycle.EVICTION.getTag());
        WORKFLOWAPP workflow = new WORKFLOWAPP();
        String wfName = EntityUtil.getWorkflowName(Tag.RETENTION, feed).toString();

        //Add eviction action
        ACTION eviction = OozieBuilderUtils.unmarshalAction(EVICTION_ACTION_TEMPLATE);

        if (!Boolean.parseBoolean(OozieBuilderUtils.ENABLE_POSTPROCESSING)){
            OozieBuilderUtils.addTransition(eviction, OozieBuilderUtils.OK_ACTION_NAME,
                    OozieBuilderUtils.FAIL_ACTION_NAME);
            workflow.getDecisionOrForkOrJoin().add(eviction);
        } else {
            OozieBuilderUtils.addTransition(eviction, OozieBuilderUtils.SUCCESS_POSTPROCESS_ACTION_NAME,
                    OozieBuilderUtils.FAIL_POSTPROCESS_ACTION_NAME);
            workflow.getDecisionOrForkOrJoin().add(eviction);

            //Add post-processing actions
            ACTION success = OozieBuilderUtils.getSuccessPostProcessAction();
            OozieBuilderUtils.addTransition(success, OozieBuilderUtils.OK_ACTION_NAME,
                    OozieBuilderUtils.FAIL_ACTION_NAME);
            workflow.getDecisionOrForkOrJoin().add(success);

            ACTION fail = OozieBuilderUtils.getFailPostProcessAction();
            OozieBuilderUtils.addTransition(fail, OozieBuilderUtils.FAIL_ACTION_NAME,
                    OozieBuilderUtils.FAIL_ACTION_NAME);
            workflow.getDecisionOrForkOrJoin().add(fail);
        }
        OozieBuilderUtils.decorateWorkflow(workflow, wfName, EVICTION_ACTION_NAME);
        OozieBuilderUtils.addLibExtensionsToWorkflow(cluster, workflow, Tag.RETENTION, EntityType.FEED);

        // Prepare and marshal properties to config-default.xml
        Properties props = OozieBuilderUtils.getProperties(buildPath, wfName);
        props.putAll(getWorkflowProperties(feed, cluster));
        props.putAll(OozieBuilderUtils.createDefaultConfiguration(cluster, feed,
                WorkflowExecutionContext.EntityOperations.DELETE));
        props.putAll(FeedHelper.getUserWorkflowProperties(LifeCycle.EVICTION));
        // override the queueName and priority
        RetentionStage retentionStage = FeedHelper.getRetentionStage(feed, cluster.getName());
        if (StringUtils.isNotBlank(retentionStage.getQueue())) {
            props.put(OozieBuilderUtils.MR_QUEUE_NAME, retentionStage.getQueue());
        }
        if (StringUtils.isNotBlank(retentionStage.getPriority())) {
            props.put(OozieBuilderUtils.MR_JOB_PRIORITY, retentionStage.getPriority());
        }

        if (EntityUtil.isTableStorageType(cluster, feed)) {
            setupHiveCredentials(cluster, buildPath, workflow);
            // copy paste todo kludge send source hcat creds for coord dependency check to pass
            props.putAll(HiveUtil.getHiveCredentials(cluster));
        }

        // Write out the config to config-default.xml
        OozieBuilderUtils.marshalDefaultConfig(cluster, workflow, props, buildPath);

        // write out the workflow.xml
        OozieBuilderUtils.marshalWokflow(cluster, workflow, buildPath);
        return props;
    }

    private static Properties getWorkflowProperties(Feed feed, Cluster cluster) throws FalconException {
        final Storage storage = FeedHelper.createStorage(cluster, feed);
        Properties props = new Properties();
        props.setProperty("srcClusterName", "NA");
        props.setProperty("availabilityFlag", "NA");
        props.put("timeZone", feed.getTimezone().getID());
        props.put("frequency", feed.getFrequency().getTimeUnit().name());
        props.put("falconFeedStorageType", storage.getType().name());
        props.put("limit", new AgeBasedDelete().getRetentionLimit(feed, cluster.getName()).toString());
        props.put("falconInputFeeds", feed.getName());
        props.put("falconInPaths", OozieBuilderUtils.IGNORE);

        String feedDataPath = storage.getUriTemplate();
        props.put("feedDataPath",
                feedDataPath.replaceAll(Storage.DOLLAR_EXPR_START_REGEX, Storage.QUESTION_EXPR_START_REGEX));

        props.put(WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName(), feed.getName());
        props.put(WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName(), OozieBuilderUtils.IGNORE);

        return props;
    }

    private static void setupHiveCredentials(Cluster cluster, Path wfPath, WORKFLOWAPP workflowApp)
        throws FalconException {
        if (SecurityUtil.isSecurityEnabled()) {
            // add hcatalog credentials for secure mode and add a reference to each action
            OozieBuilderUtils.addHCatalogCredentials(workflowApp, cluster, OozieBuilderUtils.HIVE_CREDENTIAL_NAME);
        }

        // create hive-site.xml file so actions can use it in the classpath
        OozieBuilderUtils.createHiveConfiguration(cluster, wfPath, ""); // no prefix since only one hive instance

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

                if (SecurityUtil.isSecurityEnabled()) {
                    // add a reference to credential in the action
                    action.setCred(OozieBuilderUtils.HIVE_CREDENTIAL_NAME);
                }
            }
        }
    }
}
