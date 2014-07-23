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
import org.apache.falcon.Tag;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.oozie.OozieOrchestrationWorkflowBuilder;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.hadoop.fs.Path;

import java.util.Properties;

/**
 * Builds feed retention workflow.
 */
public class FeedRetentionWorkflowBuilder extends OozieOrchestrationWorkflowBuilder<Feed> {
    private static final String RETENTION_WF_TEMPLATE = "/workflow/retention-workflow.xml";

    public FeedRetentionWorkflowBuilder(Feed entity) {
        super(entity, Tag.DEFAULT);
    }

    @Override public Properties build(Cluster cluster, Path buildPath) throws FalconException {
        WORKFLOWAPP workflow = unmarshal(RETENTION_WF_TEMPLATE);
        String wfName = EntityUtil.getWorkflowName(Tag.RETENTION, entity).toString();
        workflow.setName(wfName);
        addLibExtensionsToWorkflow(cluster, workflow, Tag.RETENTION);
        addOozieRetries(workflow);

        if (isTableStorageType(cluster)) {
            setupHiveCredentials(cluster, buildPath, workflow);
        }

        Path marshalPath = marshal(cluster, workflow, buildPath);
        return getProperties(marshalPath, wfName);
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
            if ("eviction".equals(actionName)) {
                // add reference to hive-site conf to each action
                action.getJava().setJobXml("${wf:appPath()}/conf/hive-site.xml");

                if (isSecurityEnabled) {
                    // add a reference to credential in the action
                    action.setCred(HIVE_CREDENTIAL_NAME);
                }
            }
        }
    }

}
