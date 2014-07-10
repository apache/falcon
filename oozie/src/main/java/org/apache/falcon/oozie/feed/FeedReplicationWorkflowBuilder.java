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
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.oozie.OozieOrchestrationWorkflowBuilder;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.hadoop.fs.Path;

import java.util.Properties;

/**
 * Builds feed replication workflow, one per source-target cluster combination.
 */
public class FeedReplicationWorkflowBuilder extends OozieOrchestrationWorkflowBuilder<Feed> {
    private static final String REPLICATION_WF_TEMPLATE = "/workflow/replication-workflow.xml";
    private static final String SOURCE_HIVE_CREDENTIAL_NAME = "falconSourceHiveAuth";
    private static final String TARGET_HIVE_CREDENTIAL_NAME = "falconTargetHiveAuth";

    public FeedReplicationWorkflowBuilder(Feed entity) {
        super(entity, Tag.REPLICATION);
    }

    @Override public Properties build(Cluster cluster, Path buildPath) throws FalconException {
        WORKFLOWAPP workflow = getWorkflow(REPLICATION_WF_TEMPLATE);
        Cluster srcCluster = ConfigurationStore.get().get(EntityType.CLUSTER, buildPath.getName());
        String wfName = EntityUtil.getWorkflowName(Tag.REPLICATION, entity).toString();
        workflow.setName(wfName);

        addLibExtensionsToWorkflow(cluster, workflow, Tag.REPLICATION);

        addOozieRetries(workflow);

        if (isTableStorageType(cluster)) {
            setupHiveCredentials(cluster, srcCluster, workflow);
        }

        marshal(cluster, workflow, buildPath);

        return getProperties(buildPath, wfName);
    }

    private void setupHiveCredentials(Cluster targetCluster, Cluster sourceCluster, WORKFLOWAPP workflowApp) {
        if (isSecurityEnabled) {
            // add hcatalog credentials for secure mode and add a reference to each action
            addHCatalogCredentials(workflowApp, sourceCluster, SOURCE_HIVE_CREDENTIAL_NAME);
            addHCatalogCredentials(workflowApp, targetCluster, TARGET_HIVE_CREDENTIAL_NAME);
        }

        // hive-site.xml file is created later in coordinator initialization but
        // actions are set to point to that here

        for (Object object : workflowApp.getDecisionOrForkOrJoin()) {
            if (!(object instanceof org.apache.falcon.oozie.workflow.ACTION)) {
                continue;
            }

            org.apache.falcon.oozie.workflow.ACTION action =
                (org.apache.falcon.oozie.workflow.ACTION) object;
            String actionName = action.getName();
            if ("recordsize".equals(actionName)) {
                // add reference to hive-site conf to each action
                action.getJava().setJobXml("${wf:appPath()}/conf/falcon-source-hive-site.xml");

                if (isSecurityEnabled) { // add a reference to credential in the action
                    action.setCred(SOURCE_HIVE_CREDENTIAL_NAME);
                }
            } else if ("table-export".equals(actionName)) {
                if (isSecurityEnabled) { // add a reference to credential in the action
                    action.setCred(SOURCE_HIVE_CREDENTIAL_NAME);
                }
            } else if ("table-import".equals(actionName)) {
                if (isSecurityEnabled) { // add a reference to credential in the action
                    action.setCred(TARGET_HIVE_CREDENTIAL_NAME);
                }
            }
        }
    }
}
