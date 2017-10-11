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
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.HiveUtil;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.oozie.workflow.ACTION;
import org.apache.falcon.oozie.workflow.WORKFLOWAPP;
import org.apache.falcon.oozie.workflow.CONFIGURATION;
import org.apache.falcon.util.OozieUtils;
import org.apache.falcon.workflow.WorkflowExecutionArgs;

import javax.xml.bind.JAXBElement;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * Builds replication workflow for hcat based feed.
 */
public class HCatReplicationWorkflowBuilder extends FeedReplicationWorkflowBuilder {
    private static final String EXPORT_ACTION_TEMPLATE = "/action/feed/table-export.xml";
    private static final String IMPORT_ACTION_TEMPLATE = "/action/feed/table-import.xml";
    private static final String CLEANUP_ACTION_TEMPLATE = "/action/feed/table-cleanup.xml";

    private static final String SOURCE_HIVE_CREDENTIAL_NAME = "falconSourceHiveAuth";
    private static final String TARGET_HIVE_CREDENTIAL_NAME = "falconTargetHiveAuth";
    public static final String EXPORT_ACTION_NAME = "table-export";
    public static final String IMPORT_ACTION_NAME = "table-import";
    private static final String CLEANUP_ACTION_NAME = "cleanup-table-staging-dir";

    public HCatReplicationWorkflowBuilder(Feed entity) {
        super(entity);
    }

    @Override protected WORKFLOWAPP getWorkflow(Cluster src, Cluster target) throws FalconException {
        WORKFLOWAPP workflow = new WORKFLOWAPP();
        String wfName = EntityUtil.getWorkflowName(Tag.REPLICATION, Arrays.asList(src.getName()), entity).toString();

        String start = EXPORT_ACTION_NAME;

        //Add pre-processing
        if (shouldPreProcess()) {
            ACTION action = getPreProcessingAction(false, Tag.REPLICATION);
            Properties hiveConf = HiveUtil.getHiveCredentials(src);
            for (Map.Entry<Object, Object> e : hiveConf.entrySet()) {
                CONFIGURATION.Property prop = new CONFIGURATION.Property();
                prop.setName((String) e.getKey());
                prop.setValue((String) e.getValue());
                LOG.info("Adding config to replication hive preprocessing action : key = {}  value = {}",
                        e.getKey(), e.getValue());
                action.getJava().getConfiguration().getProperty().add(prop);
            }
            addHDFSServersConfig(action, src, target);
            addTransition(action, EXPORT_ACTION_NAME, getFailAction());
            workflow.getDecisionOrForkOrJoin().add(action);
            start = PREPROCESS_ACTION_NAME;
        }

        //Add export action
        ACTION export = unmarshalAction(EXPORT_ACTION_TEMPLATE);
        JAXBElement<org.apache.falcon.oozie.hive.ACTION> exportActionJaxbElement =
            OozieUtils.unMarshalHiveAction(export);
        org.apache.falcon.oozie.hive.ACTION hiveExportAction = exportActionJaxbElement.getValue();
        addHDFSServersConfig(hiveExportAction, src, target);
        Properties hiveConf = HiveUtil.getHiveCredentials(src);
        for (Map.Entry<Object, Object> e : hiveConf.entrySet()) {
            org.apache.falcon.oozie.hive.CONFIGURATION.Property prop =
                    new org.apache.falcon.oozie.hive.CONFIGURATION.Property();
            prop.setName((String) e.getKey());
            prop.setValue((String) e.getValue());
            LOG.info("Adding config to replication hive export action : key = {}  value = {}",
                    e.getKey(), e.getValue());
            hiveExportAction.getConfiguration().getProperty().add(prop);
        }
        OozieUtils.marshalHiveAction(export, exportActionJaxbElement);
        addTransition(export, REPLICATION_ACTION_NAME, getFailAction());
        workflow.getDecisionOrForkOrJoin().add(export);

        //Add replication
        ACTION replication = unmarshalAction(REPLICATION_ACTION_TEMPLATE);
        addHDFSServersConfig(replication, src, target);
        addAdditionalReplicationProperties(replication);
        addTransition(replication, IMPORT_ACTION_NAME, getFailAction());
        workflow.getDecisionOrForkOrJoin().add(replication);

        //Add import action
        ACTION importAction = unmarshalAction(IMPORT_ACTION_TEMPLATE);
        JAXBElement<org.apache.falcon.oozie.hive.ACTION> importActionJaxbElement =
            OozieUtils.unMarshalHiveAction(importAction);
        org.apache.falcon.oozie.hive.ACTION hiveImportAction = importActionJaxbElement.getValue();
        addHDFSServersConfig(hiveImportAction, src, target);
        Properties hiveConf2 = HiveUtil.getHiveCredentials(target);
        for (Map.Entry<Object, Object> e : hiveConf2.entrySet()) {
            org.apache.falcon.oozie.hive.CONFIGURATION.Property prop =
                    new org.apache.falcon.oozie.hive.CONFIGURATION.Property();
            prop.setName((String) e.getKey());
            prop.setValue((String) e.getValue());
            LOG.info("Adding config to replication hive import action : key = {}  value = {}",
                    e.getKey(), e.getValue());
            hiveImportAction.getConfiguration().getProperty().add(prop);
        }
        OozieUtils.marshalHiveAction(importAction, importActionJaxbElement);
        addTransition(importAction, CLEANUP_ACTION_NAME, getFailAction());
        workflow.getDecisionOrForkOrJoin().add(importAction);

        //Add cleanup action
        ACTION cleanup = unmarshalAction(CLEANUP_ACTION_TEMPLATE);
        addPostProcessing(workflow, cleanup);
        decorateWorkflow(workflow, wfName, start);
        setupHiveCredentials(src, target, workflow);
        return workflow;
    }

    private void setupHiveCredentials(Cluster sourceCluster, Cluster targetCluster, WORKFLOWAPP workflowApp) {
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
            if (PREPROCESS_ACTION_NAME.equals(actionName)) {

                // add reference to hive-site conf to each action

                if (isSecurityEnabled) { // add a reference to credential in the action
                    action.setCred(SOURCE_HIVE_CREDENTIAL_NAME);
                }
            } else if (EXPORT_ACTION_NAME.equals(actionName)) {
                if (isSecurityEnabled) { // add a reference to credential in the action
                    action.setCred(SOURCE_HIVE_CREDENTIAL_NAME);
                }
            } else if (IMPORT_ACTION_NAME.equals(actionName)) {
                if (isSecurityEnabled) { // add a reference to credential in the action
                    action.setCred(TARGET_HIVE_CREDENTIAL_NAME);
                }
            }
        }
    }

    protected Properties getWorkflowProperties(Feed feed) throws FalconException {
        Properties props = super.getWorkflowProperties(feed);
        props.put("availabilityFlag", "NA");
        props.put(WorkflowExecutionArgs.DATASOURCE_NAME.getName(), "NA");
        return props;
    }

    private org.apache.falcon.oozie.hive.ACTION addHDFSServersConfig(org.apache.falcon.oozie.hive.ACTION action,
        Cluster sourceCluster, Cluster targetCluster) {
        if (isSecurityEnabled) {
            // this is to ensure that the delegation tokens are checked out for both clusters
            org.apache.falcon.oozie.hive.CONFIGURATION.Property hiveProperty = new org.apache.falcon.oozie.hive
                    .CONFIGURATION.Property();
            hiveProperty.setName("oozie.launcher.mapreduce.job.hdfs-servers");
            hiveProperty.setValue(ClusterHelper.getReadOnlyStorageUrl(sourceCluster)
                    + "," + ClusterHelper.getStorageUrl(targetCluster));
            action.getConfiguration().getProperty().add(hiveProperty);
        }
        return action;
    }

}
