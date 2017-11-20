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

package org.apache.falcon.lifecycle.engine.oozie.archival;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Property;
import org.apache.falcon.lifecycle.engine.oozie.utils.OozieBuilderUtils;
import org.apache.falcon.oozie.workflow.*;
import org.apache.falcon.util.ReplicationDistCpOption;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Properties;

public class AgeBasedArchivalWorkflowBuilder {
    private static final String ARCHIVAL_ACTION_TEMPLATE = "/action/feed/archival-action.xml";
    private static final String ARCHIVAL_ACTION_NAME = "archival";

    private static final String ARCHIVAL_JOB_COUNTER = "job.counter";
    private static final String TDE_ENCRYPTION_ENABLED = "tdeEncryptionEnabled";
    private static final String MR_MAX_MAPS = "maxMaps";
    private static final String MR_MAP_BANDWIDTH = "mapBandwidth";

    private AgeBasedArchivalWorkflowBuilder(){

    }

    public static Properties build(Cluster cluster, Path basePath, Feed feed) throws FalconException {
        Path buildPath = OozieBuilderUtils.getBuildPath(basePath, LifeCycle.ARCHIVAL.getTag());
        WORKFLOWAPP workflow = new WORKFLOWAPP();
        String wfName = EntityUtil.getWorkflowName(Tag.ARCHIVAL, feed).toString();

        String start = ARCHIVAL_ACTION_NAME;

        //Add pre-processing
        if (shouldPreProcess(feed)) {
            ACTION action = OozieBuilderUtils.getPreProcessingAction(Tag.ARCHIVAL);
            addTransition(action, ARCHIVAL_ACTION_NAME, OozieBuilderUtils.getFailAction());
            workflow.getDecisionOrForkOrJoin().add(action);
            start = OozieBuilderUtils.PREPROCESS_ACTION_NAME;
        }

        //Add replication
        ACTION archival = OozieBuilderUtils.unmarshalAction(ARCHIVAL_ACTION_TEMPLATE);
        addAdditionalArchivalProperties(feed, archival);
        enableCounters(feed, archival);
        enableTDE(feed, archival);
        addPostProcessing(workflow, archival);
        OozieBuilderUtils.decorateWorkflow(workflow, wfName, start);

        OozieBuilderUtils.addLibExtensionsToWorkflow(cluster, workflow, Tag.ARCHIVAL, EntityType.FEED);

        OozieBuilderUtils.marshalWokflow(cluster, workflow, buildPath);


        Properties props = OozieBuilderUtils.getProperties(buildPath, wfName);
        props.putAll(OozieBuilderUtils.createDefaultConfiguration(cluster, feed, WorkflowExecutionContext.EntityOperations.REPLICATE));

        props.putAll(getWorkflowProperties(feed, cluster));
        props.putAll(FeedHelper.getUserWorkflowProperties(LifeCycle.ARCHIVAL));

        // Write out the config to config-default.xml
        OozieBuilderUtils.marshalDefaultConfig(cluster, workflow, props, buildPath);
        return props;

    }

    private static Properties getWorkflowProperties(Feed feed, Cluster cluster) throws FalconException {
        Properties props = FeedHelper.getFeedProperties(feed);
        if (props.getProperty(MR_MAX_MAPS) == null) { // set default if user has not overridden
            props.put(MR_MAX_MAPS, getDefaultMaxMaps());
        }
        if (props.getProperty(MR_MAP_BANDWIDTH) == null) { // set default if user has not overridden
            props.put(MR_MAP_BANDWIDTH, getDefaultMapBandwidth());
        }

        if (feed.getAvailabilityFlag() == null) {
            props.put("availabilityFlag", "NA");
        } else {
            props.put("availabilityFlag", feed.getAvailabilityFlag());
        }
        props.put(WorkflowExecutionArgs.DATASOURCE_NAME.getName(), "NA");

        return props;
    }

    private static String getDefaultMaxMaps() {
        return RuntimeProperties.get().getProperty("falcon.replication.workflow.maxmaps", "5");
    }

    private static String getDefaultMapBandwidth() {
        return RuntimeProperties.get().getProperty("falcon.replication.workflow.mapbandwidth", "100");
    }

    private static boolean shouldPreProcess(Feed feed) throws FalconException {
        return !(EntityUtil.getLateProcess(feed) == null
                || EntityUtil.getLateProcess(feed).getLateInputs() == null
                || EntityUtil.getLateProcess(feed).getLateInputs().size() == 0);
    }

    private static void addTransition(ACTION action, String ok, String fail) {
        action.getOk().setTo(ok);
        action.getError().setTo(fail);
    }

    private static void addAdditionalArchivalProperties(Feed feed, ACTION archivalAction) {
        List<String> args = archivalAction.getJava().getArg();
        Properties props = EntityUtil.getEntityProperties(feed);

        for (ReplicationDistCpOption distcpOption : ReplicationDistCpOption.values()) {
            String propertyValue = props.getProperty(distcpOption.getName());
            if (StringUtils.isNotEmpty(propertyValue)) {
                args.add("-" + distcpOption.getName());
                args.add(propertyValue);
            }
        }
    }

    private static ACTION enableCounters(Feed feed, ACTION action) throws FalconException {
        if (isCounterEnabled(feed)) {
            List<String> args = action.getJava().getArg();
            args.add("-counterLogDir");
            args.add("${logDir}/job-${nominalTime}/${srcClusterName == 'NA' ? '' : srcClusterName}");
        }
        return action;
    }

    private static boolean isCounterEnabled(Feed feed) throws FalconException {
        if (feed.getProperties() != null) {
            List<Property> propertyList = feed.getProperties().getProperties();
            for (Property prop : propertyList) {
                if (prop.getName().equals(ARCHIVAL_JOB_COUNTER) && "true" .equalsIgnoreCase(prop.getValue())) {
                    return true;
                }
            }
        }
        return false;
    }

    private static ACTION enableTDE(Feed feed, ACTION action) throws FalconException {
        if (isTDEEnabled(feed)) {
            List<String> args = action.getJava().getArg();
            args.add("-tdeEncryptionEnabled");
            args.add("true");
        }
        return action;
    }

    private static boolean isTDEEnabled(Feed feed) {
        String tdeEncryptionEnabled = FeedHelper.getPropertyValue(feed, TDE_ENCRYPTION_ENABLED);
        return "true" .equalsIgnoreCase(tdeEncryptionEnabled);
    }

    private static void addPostProcessing(WORKFLOWAPP workflow, ACTION action) throws FalconException{
        if (!Boolean.parseBoolean(OozieBuilderUtils.ENABLE_POSTPROCESSING)){
            OozieBuilderUtils.addTransition(action, OozieBuilderUtils.OK_ACTION_NAME,
                    OozieBuilderUtils.FAIL_ACTION_NAME);
            workflow.getDecisionOrForkOrJoin().add(action);
        } else {
            OozieBuilderUtils.addTransition(action, OozieBuilderUtils.SUCCESS_POSTPROCESS_ACTION_NAME,
                    OozieBuilderUtils.FAIL_POSTPROCESS_ACTION_NAME);
            workflow.getDecisionOrForkOrJoin().add(action);

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
    }

}


