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

package org.apache.falcon.oozie.process;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Property;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.util.DateUtil;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.hadoop.fs.Path;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * Workflow Builder for oozie process in case of Native Scheduler.
 */
public class NativeOozieProcessWorkflowBuilder extends OozieProcessWorkflowBuilder {

    private static final ExpressionHelper EXPRESSION_HELPER = ExpressionHelper.get();
    private static final String INSTANCE_FORMAT = "yyyy-MM-dd-HH-mm";

    public NativeOozieProcessWorkflowBuilder(org.apache.falcon.entity.v0.process.Process entity) {
        super(entity);
    }

    @Override
    public java.util.Properties build(Cluster cluster,
                                      Path buildPath, Properties suppliedProps) throws FalconException {
        Properties elProps = new Properties();
        DateTimeFormatter fmt = DateTimeFormat.forPattern(INSTANCE_FORMAT);
        elProps.put(WorkflowExecutionArgs.NOMINAL_TIME.getName(), fmt.print(getNominalTime()));
        elProps.put(WorkflowExecutionArgs.TIMESTAMP.getName(), fmt.print(getNominalTime()));
        elProps.put(WorkflowExecutionArgs.USER_JMS_NOTIFICATION_ENABLED.getName(), "true");
        elProps.put(WorkflowExecutionArgs.SYSTEM_JMS_NOTIFICATION_ENABLED.getName(), "false"); //check true or false


        DateUtil.setTimeZone(entity.getTimezone().getID());
        ExpressionHelper.setReferenceDate(new Date(getNominalTime().getMillis()));
        elProps.putAll(getInputProps(cluster));
        elProps.putAll(getOutputProps());
        elProps.putAll(evalProperties());
        Properties buildProps  = build(cluster, buildPath);
        buildProps.putAll(elProps);
        copyPropsWithoutOverride(buildProps, suppliedProps);
        return buildProps;
    }

    private void copyPropsWithoutOverride(Properties buildProps, Properties suppliedProps) {
        if (suppliedProps == null || suppliedProps.isEmpty()) {
            return;
        }
        for (String propertyName : suppliedProps.stringPropertyNames()) {
            if (buildProps.containsKey(propertyName)) {
                LOG.warn("User provided property {} is already declared in the entity and will be ignored.",
                    propertyName);
                continue;
            }
            String propertyValue = suppliedProps.getProperty(propertyName);
            buildProps.put(propertyName, propertyValue);
        }
    }

    private Properties evalProperties() throws FalconException {
        Properties props = new Properties();
        org.apache.falcon.entity.v0.process.Properties processProps = entity.getProperties();
        for (Property property : processProps.getProperties()) {
            String propName = property.getName();
            String propValue = property.getValue();
            String evalExp = EXPRESSION_HELPER.evaluateFullExpression(propValue, String.class);
            props.put(propName, evalExp);
        }
        return props;
    }

    private Properties getOutputProps() throws FalconException {
        Properties props = new Properties();
        if (entity.getOutputs() == null) {
            props.put(WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName(), NONE);
            props.put(WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName(), NONE);
            props.put(WorkflowExecutionArgs.OUTPUT_NAMES.getName(), NONE);
            return props;
        }
        List<String> falconOutputFeeds = new LinkedList<>();
        List<String> feedInstancePaths= new LinkedList<>();
        List<String> falconOutputNames = new LinkedList<>();
        for (Output output : entity.getOutputs().getOutputs()) {
            Feed feed = ConfigurationStore.get().get(EntityType.FEED, output.getFeed());
            falconOutputFeeds.add(feed.getName());
            falconOutputNames.add(output.getName());
            String outputExp = output.getInstance();
            Date outTime = EXPRESSION_HELPER.evaluate(outputExp, Date.class);
            for (org.apache.falcon.entity.v0.feed.Cluster cluster : feed.getClusters().getClusters()) {
                org.apache.falcon.entity.v0.cluster.Cluster clusterEntity =
                        EntityUtil.getEntity(EntityType.CLUSTER, cluster.getName());
                if (!EntityUtil.responsibleFor(clusterEntity.getColo())) {
                    continue;
                }

                List<Location> locations = FeedHelper.getLocations(cluster, feed);
                for (Location loc : locations) {
                    String path = EntityUtil.evaluateDependentPath(loc.getPath(), outTime);
                    path = getStoragePath(path);
                    if (loc.getType() != LocationType.DATA) {
                        props.put(output.getName() + "." + loc.getType().toString().toLowerCase(), path);
                    } else {
                        props.put(output.getName(), path);
                    }
                    feedInstancePaths.add(path);
                }
            }
        }
        props.put(WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName(), StringUtils.join(falconOutputFeeds, ","));
        props.put(WorkflowExecutionArgs.OUTPUT_NAMES.getName(), StringUtils.join(falconOutputNames, ","));
        props.put(WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName(), StringUtils.join(feedInstancePaths, ","));
        return props;
    }

    private Properties getInputProps(Cluster clusterObj) throws FalconException {
        Properties props = new Properties();

        if (entity.getInputs() == null) {
            props.put(WorkflowExecutionArgs.INPUT_FEED_NAMES.getName(), NONE);
            props.put(WorkflowExecutionArgs.INPUT_FEED_PATHS.getName(), NONE);
            props.put(WorkflowExecutionArgs.INPUT_NAMES.getName(), NONE);
            props.put(WorkflowExecutionArgs.INPUT_STORAGE_TYPES.getName(), NONE);
            return props;
        }
        List<String> falconInputFeeds = new LinkedList<>();
        List<String> falconInputNames = new LinkedList<>();
        List<String> falconInputPaths = new LinkedList<>();
        List<String> falconInputFeedStorageTypes = new LinkedList<>();
        for (Input input : entity.getInputs().getInputs()) {
            Feed feed = ConfigurationStore.get().get(EntityType.FEED, input.getFeed());
            Storage storage = FeedHelper.createStorage(clusterObj, feed);
            if (storage.getType() != Storage.TYPE.FILESYSTEM) {
                throw new UnsupportedOperationException("Storage Type not supported " + storage.getType());
            }
            falconInputFeeds.add(feed.getName());
            falconInputNames.add(input.getName());
            falconInputFeedStorageTypes.add(storage.getType().name());
            String partition  = input.getPartition();

            String startTimeExp = input.getStart();
            String endTimeExp = input.getEnd();
            ExpressionHelper.setReferenceDate(new Date(getNominalTime().getMillis()));
            Date startTime = EXPRESSION_HELPER.evaluate(startTimeExp, Date.class);
            Date endTime = EXPRESSION_HELPER.evaluate(endTimeExp, Date.class);

            for (org.apache.falcon.entity.v0.feed.Cluster cluster : feed.getClusters().getClusters()) {
                org.apache.falcon.entity.v0.cluster.Cluster clusterEntity =
                        EntityUtil.getEntity(EntityType.CLUSTER, cluster.getName());
                if (!EntityUtil.responsibleFor(clusterEntity.getColo())) {
                    continue;
                }

                List<Location> locations = FeedHelper.getLocations(cluster, feed);
                for (Location loc : locations) {
                    if (loc.getType() != LocationType.DATA) {
                        continue;
                    }
                    List<String> paths = new ArrayList<>();
                    List<Date> instanceTimes = EntityUtil.getEntityInstanceTimes(feed, cluster.getName(),
                            startTime, endTime); // test when startTime and endTime are equal.
                    for (Date instanceTime : instanceTimes) {
                        String path = EntityUtil.evaluateDependentPath(loc.getPath(), instanceTime);
                        if (StringUtils.isNotBlank(partition)) {
                            if (!path.endsWith("/") && !partition.startsWith("/")) {
                                path = path + "/";
                            }
                            path = path + partition;
                        }
                        path = getStoragePath(path);
                        paths.add(path);
                    }
                    if (loc.getType() != LocationType.DATA) {
                        props.put(input.getName() + "." + loc.getType().toString().toLowerCase(),
                                StringUtils.join(paths, ","));
                    } else {
                        props.put(input.getName(), StringUtils.join(paths, ","));
                    }
                    falconInputPaths.add(StringUtils.join(paths, ","));
                }
            }
        }
        props.put(WorkflowExecutionArgs.INPUT_FEED_NAMES.getName(), StringUtils.join(falconInputFeeds, "#"));
        props.put(WorkflowExecutionArgs.INPUT_NAMES.getName(), StringUtils.join(falconInputNames, "#"));
        props.put(WorkflowExecutionArgs.INPUT_FEED_PATHS.getName(), StringUtils.join(falconInputPaths, "#"));
        props.put(WorkflowExecutionArgs.INPUT_STORAGE_TYPES.getName(),
                StringUtils.join(falconInputFeedStorageTypes, "#"));
        return props;
    }

}
