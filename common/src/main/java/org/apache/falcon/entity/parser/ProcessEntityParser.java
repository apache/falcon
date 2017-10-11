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

package org.apache.falcon.entity.parser;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.process.Properties;
import org.apache.falcon.entity.v0.process.Property;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.ACL;
import org.apache.falcon.entity.v0.process.EngineType;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Inputs;
import org.apache.falcon.entity.v0.process.LateInput;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Outputs;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.SparkAttributes;
import org.apache.falcon.entity.v0.process.Workflow;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.util.DateUtil;
import org.apache.falcon.util.HadoopQueueUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

/**
 * Concrete Parser which has XML parsing and validation logic for Process XML.
 */
public class ProcessEntityParser extends EntityParser<Process> {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessEntityParser.class);

    public ProcessEntityParser() {
        super(EntityType.PROCESS);
    }

    @Override
    public void validate(Process process) throws FalconException {
        validate(process, true);
    }

    public void validate(Process process, boolean checkDependentFeeds) throws FalconException {
        if (process.getTimezone() == null) {
            process.setTimezone(TimeZone.getTimeZone("UTC"));
        }

        validateACL(process);
        // check if dependent entities exists
        Set<String> clusters = new HashSet<String>();
        for (org.apache.falcon.entity.v0.process.Cluster cluster : process.getClusters().getClusters()) {
            String clusterName = cluster.getName();
            if (!clusters.add(cluster.getName())) {
                throw new ValidationException("Cluster: " + cluster.getName()
                        + " is defined more than once for process: " + process.getName());
            }
            validateEntityExists(EntityType.CLUSTER, clusterName);

            // Optinal end_date
            if (cluster.getValidity().getEnd() == null) {
                cluster.getValidity().setEnd(DateUtil.NEVER);
            }

            // set Cluster version
            int clusterVersion = ClusterHelper.getCluster(cluster.getName()).getVersion();
            if (cluster.getVersion() > 0 && cluster.getVersion() > clusterVersion) {
                throw new ValidationException("Process should not set cluster to a version that does not exist");
            } else {
                cluster.setVersion(clusterVersion);
            }

            validateProcessValidity(cluster.getValidity().getStart(), cluster.getValidity().getEnd());
            validateHDFSPaths(process, clusterName);
            validateProperties(process);

            if (checkDependentFeeds) {
                if (process.getInputs() != null) {
                    for (Input input : process.getInputs().getInputs()) {
                        validateEntityExists(EntityType.FEED, input.getFeed());
                        Feed feed = ConfigurationStore.get().get(EntityType.FEED, input.getFeed());
                        CrossEntityValidations.validateFeedDefinedForCluster(feed, clusterName);
                        CrossEntityValidations.validateFeedRetentionPeriod(input.getStart(), feed, clusterName);
                        CrossEntityValidations.validateInstanceRange(process, input, feed);
                        validateInputPartition(input, feed);
                        validateOptionalInputsForTableStorage(feed, input);
                    }
                }


                if (process.getOutputs() != null) {
                    for (Output output : process.getOutputs().getOutputs()) {
                        validateEntityExists(EntityType.FEED, output.getFeed());
                        Feed feed = ConfigurationStore.get().get(EntityType.FEED, output.getFeed());
                        CrossEntityValidations.validateFeedDefinedForCluster(feed, clusterName);
                        CrossEntityValidations.validateInstance(process, output, feed);
                    }
                }
            }
        }
        validateDatasetName(process.getInputs(), process.getOutputs());
        if (checkDependentFeeds) {
            validateLateInputs(process);
        }
        validateProcessSLA(process);
        validateHadoopQueue(process);
        validateProcessEntity(process);
    }


    private void validateProcessSLA(Process process) throws FalconException {
        if (process.getSla() != null) {
            ExpressionHelper evaluator = ExpressionHelper.get();
            ExpressionHelper.setReferenceDate(new Date());
            Frequency shouldStartExpression = process.getSla().getShouldStartIn();
            Frequency shouldEndExpression = process.getSla().getShouldEndIn();
            Frequency timeoutExpression = process.getTimeout();

            if (shouldStartExpression != null){
                Date shouldStart = new Date(evaluator.evaluate(shouldStartExpression.toString(), Long.class));

                if (shouldEndExpression != null) {
                    Date shouldEnd = new Date(evaluator.evaluate(shouldEndExpression.toString(), Long.class));
                    if (shouldStart.after(shouldEnd)) {
                        throw new ValidationException("shouldStartIn of Process: " + shouldStartExpression
                                + "is greater than shouldEndIn: "
                                + shouldEndExpression);
                    }
                }

                if (timeoutExpression != null) {
                    Date timeout = new Date(evaluator.evaluate(timeoutExpression.toString(), Long.class));
                    if (timeout.before(shouldStart)) {
                        throw new ValidationException("shouldStartIn of Process: " + shouldStartExpression
                                + " is greater than timeout: " + process.getTimeout());
                    }
                }
            }
        }
    }
    /**
     * Validate if the user submitting this entity has access to the specific dirs on HDFS.
     *
     * @param process process
     * @param clusterName cluster the process is materialized on
     * @throws FalconException
     */
    private void validateHDFSPaths(Process process, String clusterName) throws FalconException {
        org.apache.falcon.entity.v0.cluster.Cluster cluster =
                ConfigurationStore.get().get(EntityType.CLUSTER, clusterName);

        if (!EntityUtil.responsibleFor(cluster.getColo())) {
            return;
        }

        String workflowPath = process.getWorkflow().getPath();
        String libPath = process.getWorkflow().getLib();
        String nameNode = getNameNode(cluster);
        try {
            Configuration configuration = ClusterHelper.getConfiguration(cluster);
            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(configuration);
            if (!fs.exists(new Path(workflowPath))) {
                throw new ValidationException(
                        "Workflow path: " + workflowPath + " does not exists in HDFS: " + nameNode);
            }

            if (StringUtils.isNotBlank(libPath)) {
                String[] libPaths = libPath.split(EntityUtil.WF_LIB_SEPARATOR);
                for (String path : libPaths) {
                    if (!fs.exists(new Path(path))) {
                        throw new ValidationException("Lib path: " + path + " does not exists in HDFS: " + nameNode);
                    }
                }
            }
        } catch (IOException e) {
            throw new FalconException("Error validating workflow path " + workflowPath, e);
        }
    }

    private String getNameNode(Cluster cluster) throws ValidationException {
        // cluster should never be null as it is validated while submitting feeds.
        if (new Path(ClusterHelper.getStorageUrl(cluster)).toUri().getScheme() == null) {
            throw new ValidationException(
                    "Cannot get valid nameNode scheme from write interface of cluster: " + cluster.getName());
        }
        return ClusterHelper.getStorageUrl(cluster);
    }

    private void validateProcessValidity(Date start, Date end) throws FalconException {
        try {
            if (!start.before(end)) {
                throw new ValidationException(
                        "Process start time: " + start + " should be before process end time: " + end);
            }
        } catch (ValidationException e) {
            throw new ValidationException(e);
        } catch (Exception e) {
            throw new FalconException(e);
        }
    }

    private void validateInputPartition(Input input, Feed feed) throws FalconException {
        if (input.getPartition() == null) {
            return;
        }

        final Storage.TYPE baseFeedStorageType = FeedHelper.getStorageType(feed);
        if (baseFeedStorageType == Storage.TYPE.FILESYSTEM) {
            CrossEntityValidations.validateInputPartition(input, feed);
        } else if (baseFeedStorageType == Storage.TYPE.TABLE) {
            throw new ValidationException("Input partitions are not supported for table storage: " + input.getName());
        }
    }

    private void validateDatasetName(Inputs inputs, Outputs outputs) throws ValidationException {
        Set<String> datasetNames = new HashSet<String>();
        if (inputs != null) {
            for (Input input : inputs.getInputs()) {
                if (!datasetNames.add(input.getName())) {
                    throw new ValidationException("Input name: " + input.getName() + " is already used");
                }
            }
        }

        if (outputs != null) {
            for (Output output : outputs.getOutputs()) {
                if (!datasetNames.add(output.getName())) {
                    throw new ValidationException("Output name: " + output.getName() + " is already used");
                }
            }
        }
    }

    private void validateLateInputs(Process process) throws ValidationException {
        if (process.getLateProcess() == null) {
            return;
        }

        Map<String, String> feeds = new HashMap<String, String>();
        if (process.getInputs() != null) {
            for (Input in : process.getInputs().getInputs()) {
                feeds.put(in.getName(), in.getFeed());
            }
        }

        for (LateInput lp : process.getLateProcess().getLateInputs()) {
            if (!feeds.keySet().contains(lp.getInput())) {
                throw new ValidationException("Late Input: " + lp.getInput() + " is not specified in the inputs");
            }

            try {
                Feed feed = ConfigurationStore.get().get(EntityType.FEED, feeds.get(lp.getInput()));
                if (feed.getLateArrival() == null) {
                    throw new ValidationException(
                            "Late Input feed: " + lp.getInput() + " is not configured with late arrival cut-off");
                }
            } catch (FalconException e) {
                throw new ValidationException(e);
            }
        }
    }

    private void validateOptionalInputsForTableStorage(Feed feed, Input input) throws FalconException {
        if (input.isOptional() && FeedHelper.getStorageType(feed) == Storage.TYPE.TABLE) {
            throw new ValidationException("Optional Input is not supported for feeds with table storage! "
                    + input.getName());
        }
    }

    /**
     * Validate ACL if authorization is enabled.
     *
     * @param process process entity
     * @throws ValidationException
     */
    protected void validateACL(Process process) throws FalconException {
        if (isAuthorizationDisabled) {
            return;
        }

        // Validate the entity owner is logged-in, authenticated user if authorization is enabled
        ACL processACL = process.getACL();
        if (processACL == null) {
            throw new ValidationException("Process ACL cannot be empty for:  " + process.getName());
        }

        validateACLOwnerAndGroup(processACL);

        try {
            authorize(process.getName(), processACL);
        } catch (AuthorizationException e) {
            throw new ValidationException(e);
        }
    }

    protected void validateProperties(Process process) throws ValidationException {
        Properties properties = process.getProperties();
        if (properties == null) {
            return; // Cluster has no properties to validate.
        }

        List<Property> propertyList = process.getProperties().getProperties();
        HashSet<String> propertyKeys = new HashSet<String>();
        for (Property prop : propertyList) {
            if (StringUtils.isBlank(prop.getName())) {
                throw new ValidationException("Property name and value cannot be empty for Process : "
                        + process.getName());
            }
            if (!propertyKeys.add(prop.getName())) {
                throw new ValidationException("Multiple properties with same name found for Process : "
                        + process.getName());
            }
        }
    }

    private void validateHadoopQueue(Process process) throws FalconException {
        // get queue name specified in the process entity
        String processQueueName = null;
        java.util.Properties props = EntityUtil.getEntityProperties(process);
        if ((props != null) && (props.containsKey(EntityUtil.MR_QUEUE_NAME))) {
            processQueueName = props.getProperty(EntityUtil.MR_QUEUE_NAME);
        } else {
            return;
        }

        // iterate through each cluster in process entity to check if the cluster has the process entity queue
        for (org.apache.falcon.entity.v0.process.Cluster cluster : process.getClusters().getClusters()) {
            String clusterName = cluster.getName();
            org.apache.falcon.entity.v0.cluster.Cluster clusterEntity =
                    ConfigurationStore.get().get(EntityType.CLUSTER, clusterName);

            String rmURL = ClusterHelper.getPropertyValue(clusterEntity, "yarn.resourcemanager.webapp.https.address");
            if (rmURL == null) {
                rmURL = ClusterHelper.getPropertyValue(clusterEntity, "yarn.resourcemanager.webapp.address");
            }

            if (rmURL != null) {
                LOG.info("Fetching hadoop queue names from cluster {} RM URL {}", cluster.getName(), rmURL);
                Set<String> queueNames = HadoopQueueUtil.getHadoopClusterQueueNames(rmURL);

                if (queueNames.contains(processQueueName)) {
                    LOG.info("Validated presence of queue {} specified in process "
                            + "entity for cluster {}", processQueueName, clusterName);
                } else {
                    String strMsg = String.format("The hadoop queue name %s specified in process "
                            + "entity for cluster %s is invalid.", processQueueName, cluster.getName());
                    LOG.info(strMsg);
                    throw new FalconException(strMsg);
                }
            }
        }
    }

    protected void validateProcessEntity(Process process) throws FalconException {
        validateSparkProcessEntity(process, process.getSparkAttributes());
    }

    private void validateSparkProcessEntity(Process process, SparkAttributes sparkAttributes) throws
            FalconException {
        Workflow workflow = process.getWorkflow();
        if (workflow.getEngine() == EngineType.SPARK) {
            if (sparkAttributes == null) {
                throw new ValidationException(
                        "For Spark Workflow engine Spark Attributes in Process Entity can't be null");
            } else {
                String clusterName = process.getClusters().getClusters().get(0).getName();
                org.apache.falcon.entity.v0.cluster.Cluster cluster =
                        ConfigurationStore.get().get(EntityType.CLUSTER, clusterName);
                String clusterEntitySparkMaster = ClusterHelper.getSparkMasterEndPoint(cluster);
                String processEntitySparkMaster = sparkAttributes.getMaster();
                String sparkMaster = (processEntitySparkMaster == null)
                        ? clusterEntitySparkMaster
                        : processEntitySparkMaster;
                if (StringUtils.isEmpty(sparkMaster)
                        || StringUtils.isEmpty(sparkAttributes.getJar())) {
                    throw new ValidationException("Spark master and jar/python file can't be null");
                }
            }
        }
    }

}
