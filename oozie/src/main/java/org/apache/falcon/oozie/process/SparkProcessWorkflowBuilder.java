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
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.oozie.spark.CONFIGURATION.Property;
import org.apache.falcon.oozie.workflow.ACTION;
import org.apache.falcon.oozie.workflow.CONFIGURATION;
import org.apache.falcon.util.OozieUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.xml.bind.JAXBElement;
import java.util.List;

/**
 * Builds orchestration workflow for process where engine is spark.
 */
public class SparkProcessWorkflowBuilder extends ProcessExecutionWorkflowBuilder {
    private static final String ACTION_TEMPLATE = "/action/process/spark-action.xml";
    private static final String FALCON_PREFIX = "falcon_";

    public SparkProcessWorkflowBuilder(Process entity) {
        super(entity);
    }

    @Override
    protected ACTION getUserAction(Cluster cluster, Path buildPath) throws FalconException {
        ACTION action = unmarshalAction(ACTION_TEMPLATE);
        JAXBElement<org.apache.falcon.oozie.spark.ACTION> actionJaxbElement = OozieUtils.unMarshalSparkAction(action);
        org.apache.falcon.oozie.spark.ACTION sparkAction = actionJaxbElement.getValue();

        String sparkMasterURL = entity.getSparkAttributes().getMaster();
        String sparkFilePath = entity.getSparkAttributes().getJar();
        String sparkJobName = entity.getSparkAttributes().getName();
        String sparkOpts = entity.getSparkAttributes().getSparkOpts();
        String sparkClassName = entity.getSparkAttributes().getClazz();

        String clusterEntitySparkMasterURL = getClusterEntitySparkMaster(cluster);

        //Overriding cluster spark master url if defined in process entity
        sparkMasterURL = (sparkMasterURL == null) ? clusterEntitySparkMasterURL : sparkMasterURL;
        if (StringUtils.isBlank(sparkMasterURL)) {
            throw new FalconException("Spark Master URL can'be empty");
        }
        sparkAction.setMaster(sparkMasterURL);
        sparkAction.setName(sparkJobName);

        addPrepareDeleteOutputPath(sparkAction);

        if (StringUtils.isNotEmpty(sparkOpts)) {
            sparkAction.setSparkOpts(sparkOpts);
        }

        if (StringUtils.isNotEmpty(sparkClassName)) {
            sparkAction.setClazz(sparkClassName);
        }

        List<String> argList = sparkAction.getArg();
        List<String> sparkArgs = entity.getSparkAttributes().getArgs();
        if (sparkArgs != null) {
            argList.addAll(sparkArgs);
        }

        //Adding output first so that final order must have input and then output followed by user's arguments.
        addOutputFeedsAsArgument(argList, cluster);
        addInputFeedsAsArgument(argList, cluster);

        sparkAction.setJar(addUri(sparkFilePath, cluster));

        setSparkLibFileToWorkflowLib(sparkFilePath, entity);
        propagateEntityProperties(sparkAction);

        OozieUtils.marshalSparkAction(action, actionJaxbElement);
        return action;
    }

    private void setSparkLibFileToWorkflowLib(String sparkFile, Process entity) {
        if (StringUtils.isEmpty(entity.getWorkflow().getLib())) {
            entity.getWorkflow().setLib(sparkFile);
        }
    }

    private void addPrepareDeleteOutputPath(org.apache.falcon.oozie.spark.ACTION sparkAction) throws FalconException {
        List<String> deleteOutputPathList = getPrepareDeleteOutputPathList();
        if (deleteOutputPathList.isEmpty()) {
            return;
        }

        org.apache.falcon.oozie.spark.PREPARE prepare = new org.apache.falcon.oozie.spark.PREPARE();
        List<org.apache.falcon.oozie.spark.DELETE> deleteList = prepare.getDelete();

        for (String deletePath : deleteOutputPathList) {
            org.apache.falcon.oozie.spark.DELETE delete = new org.apache.falcon.oozie.spark.DELETE();
            delete.setPath(deletePath);
            deleteList.add(delete);
        }

        if (!deleteList.isEmpty()) {
            sparkAction.setPrepare(prepare);
        }
    }

    private void propagateEntityProperties(org.apache.falcon.oozie.spark.ACTION sparkAction) {
        CONFIGURATION conf = new CONFIGURATION();
        super.propagateEntityProperties(conf, null);

        List<Property> sparkConf = sparkAction.getConfiguration().getProperty();
        for (CONFIGURATION.Property prop : conf.getProperty()) {
            Property sparkProp = new Property();
            sparkProp.setName(prop.getName());
            sparkProp.setValue(prop.getValue());
            sparkConf.add(sparkProp);
        }
    }

    private void addInputFeedsAsArgument(List<String> argList, Cluster cluster) throws FalconException {
        if (entity.getInputs() == null) {
            return;
        }

        //Adding to the 0th index and getting the args shifted as arguments are added to get the desired effect.
        int numInputFeed = entity.getInputs().getInputs().size();
        while (numInputFeed > 0) {
            Input input = entity.getInputs().getInputs().get(numInputFeed-1);
            Feed feed = EntityUtil.getEntity(EntityType.FEED, input.getFeed());
            Storage storage = FeedHelper.createStorage(cluster, feed);
            final String inputName = input.getName();
            if (storage.getType() == Storage.TYPE.FILESYSTEM) {
                argList.add(0, "${" + inputName + "}");
            } else if (storage.getType() == Storage.TYPE.TABLE) {
                argList.add(0, "${" + FALCON_PREFIX+inputName+"_database" + "}");
                argList.add(0, "${" + FALCON_PREFIX+inputName+"_table" + "}");
                argList.add(0, "${" + FALCON_PREFIX+inputName+"_partition_filter_hive" + "}");
            }
            numInputFeed--;
        }
    }

    private void addOutputFeedsAsArgument(List<String> argList, Cluster cluster) throws FalconException {
        if (entity.getOutputs() == null) {
            return;
        }

        //Adding to the 0th index and getting the args shifted as arguments are added to get the desired effect.
        int numOutputFeed = entity.getOutputs().getOutputs().size();
        while (numOutputFeed > 0) {
            Output output = entity.getOutputs().getOutputs().get(numOutputFeed-1);
            Feed feed = EntityUtil.getEntity(EntityType.FEED, output.getFeed());
            Storage storage = FeedHelper.createStorage(cluster, feed);
            final String outputName = output.getName();
            if (storage.getType() == Storage.TYPE.FILESYSTEM) {
                argList.add(0, "${" + outputName + "}");
            } else if (storage.getType() == Storage.TYPE.TABLE) {
                argList.add(0, "${" + FALCON_PREFIX+outputName+"_database" + "}");
                argList.add(0, "${" + FALCON_PREFIX+outputName+"_table" + "}");
                argList.add(0, "${" + FALCON_PREFIX+outputName+"_partitions_hive" + "}");
            }
            numOutputFeed--;
        }
    }

    private String addUri(String jarFile, Cluster cluster) throws FalconException {
        FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(
                ClusterHelper.getConfiguration(cluster));
        Path jarFilePath = new Path(jarFile);
        if (jarFilePath.isAbsoluteAndSchemeAuthorityNull()) {
            return fs.makeQualified(jarFilePath).toString();
        }
        return jarFile;
    }

    private String getClusterEntitySparkMaster(Cluster cluster) {
        return ClusterHelper.getSparkMasterEndPoint(cluster);
    }
}
