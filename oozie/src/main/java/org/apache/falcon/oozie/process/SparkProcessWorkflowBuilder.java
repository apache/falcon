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
import org.apache.falcon.entity.v0.cluster.Cluster;
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
            LOG.error("Spark Master URL can'be empty");
            return null;
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

        checkDataFeedsAsArgument(entity);
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

    private void checkDataFeedsAsArgument(Process entity) throws FalconException {
        if (entity.getInputs() == null) {
            LOG.error("Input Feed Entity is null for process : {}" +entity.getName());
            return;
        }

        if (entity.getOutputs() == null) {
            LOG.error("Output Feed Entity is null for process : {}" +entity.getName());
            return;
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
