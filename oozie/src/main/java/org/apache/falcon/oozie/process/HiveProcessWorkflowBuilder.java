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

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.oozie.hive.CONFIGURATION.Property;
import org.apache.falcon.oozie.workflow.ACTION;
import org.apache.falcon.oozie.workflow.CONFIGURATION;
import org.apache.falcon.util.OozieUtils;
import org.apache.hadoop.fs.Path;

import javax.xml.bind.JAXBElement;
import java.util.List;

/**
 * Builds orchestration workflow for process where engine is hive.
 */
public class HiveProcessWorkflowBuilder extends ProcessExecutionWorkflowBuilder {
    private static final String ACTION_TEMPLATE = "/action/process/hive-action.xml";

    public HiveProcessWorkflowBuilder(Process entity) {
        super(entity);
    }

    @Override protected ACTION getUserAction(Cluster cluster, Path buildPath) throws FalconException {
        ACTION action = unmarshalAction(ACTION_TEMPLATE);

        JAXBElement<org.apache.falcon.oozie.hive.ACTION> actionJaxbElement = OozieUtils.unMarshalHiveAction(action);
        org.apache.falcon.oozie.hive.ACTION hiveAction = actionJaxbElement.getValue();

        Path userWfPath = new Path(entity.getWorkflow().getPath());
        hiveAction.setScript(getStoragePath(userWfPath));

        addPrepareDeleteOutputPath(hiveAction);

        final List<String> paramList = hiveAction.getParam();
        addInputFeedsAsParams(paramList, cluster);
        addOutputFeedsAsParams(paramList, cluster);

        propagateEntityProperties(hiveAction);

        // adds hive-site.xml in hive classpath
        hiveAction.setJobXml("${wf:appPath()}/conf/hive-site.xml");

        addArchiveForCustomJars(cluster, hiveAction.getArchive(), entity.getWorkflow().getLib());

        OozieUtils.marshalHiveAction(action, actionJaxbElement);
        return action;
    }

    private void propagateEntityProperties(org.apache.falcon.oozie.hive.ACTION hiveAction) {
        CONFIGURATION conf = new CONFIGURATION();
        super.propagateEntityProperties(conf, hiveAction.getParam());

        List<Property> hiveConf = hiveAction.getConfiguration().getProperty();
        for (CONFIGURATION.Property prop : conf.getProperty()) {
            Property hiveProp = new Property();
            hiveProp.setName(prop.getName());
            hiveProp.setValue(prop.getValue());
            hiveConf.add(hiveProp);
        }
    }

    private void addPrepareDeleteOutputPath(org.apache.falcon.oozie.hive.ACTION hiveAction) throws FalconException {

        List<String> deleteOutputPathList = getPrepareDeleteOutputPathList();
        if (deleteOutputPathList.isEmpty()) {
            return;
        }

        org.apache.falcon.oozie.hive.PREPARE prepare = new org.apache.falcon.oozie.hive.PREPARE();
        List<org.apache.falcon.oozie.hive.DELETE> deleteList = prepare.getDelete();

        for (String deletePath : deleteOutputPathList) {
            org.apache.falcon.oozie.hive.DELETE delete = new org.apache.falcon.oozie.hive.DELETE();
            delete.setPath(deletePath);
            deleteList.add(delete);
        }

        if (!deleteList.isEmpty()) {
            hiveAction.setPrepare(prepare);
        }
    }
}
