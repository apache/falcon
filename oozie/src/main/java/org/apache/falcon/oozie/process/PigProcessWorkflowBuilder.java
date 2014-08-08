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
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.ProcessHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.oozie.workflow.ACTION;
import org.apache.falcon.oozie.workflow.DELETE;
import org.apache.falcon.oozie.workflow.PIG;
import org.apache.falcon.oozie.workflow.PREPARE;
import org.apache.hadoop.fs.Path;

import java.util.List;

/**
 * Builds orchestration workflow for process where engine is pig.
 */
public class PigProcessWorkflowBuilder extends ProcessExecutionWorkflowBuilder {
    private static final String ACTION_TEMPLATE = "/action/process/pig-action.xml";

    public PigProcessWorkflowBuilder(Process entity) {
        super(entity);
    }

    @Override protected ACTION getUserAction(Cluster cluster, Path buildPath) throws FalconException {
        ACTION action = unmarshalAction(ACTION_TEMPLATE);

        PIG pigAction = action.getPig();
        Path userWfPath = ProcessHelper.getUserWorkflowPath(entity, cluster, buildPath);
        pigAction.setScript(getStoragePath(userWfPath));

        addPrepareDeleteOutputPath(pigAction);

        final List<String> paramList = pigAction.getParam();
        addInputFeedsAsParams(paramList, cluster);
        addOutputFeedsAsParams(paramList, cluster);

        propagateEntityProperties(pigAction.getConfiguration(), pigAction.getParam());

        if (EntityUtil.isTableStorageType(cluster, entity)) { // adds hive-site.xml in pig classpath
            pigAction.getFile().add("${wf:appPath()}/conf/hive-site.xml");
        }

        addArchiveForCustomJars(cluster, pigAction.getArchive(), getLibPath(cluster, buildPath));

        return action;
    }

    private void addPrepareDeleteOutputPath(PIG pigAction) throws FalconException {
        List<String> deleteOutputPathList = getPrepareDeleteOutputPathList();
        if (deleteOutputPathList.isEmpty()) {
            return;
        }

        final PREPARE prepare = new PREPARE();
        final List<DELETE> deleteList = prepare.getDelete();

        for (String deletePath : deleteOutputPathList) {
            final DELETE delete = new DELETE();
            delete.setPath(deletePath);
            deleteList.add(delete);
        }

        if (!deleteList.isEmpty()) {
            pigAction.setPrepare(prepare);
        }
    }

}
