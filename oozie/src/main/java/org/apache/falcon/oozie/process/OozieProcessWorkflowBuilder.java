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
import org.apache.falcon.oozie.workflow.ACTION;
import org.apache.hadoop.fs.Path;

/**
 * Builds oozie workflow for process where the engine is oozie.
 */
public class OozieProcessWorkflowBuilder extends ProcessExecutionWorkflowBuilder {
    private static final String ACTION_TEMPLATE = "/action/process/oozie-action.xml";

    public OozieProcessWorkflowBuilder(Process entity) {
        super(entity);
    }

    @Override protected ACTION getUserAction(Cluster cluster, Path buildPath) throws FalconException {
        ACTION action = unmarshalAction(ACTION_TEMPLATE);
        action.getSubWorkflow().setAppPath(getStoragePath(entity.getWorkflow().getPath()));
        return action;
    }
}
