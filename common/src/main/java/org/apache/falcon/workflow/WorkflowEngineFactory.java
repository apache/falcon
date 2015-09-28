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

package org.apache.falcon.workflow;

import org.apache.falcon.FalconException;
import org.apache.falcon.lifecycle.AbstractPolicyBuilderFactory;
import org.apache.falcon.util.ReflectionUtils;
import org.apache.falcon.workflow.engine.AbstractWorkflowEngine;

/**
 * Factory for providing appropriate workflow engine to the falcon service.
 */
@SuppressWarnings("unchecked")
public final class WorkflowEngineFactory {

    private static final String WORKFLOW_ENGINE = "workflow.engine.impl";

    private static final String LIFECYCLE_ENGINE = "lifecycle.engine.impl";

    private WorkflowEngineFactory() {
    }

    public static AbstractWorkflowEngine getWorkflowEngine() throws FalconException {
        return ReflectionUtils.getInstance(WORKFLOW_ENGINE);
    }

    public static AbstractPolicyBuilderFactory getLifecycleEngine() throws FalconException {
        return ReflectionUtils.getInstance(LIFECYCLE_ENGINE);
    }

}
