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
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.lifecycle.AbstractPolicyBuilderFactory;
import org.apache.falcon.util.ReflectionUtils;
import org.apache.falcon.workflow.engine.AbstractWorkflowEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Factory for providing appropriate workflow engine to the falcon service.
 */
@SuppressWarnings("unchecked")
public final class WorkflowEngineFactory {

    private static final Logger LOG = LoggerFactory.getLogger(WorkflowEngineFactory.class);
    public static final String ENGINE_PROP="falcon.scheduler";
    private static AbstractWorkflowEngine nativeWorkflowEngine;
    private static AbstractWorkflowEngine configuredWorkflowEngine;
    private static final String CONFIGURED_WORKFLOW_ENGINE = "workflow.engine.impl";
    private static final String LIFECYCLE_ENGINE = "lifecycle.engine.impl";

    private WorkflowEngineFactory() {
    }

    /**
     * @param entity
     * @return The workflow engine using which the entity is scheduled.
     * @throws FalconException
     */
    public static AbstractWorkflowEngine getWorkflowEngine(Entity entity) throws FalconException {
        // The below check is only for schedulable entities.
        if (entity != null
                && entity.getEntityType().isSchedulable() && getNativeWorkflowEngine().isActive(entity)) {
            LOG.debug("Returning native workflow engine for entity {}", entity.getName());
            return nativeWorkflowEngine;
        }
        LOG.debug("Returning configured workflow engine for entity {}", (entity == null)? null : entity.getName());
        return getWorkflowEngine();
    }

    /**
     * @param entity
     * @param props
     * @return Workflow engine as specified in the props and for a given schedulable entity.
     * @throws FalconException
     */
    public static AbstractWorkflowEngine getWorkflowEngine(Entity entity, Map<String, String> props)
        throws FalconException {
        // If entity is null or not schedulable and the engine property is not specified, return the configured WE.
        if (entity == null || !entity.getEntityType().isSchedulable()) {
            LOG.debug("Returning configured workflow engine for entity {}", (entity == null)? null : entity.getName());
            return getWorkflowEngine();
        }

        // Default to configured workflow engine when no properties are specified.
        String engineName = getWorkflowEngine().getName();
        if (props != null && props.containsKey(ENGINE_PROP)) {
            engineName = props.get(ENGINE_PROP);
        }

        if (engineName.equalsIgnoreCase(getWorkflowEngine().getName())) {
            // If already active on native
            if (getNativeWorkflowEngine().isActive(entity)) {
                throw new FalconException("Entity " + entity.getName() + " is already scheduled on native engine.");
            }
            LOG.debug("Returning configured workflow engine for entity {}", entity.getName());
            return configuredWorkflowEngine;
        } else if (engineName.equalsIgnoreCase(getNativeWorkflowEngine().getName())) {
            // If already active on configured workflow engine
            if (getWorkflowEngine().isActive(entity)) {
                throw new FalconException("Entity " + entity.getName() + " is already scheduled on "
                        + "configured workflow engine.");
            }
            LOG.debug("Returning native workflow engine for entity {}", entity.getName());
            return nativeWorkflowEngine;
        } else {
            throw new IllegalArgumentException("Property " + ENGINE_PROP + " is not set to a valid value.");
        }
    }

    /**
     * @return An instance of the configurated workflow engine.
     * @throws FalconException
     */
    public static AbstractWorkflowEngine getWorkflowEngine() throws FalconException {
        // Caching is only for optimization, workflow engine doesn't need to be a singleton.
        if (configuredWorkflowEngine == null) {
            configuredWorkflowEngine = ReflectionUtils.getInstance(CONFIGURED_WORKFLOW_ENGINE);
        }
        return configuredWorkflowEngine;
    }

    public static AbstractWorkflowEngine getNativeWorkflowEngine() throws FalconException {
        if (nativeWorkflowEngine  ==  null) {
            nativeWorkflowEngine =
                    ReflectionUtils.getInstanceByClassName("org.apache.falcon.workflow.engine.FalconWorkflowEngine");
        }
        return nativeWorkflowEngine;
    }

    public static AbstractPolicyBuilderFactory getLifecycleEngine() throws FalconException {
        return ReflectionUtils.getInstance(LIFECYCLE_ENGINE);
    }
}
