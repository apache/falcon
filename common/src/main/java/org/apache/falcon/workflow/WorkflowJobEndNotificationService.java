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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.aspect.GenericAlert;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.service.FalconService;
import org.apache.falcon.util.ReflectionUtils;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.engine.AbstractWorkflowEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * A workflow job end notification service.
 */
public class WorkflowJobEndNotificationService implements FalconService {

    private static final Logger LOG = LoggerFactory.getLogger(WorkflowJobEndNotificationService.class);

    public static final String SERVICE_NAME = WorkflowJobEndNotificationService.class.getSimpleName();

    private Set<WorkflowExecutionListener> listeners = new LinkedHashSet<WorkflowExecutionListener>();

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public void init() throws FalconException {
        String listenerClassNames = StartupProperties.get().getProperty(
                "workflow.execution.listeners");
        if (StringUtils.isEmpty(listenerClassNames)) {
            return;
        }

        for (String listenerClassName : listenerClassNames.split(",")) {
            listenerClassName = listenerClassName.trim();
            if (listenerClassName.isEmpty()) {
                continue;
            }
            WorkflowExecutionListener listener = ReflectionUtils.getInstanceByClassName(listenerClassName);
            registerListener(listener);
        }
    }

    @Override
    public void destroy() throws FalconException {
        listeners.clear();
    }

    public void registerListener(WorkflowExecutionListener listener) {
        listeners.add(listener);
    }

    public void unregisterListener(WorkflowExecutionListener listener) {
        listeners.remove(listener);
    }

    public void notifyFailure(WorkflowExecutionContext context) {
        for (WorkflowExecutionListener listener : listeners) {
            try {
                listener.onFailure(context);
            } catch (Throwable t) {
                // do not rethrow as other listeners do not get a chance
                LOG.error("Error in listener {}", listener.getClass().getName(), t);
            }
        }

        instrumentAlert(context);
    }

    public void notifySuccess(WorkflowExecutionContext context) {
        for (WorkflowExecutionListener listener : listeners) {
            try {
                listener.onSuccess(context);
            } catch (Throwable t) {
                // do not rethrow as other listeners do not get a chance
                LOG.error("Error in listener {}", listener.getClass().getName(), t);
            }
        }

        instrumentAlert(context);
    }

    private void instrumentAlert(WorkflowExecutionContext context) {
        String clusterName = context.getClusterName();
        String entityName = context.getEntityName();
        String entityType = context.getEntityType();
        String operation = context.getOperation().name();
        String workflowId = context.getWorkflowId();
        String workflowUser = context.getWorkflowUser();
        String nominalTime = context.getNominalTimeAsISO8601();
        String runId = String.valueOf(context.getWorkflowRunId());

        try {
            CurrentUser.authenticate(context.getWorkflowUser());
            AbstractWorkflowEngine wfEngine = WorkflowEngineFactory.getWorkflowEngine();
            InstancesResult result = wfEngine.getJobDetails(clusterName, workflowId);
            Date startTime = result.getInstances()[0].startTime;
            Date endTime = result.getInstances()[0].endTime;
            Long duration = (endTime.getTime() - startTime.getTime()) * 1000000;

            if (context.hasWorkflowFailed()) {
                GenericAlert.instrumentFailedInstance(clusterName, entityType,
                        entityName, nominalTime, workflowId, workflowUser, runId, operation,
                        SchemaHelper.formatDateUTC(startTime), "", "", duration);
            } else {
                GenericAlert.instrumentSucceededInstance(clusterName, entityType,
                        entityName, nominalTime, workflowId, workflowUser, runId, operation,
                        SchemaHelper.formatDateUTC(startTime), duration);
            }
        } catch (FalconException e) {
            // Logging an error and ignoring since there are listeners for extensions
            LOG.error("Instrumenting alert failed for: " + context, e);
        }
    }
}
