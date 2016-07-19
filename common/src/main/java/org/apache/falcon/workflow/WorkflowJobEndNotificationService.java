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
import org.apache.falcon.entity.EntityNotRegisteredException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.service.FalconService;
import org.apache.falcon.util.ReflectionUtils;
import org.apache.falcon.util.StartupProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A workflow job end notification service.
 */
public class WorkflowJobEndNotificationService implements FalconService {

    private static final Logger LOG = LoggerFactory.getLogger(WorkflowJobEndNotificationService.class);

    public static final String SERVICE_NAME = WorkflowJobEndNotificationService.class.getSimpleName();

    private Set<WorkflowExecutionListener> listeners = new LinkedHashSet<WorkflowExecutionListener>();

    // Maintain a cache of context built, so we don't have to query Oozie for every state change.
    private Map<String, Properties> contextMap = new ConcurrentHashMap<>();

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    // Mainly for test
    Map<String, Properties> getContextMap() {
        return contextMap;
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

    public void notifyFailure(WorkflowExecutionContext context) throws FalconException {
        notifyWorkflowEnd(context);
    }

    public void notifySuccess(WorkflowExecutionContext context) throws FalconException {
        notifyWorkflowEnd(context);
    }

    public void notifyStart(WorkflowExecutionContext context) throws FalconException {
        // Start notifications can only be from Oozie JMS notifications
        if (!updateContextFromWFConf(context)) {
            return;
        }
        LOG.debug("Sending workflow start notification to listeners with context : {} ", context);
        for (WorkflowExecutionListener listener : listeners) {
            try {
                listener.onStart(context);
            } catch (Throwable t) {
                // do not rethrow as other listeners do not get a chance
                LOG.error("Error in listener {}", listener.getClass().getName(), t);
            }
        }
    }

    public void notifySuspend(WorkflowExecutionContext context) throws FalconException {
        // Suspend notifications can only be from Oozie JMS notifications
        if (!updateContextFromWFConf(context)) {
            return;
        }
        LOG.debug("Sending workflow suspend notification to listeners with context : {} ", context);
        for (WorkflowExecutionListener listener : listeners) {
            try {
                listener.onSuspend(context);
            } catch (Throwable t) {
                // do not rethrow as other listeners do not get a chance
                LOG.error("Error in listener {}", listener.getClass().getName(), t);
            }
        }

        instrumentAlert(context);
        contextMap.remove(context.getWorkflowId());
    }

    public void notifyWait(WorkflowExecutionContext context) throws FalconException {
        // Wait notifications can only be from Oozie JMS notifications
        LOG.debug("Sending workflow wait notification to listeners with context : {} ", context);
        for (WorkflowExecutionListener listener : listeners) {
            try {
                listener.onWait(context);
            } catch (Throwable t) {
                // do not rethrow as other listeners do not get a chance
                LOG.error("Error in listener {}", listener.getClass().getName(), t);
            }
        }
    }

    // The method retrieves the conf from the cache if it is in cache.
    // Else, queries WF Engine to retrieve the conf of the workflow
    private boolean updateContextFromWFConf(WorkflowExecutionContext context) throws FalconException {
        Properties wfProps = contextMap.get(context.getWorkflowId());
        if (wfProps == null) {
            wfProps = new Properties();
            Entity entity = null;
            try {
                entity = EntityUtil.getEntity(context.getEntityType(), context.getEntityName());
            } catch (EntityNotRegisteredException e) {
                // Entity no longer exists. No need to notify.
                LOG.debug("Entity {} of type {} doesn't exist in config store. Notification Ignored.",
                        context.getEntityName(), context.getEntityType());
                contextMap.remove(context.getWorkflowId());
                return false;
            }
            for (String cluster : EntityUtil.getClustersDefinedInColos(entity)) {
                wfProps.setProperty(WorkflowExecutionArgs.CLUSTER_NAME.getName(), cluster);
                try {
                    InstancesResult.Instance[] instances = WorkflowEngineFactory.getWorkflowEngine(entity)
                            .getJobDetails(cluster, context.getWorkflowId()).getInstances();
                    if (instances != null && instances.length > 0) {
                        wfProps.putAll(getWFProps(instances[0].getWfParams()));
                        // Required by RetryService. But, is not part of conf.
                        wfProps.setProperty(WorkflowExecutionArgs.RUN_ID.getName(),
                                Integer.toString(instances[0].getRunId()));
                    }
                } catch (FalconException e) {
                    // Do Nothing. Move on to the next cluster.
                    continue;
                }
                contextMap.put(context.getWorkflowId(), wfProps);
            }
        }

        // No extra props to enhance the context with.
        if (wfProps == null || wfProps.isEmpty()) {
            return true;
        }

        for (WorkflowExecutionArgs arg : WorkflowExecutionArgs.values()) {
            if (wfProps.containsKey(arg.getName())) {
                context.setValue(arg, wfProps.getProperty(arg.getName()));
            }
        }
        return true;
    }

    private Properties getWFProps(InstancesResult.KeyValuePair[] wfParams) {
        Properties props = new Properties();
        for (InstancesResult.KeyValuePair kv : wfParams) {
            props.put(kv.getKey(), kv.getValue());
        }
        return props;
    }

    // This method handles both success and failure notifications.
    private void notifyWorkflowEnd(WorkflowExecutionContext context) throws FalconException {
        // Need to distinguish notification from post processing for backward compatibility
        if (context.getContextType() == WorkflowExecutionContext.Type.POST_PROCESSING) {
            boolean engineNotifEnabled = false;
            try {
                engineNotifEnabled = WorkflowEngineFactory.getWorkflowEngine()
                        .isNotificationEnabled(context.getClusterName(), context.getWorkflowId());
            } catch (FalconException e) {
                LOG.debug("Received error while checking if notification is enabled. "
                        + "Hence, assuming notification is not enabled.");
            }
            // Ignore the message from post processing as there will be one more from Oozie.
            if (engineNotifEnabled) {
                LOG.info("Ignoring message from post processing as engine notification is enabled.");
                return;
            } else {
                updateContextWithTime(context);
            }
        } else {
            if (!updateContextFromWFConf(context)) {
                return;
            }
        }

        LOG.debug("Sending workflow end notification to listeners with context : {} ", context);

        for (WorkflowExecutionListener listener : listeners) {
            try {
                if (context.hasWorkflowSucceeded()) {
                    listener.onSuccess(context);
                    instrumentAlert(context);
                } else {
                    listener.onFailure(context);
                    if (context.hasWorkflowBeenKilled() || context.hasWorkflowFailed()) {
                        instrumentAlert(context);
                    }
                }
            } catch (Throwable t) {
                // do not rethrow as other listeners do not get a chance
                LOG.error("Error in listener {}", listener.getClass().getName(), t);
            }
        }

        contextMap.remove(context.getWorkflowId());
    }

    // In case of notifications coming from post notifications, start and end time need to be populated.
    private void updateContextWithTime(WorkflowExecutionContext context) {
        try {
            InstancesResult result = WorkflowEngineFactory.getWorkflowEngine()
                    .getJobDetails(context.getClusterName(), context.getWorkflowId());
            Date startTime = result.getInstances()[0].startTime;
            Date endTime = result.getInstances()[0].endTime;
            Date now = new Date();
            if (startTime == null) {
                startTime = now;
            }
            if (endTime == null) {
                endTime = now;
            }
            context.setValue(WorkflowExecutionArgs.WF_START_TIME, Long.toString(startTime.getTime()));
            context.setValue(WorkflowExecutionArgs.WF_END_TIME, Long.toString(endTime.getTime()));
        } catch(FalconException e) {
            LOG.error("Unable to retrieve job details for " + context.getWorkflowId() + " on cluster "
                    + context.getClusterName(), e);
        }
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
        Date now = new Date();
        // Start and/or End time may not be set in case of workflow suspend
        Date endTime;
        if (context.getWorkflowEndTime() == 0) {
            endTime = now;
        } else {
            endTime = new Date(context.getWorkflowEndTime());
        }

        Date startTime;
        if (context.getWorkflowStartTime() == 0) {
            startTime = now;
        } else {
            startTime = new Date(context.getWorkflowStartTime());
        }
        Long duration = (endTime.getTime() - startTime.getTime()) * 1000000;

        if (!context.hasWorkflowSucceeded()) {
            GenericAlert.instrumentFailedInstance(clusterName, entityType,
                    entityName, nominalTime, workflowId, workflowUser, runId, operation,
                    SchemaHelper.formatDateUTC(startTime), "", "", duration);
        } else {
            GenericAlert.instrumentSucceededInstance(clusterName, entityType,
                    entityName, nominalTime, workflowId, workflowUser, runId, operation,
                    SchemaHelper.formatDateUTC(startTime), duration);
        }
    }
}
