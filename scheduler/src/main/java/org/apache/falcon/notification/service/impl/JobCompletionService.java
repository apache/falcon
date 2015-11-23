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
package org.apache.falcon.notification.service.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.exception.NotificationServiceException;
import org.apache.falcon.execution.NotificationHandler;
import org.apache.falcon.notification.service.FalconNotificationService;
import org.apache.falcon.notification.service.event.JobCompletedEvent;
import org.apache.falcon.notification.service.request.JobCompletionNotificationRequest;
import org.apache.falcon.notification.service.request.NotificationRequest;
import org.apache.falcon.service.Services;
import org.apache.falcon.state.ID;
import org.apache.falcon.state.InstanceID;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.falcon.workflow.WorkflowExecutionListener;
import org.apache.falcon.workflow.WorkflowJobEndNotificationService;
import org.apache.falcon.workflow.engine.DAGEngineFactory;
import org.apache.oozie.client.WorkflowJob;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This notification service notifies {@link NotificationHandler} when an external job
 * completes.
 */
public class JobCompletionService implements FalconNotificationService, WorkflowExecutionListener {

    private static final Logger LOG = LoggerFactory.getLogger(JobCompletionService.class);
    private static final DateTimeZone UTC = DateTimeZone.UTC;

    private List<NotificationHandler> listeners = Collections.synchronizedList(new ArrayList<NotificationHandler>());

    @Override
    public void register(NotificationRequest notifRequest) throws NotificationServiceException {
        if (notifRequest == null) {
            throw new NotificationServiceException("Request object cannot be null");
        }
        listeners.add(notifRequest.getHandler());
        JobCompletionNotificationRequest request = (JobCompletionNotificationRequest) notifRequest;
        // Check if the job is already complete.
        // If yes, send a notification synchronously.
        // If not, we expect that this class will get notified when the job completes
        // as this class is a listener to WorkflowJobEndNotificationService.
        if (request.getExternalId() != null && request.getCluster() != null) {
            try {
                Properties props = DAGEngineFactory.getDAGEngine(request.getCluster())
                        .getConfiguration(request.getExternalId());
                WorkflowExecutionContext context = createContext(props);
                if (context.hasWorkflowFailed()) {
                    onFailure(context);
                } else if (context.hasWorkflowSucceeded()) {
                    onSuccess(context);
                }
            } catch (FalconException e) {
                throw new NotificationServiceException(e);
            }
        }
    }

    @Override
    public void unregister(NotificationHandler handler, ID listenerID) {
        listeners.remove(handler);
    }

    @Override
    public RequestBuilder createRequestBuilder(NotificationHandler handler, ID callbackID) {
        return new JobCompletionRequestBuilder(handler, callbackID);
    }

    @Override
    public String getName() {
        return "JobCompletionService";
    }

    @Override
    public void init() throws FalconException {
        LOG.debug("Registering to job end notification service");
        Services.get().<WorkflowJobEndNotificationService>getService(
                WorkflowJobEndNotificationService.SERVICE_NAME).registerListener(this);
    }

    @Override
    public void destroy() throws FalconException {

    }

    @Override
    public void onSuccess(WorkflowExecutionContext context) throws FalconException {
        onEnd(context, WorkflowJob.Status.SUCCEEDED);
    }

    @Override
    public void onFailure(WorkflowExecutionContext context) throws FalconException {
        onEnd(context, WorkflowJob.Status.FAILED);
    }

    @Override
    public void onStart(WorkflowExecutionContext context) throws FalconException {
        // Do nothing
    }

    @Override
    public void onSuspend(WorkflowExecutionContext context) throws FalconException {
        // Do nothing
    }

    @Override
    public void onWait(WorkflowExecutionContext context) throws FalconException {
        // Do nothing
    }

    private void onEnd(WorkflowExecutionContext context, WorkflowJob.Status status) throws FalconException {
        JobCompletedEvent event = new JobCompletedEvent(constructCallbackID(context), status, getEndTime(context));
        for (NotificationHandler handler : listeners) {
            LOG.debug("Notifying {} with event {}", handler, event.getTarget());
            handler.onEvent(event);
        }
    }

    private DateTime getEndTime(WorkflowExecutionContext context) throws FalconException {
        return new DateTime(DAGEngineFactory.getDAGEngine(context.getClusterName())
                .info(context.getWorkflowId()).getEndTime());
    }

    // Constructs the callback ID from the details available in the context.
    private InstanceID constructCallbackID(WorkflowExecutionContext context) throws FalconException {
        EntityType entityType = EntityType.valueOf(context.getEntityType());
        String entityName = context.getEntityName();
        String clusterName = context.getClusterName();
        DateTime instanceTime = new DateTime(EntityUtil.parseDateUTC(context.getNominalTimeAsISO8601()), UTC);
        return new InstanceID(entityType, entityName, clusterName, instanceTime);
    }

    private WorkflowExecutionContext createContext(Properties props) {
        // for backwards compatibility, read all args from properties
        Map<WorkflowExecutionArgs, String> wfProperties = new HashMap<WorkflowExecutionArgs, String>();
        for (WorkflowExecutionArgs arg : WorkflowExecutionArgs.values()) {
            String optionValue = props.getProperty(arg.getName());
            if (StringUtils.isNotEmpty(optionValue)) {
                wfProperties.put(arg, optionValue);
            }
        }

        return WorkflowExecutionContext.create(wfProperties);
    }

    /**
     * Builds {@link JobCompletionNotificationRequest}.
     */
    public static class JobCompletionRequestBuilder extends RequestBuilder<JobCompletionNotificationRequest> {
        private String cluster;
        private String externalId;

        public JobCompletionRequestBuilder(NotificationHandler handler, ID callbackID) {
            super(handler, callbackID);
        }

        /**
         * @param clusterName
         */
        public JobCompletionRequestBuilder setCluster(String clusterName) {
            this.cluster = clusterName;
            return this;
        }

        /**
         * @param id - The external job id for which job completion notification is requested.
         * @return
         */
        public JobCompletionRequestBuilder setExternalId(String id) {
            this.externalId = id;
            return this;
        }

        @Override
        public JobCompletionNotificationRequest build() {
            return new JobCompletionNotificationRequest(handler, callbackId, cluster, externalId);
        }
    }
}
