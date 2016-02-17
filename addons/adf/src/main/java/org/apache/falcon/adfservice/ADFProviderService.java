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

package org.apache.falcon.adfservice;

import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.ServiceBusService;
import com.microsoft.windowsazure.services.servicebus.models.BrokeredMessage;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMessageOptions;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMode;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveQueueMessageResult;
import com.microsoft.windowsazure.services.servicebus.ServiceBusConfiguration;
import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.adfservice.util.ADFJsonConstants;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.resource.AbstractInstanceManager;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesResult.Instance;
import org.apache.falcon.resource.InstancesResult.WorkflowStatus;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.service.FalconService;
import org.apache.falcon.service.Services;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.WorkflowExecutionListener;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.falcon.workflow.WorkflowJobEndNotificationService;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Falcon ADF provider to handle requests from Azure Data Factory.
 */
public class ADFProviderService implements FalconService, WorkflowExecutionListener {

    private static final Logger LOG = LoggerFactory.getLogger(ADFProviderService.class);

    /**
     * Constant for the service name.
     */
    public static final String SERVICE_NAME = ADFProviderService.class.getSimpleName();

    private static final int AZURE_SERVICEBUS_RECEIVEMESSGAEOPT_TIMEOUT = 60;
    // polling frequency in seconds
    private static final int AZURE_SERVICEBUS_DEFAULT_POLLING_FREQUENCY = 10;

    // Number of threads to handle ADF requests
    private static final int AZURE_SERVICEBUS_REQUEST_HANDLING_THREADS = 5;

    private static final String AZURE_SERVICEBUS_CONF_PREFIX = "microsoft.windowsazure.services.servicebus.";
    private static final String AZURE_SERVICEBUS_CONF_SASKEYNAME = "sasKeyName";
    private static final String AZURE_SERVICEBUS_CONF_SASKEY = "sasKey";
    private static final String AZURE_SERVICEBUS_CONF_SERVICEBUSROOTURI = "serviceBusRootUri";
    private static final String AZURE_SERVICEBUS_CONF_NAMESPACE = "namespace";
    private static final String AZURE_SERVICEBUS_CONF_POLLING_FREQUENCY = "polling.frequency";
    private static final String AZURE_SERVICEBUS_CONF_REQUEST_QUEUE_NAME = "requestqueuename";
    private static final String AZURE_SERVICEBUS_CONF_STATUS_QUEUE_NAME = "statusqueuename";
    private static final String AZURE_SERVICEBUS_CONF_SUPER_USER = "superuser";

    private static final ConfigurationStore STORE = ConfigurationStore.get();

    private ServiceBusContract service;
    private ScheduledExecutorService adfScheduledExecutorService;
    private ReceiveMessageOptions opts = ReceiveMessageOptions.DEFAULT;
    private ADFInstanceManager instanceManager = new ADFInstanceManager();
    private String requestQueueName;
    private String statusQueueName;
    private String superUser;

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public void init() throws FalconException {
        // read start up properties for adf configuration
        service = ServiceBusService.create(getServiceBusConfig());

        requestQueueName = StartupProperties.get().getProperty(AZURE_SERVICEBUS_CONF_PREFIX
                + AZURE_SERVICEBUS_CONF_REQUEST_QUEUE_NAME);
        if (StringUtils.isBlank(requestQueueName)) {
            throw new FalconException(AZURE_SERVICEBUS_CONF_PREFIX + AZURE_SERVICEBUS_CONF_REQUEST_QUEUE_NAME
                    + " property not set in startup properties. Please add it.");
        }
        statusQueueName = StartupProperties.get().getProperty(AZURE_SERVICEBUS_CONF_PREFIX
                + AZURE_SERVICEBUS_CONF_STATUS_QUEUE_NAME);
        if (StringUtils.isBlank(statusQueueName)) {
            throw new FalconException(AZURE_SERVICEBUS_CONF_PREFIX + AZURE_SERVICEBUS_CONF_STATUS_QUEUE_NAME
                    + " property not set in startup properties. Please add it.");
        }

        // init opts
        opts.setReceiveMode(ReceiveMode.PEEK_LOCK);
        opts.setTimeout(AZURE_SERVICEBUS_RECEIVEMESSGAEOPT_TIMEOUT);

        // restart handling
        superUser = StartupProperties.get().getProperty(
                AZURE_SERVICEBUS_CONF_PREFIX + AZURE_SERVICEBUS_CONF_SUPER_USER);
        if (StringUtils.isBlank(superUser)) {
            throw new FalconException(AZURE_SERVICEBUS_CONF_PREFIX + AZURE_SERVICEBUS_CONF_SUPER_USER
                    + " property not set in startup properties. Please add it.");
        }
        CurrentUser.authenticate(superUser);
        for (EntityType entityType : EntityType.values()) {
            Collection<String> entities = STORE.getEntities(entityType);
            for (String entityName : entities) {
                updateJobStatus(entityName, entityType.toString());
            }
        }

        Services.get().<WorkflowJobEndNotificationService>getService(
                WorkflowJobEndNotificationService.SERVICE_NAME).registerListener(this);
        adfScheduledExecutorService = new ADFScheduledExecutor(AZURE_SERVICEBUS_REQUEST_HANDLING_THREADS);
        adfScheduledExecutorService.scheduleWithFixedDelay(new HandleADFRequests(), 0, getDelay(), TimeUnit.SECONDS);
        LOG.info("Falcon ADFProvider service initialized");
    }

    private class HandleADFRequests implements Runnable {

        @Override
        public void run() {
            String sessionID = null;
            try {
                LOG.info("To read message from adf...");
                ReceiveQueueMessageResult resultQM =
                        service.receiveQueueMessage(requestQueueName, opts);
                BrokeredMessage message = resultQM.getValue();
                if (message != null && message.getMessageId() != null) {
                    sessionID = message.getReplyToSessionId();
                    BufferedReader rd = new BufferedReader(
                            new InputStreamReader(message.getBody()));
                    StringBuilder sb = new StringBuilder();
                    String line;
                    while ((line = rd.readLine()) != null) {
                        sb.append(line);
                    }
                    rd.close();
                    String msg = sb.toString();
                    LOG.info("ADF message: " + msg);

                    service.deleteMessage(message);

                    ADFJob job = ADFJobFactory.buildADFJob(msg, sessionID);
                    job.startJob();
                } else {
                    LOG.info("No message from adf");
                }
            } catch (FalconException e) {
                if (sessionID != null) {
                    sendErrorMessage(sessionID, e.toString());
                }
                LOG.info(e.toString());
            } catch (ServiceException | IOException e) {
                LOG.info(e.toString());
            }
        }
    }

    private static Configuration getServiceBusConfig() throws FalconException {
        String namespace = StartupProperties.get().getProperty(AZURE_SERVICEBUS_CONF_PREFIX
                + AZURE_SERVICEBUS_CONF_NAMESPACE);
        if (StringUtils.isBlank(namespace)) {
            throw new FalconException(AZURE_SERVICEBUS_CONF_PREFIX + AZURE_SERVICEBUS_CONF_NAMESPACE
                    + " property not set in startup properties. Please add it.");
        }

        String sasKeyName = StartupProperties.get().getProperty(AZURE_SERVICEBUS_CONF_PREFIX
                + AZURE_SERVICEBUS_CONF_SASKEYNAME);
        if (StringUtils.isBlank(sasKeyName)) {
            throw new FalconException(AZURE_SERVICEBUS_CONF_PREFIX + AZURE_SERVICEBUS_CONF_SASKEYNAME
                    + " property not set in startup properties. Please add it.");
        }

        String sasKey = StartupProperties.get().getProperty(AZURE_SERVICEBUS_CONF_PREFIX
                + AZURE_SERVICEBUS_CONF_SASKEY);
        if (StringUtils.isBlank(sasKey)) {
            throw new FalconException(AZURE_SERVICEBUS_CONF_PREFIX + AZURE_SERVICEBUS_CONF_SASKEY
                    + " property not set in startup properties. Please add it.");
        }

        String serviceBusRootUri = StartupProperties.get().getProperty(AZURE_SERVICEBUS_CONF_PREFIX
                + AZURE_SERVICEBUS_CONF_SERVICEBUSROOTURI);
        if (StringUtils.isBlank(serviceBusRootUri)) {
            throw new FalconException(AZURE_SERVICEBUS_CONF_PREFIX + AZURE_SERVICEBUS_CONF_SERVICEBUSROOTURI
                    + " property not set in startup properties. Please add it.");
        }

        LOG.info("namespace: {}, sas key name: {}, sas key: {}, root uri: {}",
                        namespace, sasKeyName, sasKey, serviceBusRootUri);
        return ServiceBusConfiguration.configureWithSASAuthentication(namespace, sasKeyName, sasKey,
                serviceBusRootUri);
    }


    // gets delay in seconds
    private long getDelay() throws FalconException {
        String pollingFrequencyValue = StartupProperties.get().getProperty(AZURE_SERVICEBUS_CONF_PREFIX
                + AZURE_SERVICEBUS_CONF_POLLING_FREQUENCY);
        long pollingFrequency;
        try {
            pollingFrequency = (StringUtils.isNotEmpty(pollingFrequencyValue))
                    ? Long.parseLong(pollingFrequencyValue) : AZURE_SERVICEBUS_DEFAULT_POLLING_FREQUENCY;
        } catch (NumberFormatException nfe) {
            throw new FalconException("Invalid value provided for startup property "
                    + AZURE_SERVICEBUS_CONF_PREFIX + AZURE_SERVICEBUS_CONF_POLLING_FREQUENCY
                    + ", please provide a valid long number", nfe);
        }
        return pollingFrequency;
    }

    @Override
    public void destroy() throws FalconException {
        Services.get().<WorkflowJobEndNotificationService>getService(
                WorkflowJobEndNotificationService.SERVICE_NAME).unregisterListener(this);
        adfScheduledExecutorService.shutdown();
    }

    @Override
    public void onSuccess(WorkflowExecutionContext context) throws FalconException {
        updateJobStatus(context, ADFJsonConstants.ADF_STATUS_SUCCEEDED, 100);
    }

    @Override
    public void onFailure(WorkflowExecutionContext context) throws FalconException {
        updateJobStatus(context, ADFJsonConstants.ADF_STATUS_FAILED, 0);
    }

    @Override
    public void onStart(WorkflowExecutionContext context) throws FalconException {
        updateJobStatus(context, ADFJsonConstants.ADF_STATUS_EXECUTING, 0);
    }

    @Override
    public void onSuspend(WorkflowExecutionContext context) throws FalconException {
        updateJobStatus(context, ADFJsonConstants.ADF_STATUS_CANCELED, 0);
    }

    @Override
    public void onWait(WorkflowExecutionContext context) throws FalconException {
        updateJobStatus(context, ADFJsonConstants.ADF_STATUS_EXECUTING, 0);
    }

    private void updateJobStatus(String entityName, String entityType) throws FalconException {
        // Filter non-adf jobs
        if (!ADFJob.isADFJobEntity(entityName)) {
            return;
        }

        Instance instance = instanceManager.getFirstInstance(entityName, entityType);
        if (instance == null) {
            return;
        }

        WorkflowStatus workflowStatus = instance.getStatus();
        String status;
        int progress = 0;
        switch (workflowStatus) {
        case SUCCEEDED:
            progress = 100;
            status = ADFJsonConstants.ADF_STATUS_SUCCEEDED;
            break;
        case FAILED:
        case KILLED:
        case ERROR:
        case SKIPPED:
        case UNDEFINED:
            status = ADFJsonConstants.ADF_STATUS_FAILED;
            break;
        default:
            status = ADFJsonConstants.ADF_STATUS_EXECUTING;
        }
        updateJobStatus(entityName, status, progress, instance.getLogFile());
    }

    private void updateJobStatus(WorkflowExecutionContext context, String status, int progress) {
        // Filter non-adf jobs
        String entityName = context.getEntityName();
        if (!ADFJob.isADFJobEntity(entityName)) {
            return;
        }

        updateJobStatus(entityName, status, progress, context.getLogFile());
    }

    private void updateJobStatus(String entityName, String status, int progress, String logUrl) {
        try {
            String sessionID = ADFJob.getSessionID(entityName);
            LOG.info("To update job status: " + sessionID + ", " + entityName + ", " + status + ", " + logUrl);
            JSONObject obj = new JSONObject();
            obj.put(ADFJsonConstants.ADF_STATUS_PROTOCOL, ADFJsonConstants.ADF_STATUS_PROTOCOL_NAME);
            obj.put(ADFJsonConstants.ADF_STATUS_JOBID, sessionID);
            obj.put(ADFJsonConstants.ADF_STATUS_LOG_URL, logUrl);
            obj.put(ADFJsonConstants.ADF_STATUS_STATUS, status);
            obj.put(ADFJsonConstants.ADF_STATUS_PROGRESS, progress);
            sendStatusUpdate(sessionID, obj.toString());
        } catch (JSONException | FalconException e) {
            LOG.info("Error when updating job status: " + e.toString());
        }
    }

    private void sendErrorMessage(String sessionID, String errorMessage) {
        LOG.info("Sending error message for session " + sessionID + ": " + errorMessage);
        try {
            JSONObject obj = new JSONObject();
            obj.put(ADFJsonConstants.ADF_STATUS_PROTOCOL, ADFJsonConstants.ADF_STATUS_PROTOCOL_NAME);
            obj.put(ADFJsonConstants.ADF_STATUS_JOBID, sessionID);
            obj.put(ADFJsonConstants.ADF_STATUS_STATUS, ADFJsonConstants.ADF_STATUS_FAILED);
            obj.put(ADFJsonConstants.ADF_STATUS_PROGRESS, 0);
            obj.put(ADFJsonConstants.ADF_STATUS_ERROR_TYPE, ADFJsonConstants.ADF_STATUS_ERROR_TYPE_VALUE);
            obj.put(ADFJsonConstants.ADF_STATUS_ERROR_MESSAGE, errorMessage);
            sendStatusUpdate(sessionID, obj.toString());
        } catch (JSONException e) {
            LOG.info("Error when sending error message: " + e.toString());
        }
    }

    private void sendStatusUpdate(String sessionID, String message) {
        LOG.info("Sending update for session " + sessionID + ": " + message);
        try {
            InputStream in = IOUtils.toInputStream(message, "UTF-8");
            BrokeredMessage updateMessage = new BrokeredMessage(in);
            updateMessage.setSessionId(sessionID);
            service.sendQueueMessage(statusQueueName, updateMessage);
        } catch (IOException | ServiceException e) {
            LOG.info("Error when sending status update: " + e.toString());
        }
    }

    private static class ADFInstanceManager extends AbstractInstanceManager {
        public Instance getFirstInstance(String entityName, String entityType) throws FalconException {
            InstancesResult result = getStatus(entityType, entityName, null, null, null, null, "", "", "", 0, 1, null);
            Instance[] instances = result.getInstances();
            if (instances.length > 0) {
                return instances[0];
            }
            return null;
        }
    }
}
