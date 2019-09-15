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
import org.apache.falcon.exception.NotificationServiceException;
import org.apache.falcon.execution.NotificationHandler;
import org.apache.falcon.execution.SchedulerUtil;
import org.apache.falcon.notification.service.FalconNotificationService;
import org.apache.falcon.notification.service.event.DataEvent;
import org.apache.falcon.notification.service.request.LocationBasedDataNotificationRequest;
import org.apache.falcon.notification.service.request.RegexBasedDataNotificationRequest;
import org.apache.falcon.notification.service.request.DataNotificationRequest;
import org.apache.falcon.notification.service.request.NotificationRequest;
import org.apache.falcon.state.ID;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This notification service notifies {@link NotificationHandler} when requested data
 * becomes available. This class also supports time out, in which case it notifies about the unavailability.
 */
public class DataAvailabilityService implements FalconNotificationService {

    private static final Logger LOG = LoggerFactory.getLogger(DataAvailabilityService.class);
    private static final String NUM_THREADS_PROP = "scheduler.data.notification.service.threads";
    private static final String DEFAULT_NUM_THREADS = "5";

    private DelayQueue<DataNotificationRequest> delayQueue = new DelayQueue<>();
    private ExecutorService executorService;
    // It contains all instances which are unregistered and can be ignored.
    private Map<ID, NotificationHandler> instancesToIgnore;

    @Override
    public void register(NotificationRequest request) throws NotificationServiceException {
        LOG.info("Registering Data notification for " + request.getCallbackId().toString());
        DataNotificationRequest dataNotificationRequest = (DataNotificationRequest) request;
        if (instancesToIgnore.containsKey(dataNotificationRequest.getCallbackId())) {
            instancesToIgnore.remove(dataNotificationRequest.getCallbackId());
        }
        delayQueue.offer(dataNotificationRequest);
    }

    @Override
    public void unregister(NotificationHandler handler, ID listenerID) {
        LOG.info("Removing Data notification Request with callbackID {}", listenerID.getKey());
        instancesToIgnore.put(listenerID, handler);
    }

    @Override
    public RequestBuilder createRequestBuilder(NotificationHandler handler, ID callbackID) {
        return new DataRequestBuilder(handler, callbackID);
    }

    @Override
    public String getName() {
        return "DataAvailabilityService";
    }

    @Override
    public void init() throws FalconException {
        int executorThreads = Integer.parseInt(StartupProperties.get().
                getProperty(NUM_THREADS_PROP, DEFAULT_NUM_THREADS));
        executorService = Executors.newFixedThreadPool(executorThreads);
        for (int i = 0; i < executorThreads; i++) {
            executorService.execute(new EventConsumer());
        }
        instancesToIgnore = new ConcurrentHashMap<>();
    }

    @Override
    public void destroy() throws FalconException {
        instancesToIgnore.clear();
        delayQueue.clear();
        executorService.shutdown();
    }


    /**
     * Builds {@link DataNotificationRequest}.
     */
    public static class DataRequestBuilder extends RequestBuilder<DataNotificationRequest> {
        private String cluster;
        private long pollingFrequencyInMillis;
        private long timeoutInMillis;
        private Map<Path, Boolean> locations;
        private long startTimeInMillis;
        private long endTimeInMillis;
        private int startInstance;
        private int endInstance;
        private SchedulerUtil.EXPTYPE expType;
        private String basePath;
        private long frequencyInMillis;
        private String inputName;
        private boolean isOptional;


        public DataRequestBuilder(NotificationHandler handler, ID callbackID) {
            super(handler, callbackID);
        }

        @Override
        public DataNotificationRequest build() {
            checkMandatoryArgs();
            if (expType == SchedulerUtil.EXPTYPE.ABSOLUTE) {
                checkLocations();
                return new LocationBasedDataNotificationRequest(handler, callbackId, cluster,
                        pollingFrequencyInMillis, timeoutInMillis, locations, inputName, isOptional);
            } else {
                checkArgsForRegexRequest();
                return new RegexBasedDataNotificationRequest(handler, callbackId, cluster,
                        pollingFrequencyInMillis, timeoutInMillis, startTimeInMillis,
                        endTimeInMillis, startInstance, endInstance, expType, basePath,
                        frequencyInMillis, inputName, isOptional);
            }
        }

        private void checkArgsForRegexRequest() {
            if (startTimeInMillis <= 0 || endTimeInMillis <=0 || basePath == null || frequencyInMillis <=0) {
                throw new IllegalArgumentException("Missing or incorrect, one or more of the mandatory arguments"
                        + " for Regex Request: startTime, endTime, basepath, frequency");
            }
        }

        private void checkLocations() {
            if (locations == null) {
                throw new IllegalArgumentException("Locations cannot be null for Absolute Expression Request");
            }
        }

        private void checkMandatoryArgs() {
            if (callbackId == null || cluster == null || pollingFrequencyInMillis <= 0
                    || timeoutInMillis < pollingFrequencyInMillis || inputName == null || expType == null) {
                throw new IllegalArgumentException("Missing or incorrect, one or more of the mandatory arguments:"
                        + " callbackId, cluster, pollingFrequency, timeOut, inputName, expType");
            }
        }

        public DataRequestBuilder setLocations(List<Path> locPaths) {
            Map<Path, Boolean> paths = new HashMap<>();
            for (Path loc : locPaths) {
                paths.put(loc, false);
            }
            this.locations = paths;
            return this;
        }

        public DataRequestBuilder setCluster(String clusterName) {
            this.cluster = clusterName;
            return this;
        }

        public DataRequestBuilder setPollingFrequencyInMillis(long pollingFreq) {
            if (pollingFreq <= 0) {
                throw new IllegalArgumentException("PollingFrequency should be greater than zero");
            }
            this.pollingFrequencyInMillis = pollingFreq;
            return this;
        }

        public DataRequestBuilder setTimeoutInMillis(long timeout) {
            if (timeout <= 0 || timeout < pollingFrequencyInMillis) {
                throw new IllegalArgumentException("Timeout should be positive and greater than PollingFrequency");
            }
            this.timeoutInMillis = timeout;
            return this;
        }

        public DataRequestBuilder setStartTimeInMillis(long startTime) {
            if (startTime < 0) {
                throw new IllegalArgumentException("StartTime should be greater than zero");
            }
            this.startTimeInMillis = startTime;
            return this;
        }

        public DataRequestBuilder setEndTimeInMillis(long endTime) {
            if (endTime < 0) {
                throw new IllegalArgumentException("EndTime should be greater than startTime");
            }
            this.endTimeInMillis = endTime;
            return this;
        }

        public DataRequestBuilder setStartInstance(int startInst) {
            if (startInst < 0) {
                throw new IllegalArgumentException("startInstance should be greater than zero");
            }
            this.startInstance = startInst;
            return this;
        }

        public DataRequestBuilder setEndInstance(int endInst) {
            if (endInst < 0) {
                throw new IllegalArgumentException("endInstance should be greater than zero");
            }
            this.endInstance = endInst;
            return this;
        }

        public DataRequestBuilder setBasePath(String basePathExp) {
            if (StringUtils.isBlank(basePathExp)) {
                throw new IllegalArgumentException("base path cannot be null or empty");
            }
            this.basePath = basePathExp;
            return this;
        }

        public DataRequestBuilder setFrequencyInMillis(long freqInMillis) {
            if (freqInMillis <= 0) {
                throw new IllegalArgumentException("Frequency cannot be less than zero");
            }
            this.frequencyInMillis = freqInMillis;
            return this;
        }

        public DataRequestBuilder setInputName(String inName) {
            if (StringUtils.isBlank(inName)) {
                throw new IllegalArgumentException("Feed name cannot be null or empty");
            }
            this.inputName = inName;
            return this;
        }

        public DataRequestBuilder setExpType(SchedulerUtil.EXPTYPE exp) {
            if (exp == null) {
                throw new IllegalArgumentException("exptype cannot be null");
            }
            this.expType = exp;
            return this;
        }

        public DataRequestBuilder setIsOptional(boolean optional) {
            this.isOptional = optional;
            return this;
        }

    }





    private class EventConsumer implements Runnable {

        public EventConsumer() {
        }

        @Override
        public void run() {
            DataNotificationRequest dataNotificationRequest;
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    dataNotificationRequest = delayQueue.take();
                    boolean isUnRegistered = isUnRegistered(dataNotificationRequest);
                    if (isUnRegistered) {
                        continue;
                    }
                    DataServiceHandler dataServiceHandler = getDataServiceHandler(dataNotificationRequest);
                    boolean isDataArrived = dataServiceHandler.handleRequest(dataNotificationRequest);

                    if (isDataArrived) {
                        notifyHandler(dataNotificationRequest, DataEvent.STATUS.AVAILABLE);
                    } else {
                        if (dataNotificationRequest.isOptional()) {
                            getAvailablePaths(dataNotificationRequest);
                            notifyHandler(dataNotificationRequest, DataEvent.STATUS.AVAILABLE);
                            continue;
                        }
                        if (dataNotificationRequest.isTimedout()) {
                            notifyHandler(dataNotificationRequest, DataEvent.STATUS.UNAVAILABLE);
                            continue;
                        }
                        dataNotificationRequest.accessed();
                        delayQueue.offer(dataNotificationRequest);
                    }
                } catch (Throwable e) {
                    LOG.error("Error in Data Notification Service EventConsumer", e);
                }
            }
        }

        private void getAvailablePaths(DataNotificationRequest dataNotificationRequest) {
            Map<Path, Boolean> locPaths = dataNotificationRequest.getLocationMap();
            Map<Path, Boolean> availableLocPaths = new HashMap<>();
            for (Map.Entry<Path, Boolean> entry : locPaths.entrySet()) {
                if (entry.getValue()) {
                    availableLocPaths.put(entry.getKey(), entry.getValue());
                }
            }
            if (availableLocPaths.isEmpty()) {
                // add Empty Directory from clusterHelper
                System.out.println("have to modify");
            }
            dataNotificationRequest.setLocationMap(availableLocPaths);
        }

        private DataServiceHandler getDataServiceHandler(DataNotificationRequest dataNotificationRequest) {
            if (dataNotificationRequest instanceof LocationBasedDataNotificationRequest) {
                return new PathServiceHandler();
            } else if (dataNotificationRequest instanceof RegexBasedDataNotificationRequest) {
                return new RegexServiceHandler();
            } else {
                throw new IllegalArgumentException(" Service Handler not defined for request "
                        + dataNotificationRequest); // add toString to this
            }
        }

        private void notifyHandler(DataNotificationRequest dataNotificationRequest,
                                   DataEvent.STATUS status) {
            DataEvent dataEvent = new DataEvent(dataNotificationRequest.getCallbackId(),
                    dataNotificationRequest.getLocations(), status, dataNotificationRequest.getInputName());
            boolean isUnRegistered = isUnRegistered(dataNotificationRequest);
            if (isUnRegistered) {
                return;
            }
            try {
                LOG.debug("Notifying Handler for Data Notification Request of id {} ",
                        dataNotificationRequest.getCallbackId().toString());
                dataNotificationRequest.getHandler().onEvent(dataEvent);
            } catch (FalconException e) {
                LOG.error("Unable to notify Data event with id {} ",
                        dataNotificationRequest.getCallbackId(), e);
                // ToDo Retries for notifying
            }
        }

        private boolean isUnRegistered(DataNotificationRequest dataNotificationRequest) {
            if (instancesToIgnore.containsKey(dataNotificationRequest.getCallbackId())) {
                LOG.info("Ignoring Data Notification Request of id {} ",
                        dataNotificationRequest.getCallbackId().toString());
                instancesToIgnore.remove(dataNotificationRequest.getCallbackId());
                return true;
            }
            return false;
        }
    }

}
