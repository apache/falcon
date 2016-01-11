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

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.exception.NotificationServiceException;
import org.apache.falcon.execution.NotificationHandler;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.notification.service.FalconNotificationService;
import org.apache.falcon.notification.service.event.DataEvent;
import org.apache.falcon.notification.service.request.DataNotificationRequest;
import org.apache.falcon.notification.service.request.NotificationRequest;
import org.apache.falcon.state.ID;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
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

        public DataRequestBuilder(NotificationHandler handler, ID callbackID) {
            super(handler, callbackID);
        }

        public DataRequestBuilder setLocations(List<Path> locPaths) {
            Map<Path, Boolean> paths = new HashMap<>();
            for (Path loc : locPaths) {
                paths.put(loc, false);
            }
            this.locations = paths;
            return this;
        }

        @Override
        public DataNotificationRequest build() {
            if (callbackId == null || locations == null
                    || cluster == null || pollingFrequencyInMillis <= 0
                    || timeoutInMillis < pollingFrequencyInMillis) {
                throw new IllegalArgumentException("Missing or incorrect, one or more of the mandatory arguments:"
                        + " callbackId, locations, dataType, cluster, pollingFrequency, waitTime");
            }
            return new DataNotificationRequest(handler, callbackId, cluster,
                    pollingFrequencyInMillis, timeoutInMillis, locations);
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
                    boolean isDataArrived = checkConditions(dataNotificationRequest);
                    if (isDataArrived) {
                        notifyHandler(dataNotificationRequest, DataEvent.STATUS.AVAILABLE);
                    } else {
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

        private void notifyHandler(DataNotificationRequest dataNotificationRequest,
                                   DataEvent.STATUS status) {
            DataEvent dataEvent = new DataEvent(dataNotificationRequest.getCallbackId(),
                    dataNotificationRequest.getLocations(), status);
            boolean isUnRegistered = isUnRegistered(dataNotificationRequest);
            if (isUnRegistered) {
                return;
            }
            try {
                LOG.debug("Notifying Handler for Data Notification Request of id {} " ,
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

        private boolean checkConditions(DataNotificationRequest dataNotificationRequest) {
            try {
                Entity entity = EntityUtil.getEntity(EntityType.CLUSTER, dataNotificationRequest.getCluster());
                Cluster clusterEntity = (Cluster) entity;
                Configuration conf = ClusterHelper.getConfiguration(clusterEntity);
                FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(conf);
                Map<Path, Boolean> locations = dataNotificationRequest.getLocationMap();
                List<Path> nonAvailablePaths = getUnAvailablePaths(locations);
                updatePathsAvailability(nonAvailablePaths, fs, locations);
                if (allPathsExist(locations)) {
                    return true;
                }
            } catch (FalconException e) {
                LOG.error("Retrieving the Cluster Entity " + e);
            } catch (IOException e) {
                LOG.error("Unable to connect to FileSystem " + e);
            }
            return false;
        }

        private void updatePathsAvailability(List<Path> unAvailablePaths, FileSystem fs,
                                             Map<Path, Boolean> locations) throws IOException {
            for (Path path : unAvailablePaths) {
                if (fs.exists(path)) {
                    locations.put(path, true);
                }
            }
        }

        private List<Path> getUnAvailablePaths(Map<Path, Boolean> locations) {
            List<Path> paths = new ArrayList<>();
            for (Map.Entry<Path, Boolean> pathInfo : locations.entrySet()) {
                if (!pathInfo.getValue()) {
                    paths.add(pathInfo.getKey());
                }
            }
            return paths;
        }

        private boolean allPathsExist(Map<Path, Boolean> locations) {
            if (locations.containsValue(Boolean.FALSE)) {
                return false;
            }
            return true;
        }
    }

}
