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
package org.apache.falcon.notification.service;

import org.apache.falcon.exception.NotificationServiceException;
import org.apache.falcon.execution.NotificationHandler;
import org.apache.falcon.notification.service.request.NotificationRequest;
import org.apache.falcon.service.Services;
import org.apache.falcon.state.ID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service registry that manages the notification services.
 * This class is also responsible for routing any register and unregister calls to the appropriate service.
 */
public final class NotificationServicesRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(NotificationServicesRegistry.class);

    /**
     * A list of notifiation service that the scheduler framework uses.
     */
    public enum SERVICE {
        TIME("AlarmService"),
        DATA("DataAvailabilityService"),
        JOB_COMPLETION("JobCompletionService"),
        JOB_SCHEDULE("JobSchedulerService");

        private final String name;

        private SERVICE(String name) {
            this.name = name;
        }

        public String toString() {
            return name;
        }
    }

    private NotificationServicesRegistry() {
    }

    /**
     * Routes the notification request to appropriate service based on the request.
     *
     * @param notifRequest
     */
    public static void register(NotificationRequest notifRequest) throws NotificationServiceException {
        FalconNotificationService service = getService(notifRequest.getService());
        service.register(notifRequest);
    }

    /**
     * De-registers the listener from all services.
     *
     * @param listenerID
     */
    public static void unregister(NotificationHandler handler, ID listenerID)
        throws NotificationServiceException {
        for (SERVICE service : SERVICE.values()) {
            unregisterForNotification(handler, listenerID, service);
        }
    }

    /**
     * @param serviceType - Type of service requested
     * @return An instance of {@link org.apache.falcon.notification.service.FalconNotificationService}
     */
    public static FalconNotificationService getService(SERVICE serviceType) {
        FalconNotificationService service = Services.get().getService(serviceType.toString());
        if (service == null) {
            LOG.error("Unable to find service type : {} . Service not registered.", serviceType.toString());
            throw new RuntimeException("Unable to find service : " + serviceType.toString()
                    + " . Service not registered.");
        }
        return service;
    }

    /**
     * @param serviceName - Name of service requested
     * @return - An instance of {@link org.apache.falcon.notification.service.FalconNotificationService}
     * @throws NotificationServiceException
     */
    public static FalconNotificationService getService(String serviceName) throws NotificationServiceException {
        SERVICE serviceType = null;
        for (SERVICE type : SERVICE.values()) {
            if (type.toString().equals(serviceName)) {
                serviceType = type;
            }
        }
        if (serviceType == null) {
            LOG.error("Unable to find service : {}. Not a valid service.", serviceName);
            throw new NotificationServiceException("Unable to find service : " + serviceName
                    + " . Not a valid service.");
        }
        return getService(serviceType);
    }

    /**
     * Routes the unregister request to the mentioned service.
     * @param handler
     * @param listenerID
     * @param service
     */
    public static void unregisterForNotification(NotificationHandler handler, ID listenerID, SERVICE service)
        throws NotificationServiceException {
        FalconNotificationService falconNotificationService = getService(service);
        falconNotificationService.unregister(handler, listenerID);
    }
}
