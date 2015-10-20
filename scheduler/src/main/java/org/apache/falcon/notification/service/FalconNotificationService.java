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
import org.apache.falcon.service.FalconService;
import org.apache.falcon.state.ID;

/**
 * An interface that every notification service must implement.
 */
public interface FalconNotificationService extends FalconService {

    /**
     * Register for a notification.
     *
     * @param notifRequest
     */
    void register(NotificationRequest notifRequest) throws NotificationServiceException;

    /**
     * De-register from receiving notifications.
     * @param handler - The notification handler that needs to be de-registered.
     * @param callbackID
     */
    void unregister(NotificationHandler handler, ID callbackID) throws NotificationServiceException;

    /**
     * Creates and returns an implementation of
     * {@link RequestBuilder} that is applicable to the service.
     * @param handler - The notification handler that needs to be de-registered.
     * @param callbackID
     * @return
     */
    RequestBuilder createRequestBuilder(NotificationHandler handler, ID callbackID);

    /**
     * Builder to build appropriate {@link NotificationRequest}.
     * @param <T>
     */
    abstract class RequestBuilder<T extends NotificationRequest> {

        protected NotificationHandler handler;
        protected ID callbackId;

        public RequestBuilder(NotificationHandler notificationHandler, ID callbackID) {
            if (notificationHandler == null) {
                throw new IllegalArgumentException("Handler cannot be null.");
            }
            this.handler = notificationHandler;
            this.callbackId = callbackID;
        }

        /**
         * @return Corresponding {@link NotificationRequest}.
         */
        public abstract T build();
    }
}
