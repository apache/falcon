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
import org.apache.falcon.exception.NotificationServiceException;
import org.apache.falcon.execution.NotificationHandler;
import org.apache.falcon.notification.service.FalconNotificationService;
import org.apache.falcon.notification.service.request.DataNotificationRequest;
import org.apache.falcon.notification.service.request.NotificationRequest;
import org.apache.falcon.state.ID;
import org.apache.hadoop.fs.Path;

/**
 * This notification service notifies {@link NotificationHandler} when requested data
 * becomes available. This class also supports time out, in which case it notifies about the unavailability.
 * TODO : Complete/Modify this skeletal class
 */
public class DataAvailabilityService implements FalconNotificationService {

    @Override
    public void register(NotificationRequest request) throws NotificationServiceException {
        // TODO : Implement this
    }

    @Override
    public void unregister(NotificationHandler handler, ID listenerID) {
        // TODO : Implement this
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
        // TODO : Implement this
    }

    @Override
    public void destroy() throws FalconException {

    }

    /**
     * Builds {@link DataNotificationRequest}.
     */
    public static class DataRequestBuilder extends RequestBuilder<DataNotificationRequest> {
        private Path dataLocation;

        public DataRequestBuilder(NotificationHandler handler, ID callbackID) {
            super(handler, callbackID);
        }

        /**
         * @param location
         * @return This instance
         */
        public DataRequestBuilder setDataLocation(Path location) {
            this.dataLocation = location;
            return this;
        }

        @Override
        public DataNotificationRequest build() {
            if (callbackId == null  || dataLocation == null) {
                throw new IllegalArgumentException("Missing one or more of the mandatory arguments:"
                        + " callbackId, dataLocation");
            }
            return new DataNotificationRequest(handler, callbackId, dataLocation);
        }
    }
}
