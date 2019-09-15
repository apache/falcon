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
package org.apache.falcon.notification.service.request;

import org.apache.falcon.execution.NotificationHandler;
import org.apache.falcon.state.ID;
import org.apache.hadoop.fs.Path;

import java.util.Map;

/**
 * Request intended for {@link import org.apache.falcon.notification.service.impl.DataAvailabilityService}
 * for data notifications.
 * The setter methods of the class support chaining similar to a builder class.
 */
public class LocationBasedDataNotificationRequest extends DataNotificationRequest {

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    /**
     * Constructor.
     *  @param notifHandler
     * @param callbackId
     * @param cluster
     * @param pollingFrequencyInMillis
     * @param timeoutInMillis
     * @param locations
     * @param inputName
     */
    public LocationBasedDataNotificationRequest(NotificationHandler notifHandler, ID callbackId,
                                                String cluster, long pollingFrequencyInMillis,
                                                long timeoutInMillis, Map<Path, Boolean> locations,
                                                String inputName, boolean optional) {
        super(notifHandler, callbackId, cluster, pollingFrequencyInMillis, timeoutInMillis, inputName, optional);
        this.locations = locations;
    }

    //RESUME CHECKSTYLE CHECK ParameterNumberCheck
}
