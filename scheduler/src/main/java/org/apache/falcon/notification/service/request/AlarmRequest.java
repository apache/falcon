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

import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.execution.NotificationHandler;
import org.apache.falcon.notification.service.NotificationServicesRegistry;
import org.apache.falcon.state.ID;
import org.joda.time.DateTime;

import java.util.TimeZone;

/**
 * Request intended for {@link org.apache.falcon.notification.service.impl.AlarmService}
 * for time based notifications.
 * The setter methods of the class support chaining similar to a builder class.
 * TODO : Might need a separate builder too.
 */
public class AlarmRequest extends NotificationRequest {

    private DateTime startTime;
    private DateTime endTime;
    private Frequency frequency;
    private TimeZone timeZone;

    /**
     * Constructor.
     * @param notifHandler
     * @param callbackId
     */
    public AlarmRequest(NotificationHandler notifHandler, ID callbackId, DateTime start,
                        DateTime end, Frequency freq, TimeZone tz) {
        this.handler = notifHandler;
        this.callbackId = callbackId;
        this.service = NotificationServicesRegistry.SERVICE.TIME;
        this.startTime = start;
        this.endTime = end;
        this.frequency = freq;
        this.timeZone = tz;
    }

    /**
     * @return frequency of the timer
     */
    public Frequency getFrequency() {
        return frequency;
    }

    /**
     * @return start time of the timer
     */
    public DateTime getStartTime() {
        return startTime;
    }

    /**
     * @return end time of the timer
     */
    public DateTime getEndTime() {
        return endTime;
    }

    /**
     * @return timezone of the request.
     */
    public TimeZone getTimeZone() {
        return timeZone;
    }
}
