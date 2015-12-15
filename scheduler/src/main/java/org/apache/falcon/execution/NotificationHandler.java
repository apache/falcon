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
package org.apache.falcon.execution;

import org.apache.falcon.FalconException;
import org.apache.falcon.notification.service.event.Event;

/**
 * An interface that every class that handles notifications from notification services must implement.
 */
public interface NotificationHandler {

    /**
     * When there are multiple notification handlers for the same event,
     * the priority determines which handler gets notified first.
     */
    enum PRIORITY {HIGH(10), MEDIUM(5), LOW(0);


        private final int priority;

        PRIORITY(int i) {
            this.priority = i;
        }

        public int getPriority() {
            return priority;
        }
    }
    /**
     * The method a notification service calls to onEvent an event.
     *
     * @param event
     * @throws FalconException
     */
    void onEvent(Event event) throws FalconException;

    /**
     * When there are multiple notification handlers for the same event,
     * the priority determines which handler gets notified first.
     * @return
     */
    PRIORITY getPriority();
}
