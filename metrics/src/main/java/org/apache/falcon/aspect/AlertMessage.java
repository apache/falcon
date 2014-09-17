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

package org.apache.falcon.aspect;

/**
 * Message to be sent to the alerting system.
 */
public class AlertMessage {

    private final String event;
    private final String alert;
    private final String error;

    public AlertMessage(String event, String alert, String error) {
        this.event = event;
        this.alert = alert;
        this.error = error;
    }

    public String getEvent() {
        return event;
    }

    public String getAlert() {
        return alert;
    }

    public String getError() {
        return error;
    }

    @Override
    public String toString() {
        return "AlertMessage{"
                + "event='" + event + '\''
                + ", alert='" + alert + '\''
                + ", error='" + error + '\''
                + '}';
    }
}
