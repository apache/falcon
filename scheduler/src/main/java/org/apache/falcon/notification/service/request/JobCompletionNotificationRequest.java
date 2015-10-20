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
import org.apache.falcon.notification.service.NotificationServicesRegistry;
import org.apache.falcon.state.ID;

/**
 * Request intended for {@link org.apache.falcon.notification.service.impl.JobCompletionService}
 * for job completion notifications.
 * The setter methods of the class support chaining similar to a builder class.
 */
public class JobCompletionNotificationRequest extends NotificationRequest {

    private String externalId;
    private String cluster;
    /**
     * Constructor.
     * @param notifHandler
     * @param callbackId
     */
    public JobCompletionNotificationRequest(NotificationHandler notifHandler, ID callbackId, String clstr,
                                            String jobId) {
        this.handler = notifHandler;
        this.service = NotificationServicesRegistry.SERVICE.JOB_COMPLETION;
        this.callbackId = callbackId;
        this.cluster = clstr;
        this.externalId = jobId;
    }

    /**
     * @return - The external job id for which job completion notification is requested.
     */
    public String getExternalId() {
        return externalId;
    }

    /**
     * @return cluster name
     */
    public String getCluster() {
        return cluster;
    }


}
