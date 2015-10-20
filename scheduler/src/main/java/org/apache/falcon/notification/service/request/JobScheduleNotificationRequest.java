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

import org.apache.falcon.execution.ExecutionInstance;
import org.apache.falcon.execution.NotificationHandler;
import org.apache.falcon.notification.service.NotificationServicesRegistry;
import org.apache.falcon.state.ID;

import java.util.List;

/**
 * Request intended for {@link org.apache.falcon.notification.service.impl.SchedulerService}
 * for job run notifications.
 * The setter methods of the class support chaining similar to a builder class.
 */
public class JobScheduleNotificationRequest extends NotificationRequest {
    private ExecutionInstance instance;
    private List<ExecutionInstance> dependencies;

    /**
     * Constructor.
     * @param notifHandler
     * @param id
     */
    public JobScheduleNotificationRequest(NotificationHandler notifHandler, ID id, ExecutionInstance inst,
                                          List<ExecutionInstance> deps) {
        this.handler = notifHandler;
        this.service = NotificationServicesRegistry.SERVICE.JOB_SCHEDULE;
        this.callbackId = id;
        this.instance = inst;
        this.dependencies = deps;
    }

    /**
     * @return execution instance that will be scheduled.
     */
    public ExecutionInstance getInstance() {
        return instance;
    }

    public List<ExecutionInstance> getDependencies() {
        return dependencies;
    }
}
