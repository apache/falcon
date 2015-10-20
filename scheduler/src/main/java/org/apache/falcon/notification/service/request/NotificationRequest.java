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
 * An abstract class that all notification requests of services must extend.
 * TODO : Complete/modify this skeleton class
 */
public abstract class NotificationRequest {
    protected NotificationHandler handler;
    protected ID callbackId;
    protected NotificationServicesRegistry.SERVICE service;

    /**
     * @return - The service that this request is intended for
     */
    public NotificationServicesRegistry.SERVICE getService() {
        return service;
    }

    /**
     * @return - The entity that needs to be notified when this request is satisfied.
     */
    public ID getCallbackId() {
        return callbackId;
    }

    /**
     * @return - The notification handler.
     */
    public NotificationHandler getHandler() {
        return handler;
    }
}
