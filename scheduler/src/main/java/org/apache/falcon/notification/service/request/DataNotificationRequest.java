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
import org.apache.hadoop.fs.Path;

/**
 * Request intended for {@link import org.apache.falcon.notification.service.impl.DataAvailabilityService}
 * for data notifications.
 * The setter methods of the class support chaining similar to a builder class.
 * TODO : Complete/modify this skeletal class
 */
public class DataNotificationRequest extends NotificationRequest {
    private final Path dataLocation;
    private String cluster;

    /**
     * @return data location to be watched.
     */
    public Path getDataLocation() {
        return dataLocation;
    }

    /**
     * Given a number of instances, should the service wait for exactly those many,
     * at least those many or at most those many instances.
     */
    public enum INSTANCELIMIT {
        EXACTLY_N,
        AT_LEAST_N,
        AT_MOST_N
    }

    /**
     * Constructor.
     * @param notifHandler
     * @param callbackId
     */
    public DataNotificationRequest(NotificationHandler notifHandler, ID callbackId, Path location) {
        this.handler = notifHandler;
        this.callbackId = callbackId;
        this.dataLocation = location;
        this.service = NotificationServicesRegistry.SERVICE.DATA;
    }

    /**
     * @return cluster name
     */
    public String getCluster() {
        return cluster;
    }

    /**
     * @param clusterName
     * @return This instance
     */
    public DataNotificationRequest setCluster(String clusterName) {
        this.cluster = clusterName;
        return this;
    }
}
