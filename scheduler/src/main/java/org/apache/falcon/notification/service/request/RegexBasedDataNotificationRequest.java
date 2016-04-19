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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.execution.NotificationHandler;
import org.apache.falcon.execution.SchedulerUtil;
import org.apache.falcon.state.ID;


/**
 * Request intended for {@link import org.apache.falcon.notification.service.impl.DataAvailabilityService}
 * for data notifications.
 * The setter methods of the class support chaining similar to a builder class.
 */
public class RegexBasedDataNotificationRequest extends DataNotificationRequest {


    private long startTimeinMillis;
    private int startInstance;
    private int endInstance;
    private long endTimeInMillis;
    private String basePath;
    private long frequencyInMillis;

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    /**
     * Constructor.
     * @param notifHandler
     * @param callbackId
     * @param cluster
     * @param pollingFrequencyInMillis
     * @param timeoutInMillis
     * @param basePath
     * @param frequencyInMillis
     * @param inputName
     */
    public RegexBasedDataNotificationRequest(NotificationHandler notifHandler, ID callbackId, String cluster,
                                             long pollingFrequencyInMillis, long timeoutInMillis,
                                             long startTimeinMillis, long endTimeInMillis, int startInstance,
                                             int endInstance, SchedulerUtil.EXPTYPE expType, String basePath,
                                             long frequencyInMillis, String inputName, boolean optional) {
        super(notifHandler, callbackId, cluster, pollingFrequencyInMillis, timeoutInMillis, inputName, optional);
        this.startTimeinMillis = startTimeinMillis;
        this.endTimeInMillis = endTimeInMillis;
        this.startInstance = startInstance;
        this.endInstance = endInstance;
        this.expType = expType;
        this.basePath = basePath;
        this.frequencyInMillis = frequencyInMillis;
    }
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    public long getEndTimeInMillis() {
        return endTimeInMillis;
    }

    public String getBasePath() {
        return basePath;
    }

    public long getFrequencyInMillis() {
        return frequencyInMillis;
    }

    public long getStartTimeinMillis() {
        return startTimeinMillis;
    }

    public int getStartInstance() {
        return startInstance;
    }

    public int getEndInstance() {
        return endInstance;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RegexBasedDataNotificationRequest that = (RegexBasedDataNotificationRequest) o;
        if (!StringUtils.equals(cluster, that.cluster)) {
            return false;
        }
        if (pollingFrequencyInMillis != (that.pollingFrequencyInMillis)) {
            return false;
        }
        if (timeoutInMillis != that.timeoutInMillis) {
            return false;
        }
        if (createdTimeInMillis != that.createdTimeInMillis) {
            return false;
        }
        if (expType != that.expType) {
            return false;
        }
        if (startTimeinMillis != that.startTimeinMillis) {
            return false;
        }
        if (endTimeInMillis != that.endTimeInMillis) {
            return false;
        }
        if (!StringUtils.equals(basePath, that.basePath)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = cluster.hashCode();
        result = 31 * result + Long.valueOf(pollingFrequencyInMillis).hashCode();
        result = 31 * result + Long.valueOf(timeoutInMillis).hashCode();
        result = 31 * result + Long.valueOf(createdTimeInMillis).hashCode();
        result = 31 * result + expType.hashCode();
        result = 31 * result + Long.valueOf(startTimeinMillis).hashCode();
        result = 31 * result + Long.valueOf(endTimeInMillis).hashCode();
        result = 31 * result + basePath.hashCode();
        result = 31 * result + Long.valueOf(startInstance).hashCode();
        result = 31 * result + Long.valueOf(endInstance).hashCode();
        result = 31 * result + Long.valueOf(frequencyInMillis).hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "cluster: " + this.getCluster() + " expType: " + this.expType + " createdTime: "
                + this.createdTimeInMillis + " basePath: " + this.basePath + " pollingFrequencyInMillis: "
                + this.pollingFrequencyInMillis + " frequencyInMillis: " + this.frequencyInMillis;
    }

}
