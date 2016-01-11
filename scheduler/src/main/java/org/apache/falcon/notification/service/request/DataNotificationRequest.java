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
import org.apache.falcon.notification.service.NotificationServicesRegistry;
import org.apache.falcon.state.ID;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * Request intended for {@link import org.apache.falcon.notification.service.impl.DataAvailabilityService}
 * for data notifications.
 * The setter methods of the class support chaining similar to a builder class.
 */
public class DataNotificationRequest extends NotificationRequest implements Delayed {
    // Boolean represents path availability to avoid checking all paths for every poll.
    private Map<Path, Boolean> locations;
    private long pollingFrequencyInMillis;
    private long timeoutInMillis;
    private String cluster;
    private long accessTimeInMillis;
    private long createdTimeInMillis;
    // Represents request was accessed by DataAvailability service first time or not.
    private boolean isFirst;


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
     * @param cluster
     * @param pollingFrequencyInMillis
     * @param timeoutInMillis
     * @param locations
     */
    public DataNotificationRequest(NotificationHandler notifHandler, ID callbackId,
                                   String cluster, long pollingFrequencyInMillis,
                                   long timeoutInMillis, Map<Path, Boolean> locations) {
        this.handler = notifHandler;
        this.callbackId = callbackId;
        this.service = NotificationServicesRegistry.SERVICE.DATA;
        this.cluster = cluster;
        this.pollingFrequencyInMillis = pollingFrequencyInMillis;
        this.timeoutInMillis = timeoutInMillis;
        this.locations = locations;
        this.accessTimeInMillis = System.currentTimeMillis();
        this.createdTimeInMillis = accessTimeInMillis;
        this.isFirst = true;
    }


    public void accessed() {
        this.accessTimeInMillis = System.currentTimeMillis();
    }

    public String getCluster() {
        return cluster;
    }


    public boolean isTimedout() {
        long currentTimeInMillis = System.currentTimeMillis();
        if (currentTimeInMillis - createdTimeInMillis > timeoutInMillis) {
            return true;
        }
        return false;
    }


    /**
     * Obtain list of paths from locations map.
     * @return List of paths to check.
     */
    public List<Path> getLocations() {
        if (this.locations == null) {
            return null;
        }
        List<Path> paths = new ArrayList<>();
        for (Path path : this.locations.keySet()) {
            paths.add(path);
        }
        return paths;
    }

    /**
     * @return Map of locations and their availabilities.
     */
    public Map<Path, Boolean> getLocationMap() {
        return this.locations;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        if (isFirst) {
            this.isFirst = false;
            return 0;
        }
        long age = System.currentTimeMillis() - accessTimeInMillis;
        return unit.convert(pollingFrequencyInMillis - age, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
        return (int) (this.getDelay(TimeUnit.MILLISECONDS) - other.getDelay(TimeUnit.MILLISECONDS));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataNotificationRequest that = (DataNotificationRequest) o;
        if (!StringUtils.equals(cluster, that.cluster)) {
            return false;
        }
        if (!locations.equals(that.locations)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = cluster.hashCode();
        result = 31 * result + (locations != null ? locations.hashCode() : 0);
        return result;
    }


}
