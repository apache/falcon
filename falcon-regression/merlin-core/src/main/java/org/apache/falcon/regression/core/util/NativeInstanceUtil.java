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

package org.apache.falcon.regression.core.util;

import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.resource.InstancesResult;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.testng.Assert;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * util functions related to instanceTest.
 */
public final class NativeInstanceUtil {

    public static final int INSTANCES_CREATED_TIMEOUT = OSUtil.IS_WINDOWS ? 20 : 10;
    private static final Logger LOGGER = Logger.getLogger(NativeInstanceUtil.class);

    private NativeInstanceUtil() {
        throw new AssertionError("Instantiating utility class...");
    }

    /**
     * Waits till instance of specific entity will be created during timeout.
     * Timeout is common for most of usual test cases.
     *
     * @param cluster     ColoHelper - colo on which API to be executed
     * @param entity      definition of entity which describes job
     * @param startTime   start time of instance
     * @param endTime     end time of instance
     */
    public static void waitTillInstancesAreCreated(ColoHelper cluster, Entity entity, String startTime, String endTime)
        throws InterruptedException, IOException, AuthenticationException,
            URISyntaxException {
        int sleep = INSTANCES_CREATED_TIMEOUT;
        waitTillInstancesAreCreated(cluster, entity, startTime, endTime, sleep);
    }

    /**
     * Waits till instances of specific job will be created during specific time.
     * Use this method directly in unusual test cases where timeouts are different from trivial.
     * In other cases use waitTillInstancesAreCreated(ColoHelper,Entity,String,String)
     *
     * @param cluster     ColoHelper - colo on which API to be executed
     * @param entity      definition of entity which describes job
     * @param startTime   start time of instance
     * @param endTime     end time of instance
     * @param totalMinutesToWait     total time(in minutes) to wait for instance creation
     */
    public static void waitTillInstancesAreCreated(ColoHelper cluster, Entity entity, String startTime,
        String endTime, int totalMinutesToWait)
        throws URISyntaxException, AuthenticationException, InterruptedException, IOException {
        String entityName = entity.getName();
        EntityType type = entity.getEntityType();
        String params = "?start=" + startTime;
        params += (endTime.isEmpty() ? "" : "&end=" + endTime);

        for (int sleepCount = 0; sleepCount < totalMinutesToWait; sleepCount++) {
            InstancesResult statusResult = cluster.getProcessHelper().getProcessInstanceStatus(entityName, params);
            if (statusResult.getInstances() != null) {
                break;
            }
            LOGGER.info(type + " " + entityName + " still doesn't have instance created");
            TimeUtil.sleepSeconds(60);
        }
    }

    /**
     * Waits till given instance of process/feed reach expected state during specific time.
     *
     * @param cluster           ColoHelper - colo on which API to be executed
     * @param entity            definition of entity which describes job
     * @param instanceTime      time of instance
     * @param expectedStatus    expected status we are waiting for
     * @param frequency         frequency of process/feed
     */
    public static void waitTillInstanceReachState(ColoHelper cluster, Entity entity, String instanceTime,
        CoordinatorAction.Status expectedStatus, Frequency frequency)
        throws InterruptedException, IOException, AuthenticationException, URISyntaxException {
        int totalMinutesToWait = InstanceUtil.getMinutesToWait(entity.getEntityType(), expectedStatus);
        waitTillInstanceReachState(cluster, entity, instanceTime, expectedStatus, frequency, totalMinutesToWait);
    }

    /**
     * Waits till given instance of process/feed reach expected state during
     * specific time.
     *
     * @param cluster           ColoHelper - colo on which API to be executed
     * @param entity            definition of entity which describes job
     * @param instanceTime      time of instance
     * @param expectedStatus    expected status we are waiting for
     * @param frequency         frequency of process/feed
     * @param totalMinutesToWait     total time(in minutes) to wait
     */
    public static void waitTillInstanceReachState(ColoHelper cluster, Entity entity, String instanceTime,
        CoordinatorAction.Status expectedStatus, Frequency frequency, int totalMinutesToWait)
        throws URISyntaxException, AuthenticationException, InterruptedException, IOException {
        String entityName = entity.getName();
        EntityType type = entity.getEntityType();

        String endTime=getNextInstanceTime(instanceTime, frequency);
        String params = "?start=" + instanceTime + "&end=" + endTime;

        int maxTries = 50;
        int totalSleepTime = totalMinutesToWait * 60;
        int sleepTime = totalSleepTime / maxTries;
        LOGGER.info(String.format("Sleep for %d seconds", sleepTime));
        for (int i = 0; i < maxTries; i++) {
            InstancesResult statusResult = cluster.getProcessHelper().getProcessInstanceStatus(entityName, params);
            if (statusResult.getInstances() != null) {
                if (statusResult.getInstances()[0].getStatus().name().equals(expectedStatus.toString())) {
                    return;
                }
            }
            LOGGER.info(type + " " + entityName + " still doesn't have expected status");
            TimeUtil.sleepSeconds(sleepTime);
        }
        Assert.fail("expected state of instance was never reached");
    }

    /**
     * Returns the time of next instance for a given instanceTime.
     *
     * @param instanceTime     time of instance
     * @param frequency        frequency of process/feed
     */
    public static String  getNextInstanceTime(String instanceTime, Frequency frequency) {
        String nextInstanceTime;
        int minsToAdd = 1;
        Frequency.TimeUnit timeUnit = frequency.getTimeUnit();

        switch (timeUnit) {
        case minutes:
            minsToAdd = frequency.getFrequencyAsInt();
            break;
        case hours:
            minsToAdd = frequency.getFrequencyAsInt()*60;
            break;
        case days:
            minsToAdd = frequency.getFrequencyAsInt()*60*24;
            break;
        case months:
            minsToAdd = frequency.getFrequencyAsInt()*60*24*30;
            break;
        default:
            Assert.fail("Unexpected freqType = " + frequency);
            break;
        }
        nextInstanceTime = TimeUtil.addMinsToTime(instanceTime, minsToAdd);
        return nextInstanceTime;
    }

}
