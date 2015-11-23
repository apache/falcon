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
package org.apache.falcon.notification.service;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.execution.NotificationHandler;
import org.apache.falcon.notification.service.event.Event;
import org.apache.falcon.notification.service.impl.AlarmService;
import org.apache.falcon.state.EntityID;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.TimeZone;

/**
 * Class to test the time notification service.
 */
public class AlarmServiceTest {

    private static AlarmService timeService = Mockito.spy(new AlarmService());
    private static NotificationHandler handler = Mockito.mock(NotificationHandler.class);

    @BeforeClass
    public void setup() throws FalconException {
        timeService.init();
    }

    @Test
    // This test ensures notifications are generated for time events that have occurred in the past.
    public void testbackLogCatchup() throws Exception {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateTime now = DateTime.now(DateTimeZone.forTimeZone(tz));
        // Start time 2 mins ago
        DateTime startTime = new DateTime(now.getMillis() - 2*60*1000, DateTimeZone.forTimeZone(tz));
        // End time 2 mins later
        DateTime endTime = new DateTime(now.getMillis() + 2*60*1000 , DateTimeZone.forTimeZone(tz));

        Process mockProcess = new Process();
        mockProcess.setName("test");
        EntityID id = new EntityID(mockProcess);
//        id.setCluster("testCluster");

        AlarmService.AlarmRequestBuilder request =
                new AlarmService.AlarmRequestBuilder(handler, id);
        request.setStartTime(startTime);
        request.setEndTime(endTime);
        request.setFrequency(new Frequency("minutes(1)"));
        request.setTimeZone(TimeZone.getTimeZone("UTC"));

        timeService.register(request.build());
        // Asynchronous execution, hence a small wait.
        Thread.sleep(1000);
        // Based on the minute boundary, there might be 3.
        Mockito.verify(handler, Mockito.atLeast(2)).onEvent(Mockito.any(Event.class));

    }
}
