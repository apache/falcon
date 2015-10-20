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

import org.apache.falcon.entity.v0.Frequency;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test for Utility methods.
 */
public class SchedulerUtilTest {

    @Test(dataProvider = "frequencies")
    public void testGetFrequencyInMillis(DateTime referenceTime, Frequency frequency, long expectedValue) {
        Assert.assertEquals(SchedulerUtil.getFrequencyInMillis(referenceTime, frequency), expectedValue);
    }

    @DataProvider(name = "frequencies")
    public Object[][] getTestFrequencies() {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss");
        return new Object[][] {
            {DateTime.now(), new Frequency("minutes(10)"), 10*60*1000L},
            {DateTime.now(), new Frequency("hours(6)"), 6*60*60*1000L},
            // Feb of leap year
            {formatter.parseDateTime("04/02/2012 14:00:00"), new Frequency("months(1)"), 29*24*60*60*1000L},
            // Months with 31 and 30 days
            {formatter.parseDateTime("02/10/2015 03:30:00"), new Frequency("months(2)"), (31+30)*24*60*60*1000L},
        };
    }
}
