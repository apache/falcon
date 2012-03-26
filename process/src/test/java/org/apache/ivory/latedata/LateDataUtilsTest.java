/*
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

package org.apache.ivory.latedata;

import org.testng.Assert;
import org.testng.annotations.Test;

public class LateDataUtilsTest {

    @Test
    public void testOffsetTime() throws Exception {
        Assert.assertEquals("hello", LateDataUtils.offsetTime("hello", "minutes(0)"));
        Assert.assertEquals("todayWithOffset(0,0, -0)", LateDataUtils.offsetTime("today(0,0)", "minutes(0)"));
        Assert.assertEquals("todayWithOffset(1, 1, -1)", LateDataUtils.offsetTime("today (1, 1)", "minutes(1)"));
        Assert.assertEquals("yesterdayWithOffset(0,  1, -20)", LateDataUtils.offsetTime("yesterday(0,  1)", "minutes(20)"));

        Assert.assertEquals("currentYearWithOffset(0,  1, 0, 0, -200)", LateDataUtils.offsetTime("currentYear(0,  1, 0, 0)", "minutes(200)"));
        Assert.assertEquals("lastYearWithOffset(  4,  5, 6, 7, -200)", LateDataUtils.offsetTime("lastYear    (  4,  5, 6, 7)", "minutes(200)"));
        Assert.assertEquals("currentMonthWithOffset(3,  -3,  14, -200)", LateDataUtils.offsetTime("currentMonth(3,  -3,  14)", "minutes(200)"));
        Assert.assertEquals("lastMonthWithOffset(3,  -3,  14, -200)", LateDataUtils.offsetTime("lastMonth(3,  -3,  14)", "minutes(200)"));
        Assert.assertEquals("myfunc(1, 2, 3)", LateDataUtils.offsetTime("myfunc(1, 2, 3)", "minutes(200)"));

        Assert.assertEquals("now  (0, 0-200)", LateDataUtils.offsetTime("now  (0, 0)", "minutes(200)"));
        //Assert.assertEquals("   hellonow(0,50  )", LateDataUtils.offsetTime("   hellonow(0,50  )", "minutes(50)"));
        Assert.assertEquals("   now(0,50  -50)", LateDataUtils.offsetTime("   now(0,50  )", "minutes(50)"));
        try {
            LateDataUtils.offsetTime("yesterday(0,  1)", "minute(20)");
            Assert.fail("Expecting to fail");
        } catch (Exception ignore) {
        }
    }
}
