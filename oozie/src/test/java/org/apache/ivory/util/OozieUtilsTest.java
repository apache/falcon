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

package org.apache.ivory.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.parser.Frequency;
import org.testng.Assert;
import org.testng.annotations.Test;

public class OozieUtilsTest {

    private Date getDate(String date) throws Exception {
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm Z");
        return format.parse(date);
    }

    @Test
    public void mytest() throws Exception {
        Date start = EntityUtil.parseDateUTC("2012-05-18T10:41Z");
        Date now = EntityUtil.parseDateUTC("2012-05-18T10:58Z");
        Date nextStart = OozieUtils.getNextStartTime(start, Frequency.days, 1, "UTC", now);
        System.out.println(EntityUtil.formatDateUTC(nextStart));
    }
    
    @Test
    public void testGetNextStartTime() throws Exception {
        Date now = getDate("2012-04-03 02:45 UTC");
        Date start = getDate("2012-04-02 03:00 UTC");
        Date newStart = getDate("2012-04-03 03:00 UTC");

        Frequency frequency = Frequency.hours;
        Assert.assertEquals(newStart, OozieUtils.getNextStartTime(start,
                frequency, 1, "UTC", now));
        Assert.assertEquals(OozieUtils.getNextStartTime(start, frequency, 1, "UTC", now),
                OozieUtils.getNextStartTimeOld(start, frequency, 1, "UTC", now));
    }

    @Test
    public void testGetNextStartTime1() throws Exception {
        Date now = getDate("2012-05-02 02:45 UTC");
        Date start = getDate("2012-02-01 03:00 UTC");
        Date newStart = getDate("2012-05-02 03:00 UTC");

        Frequency frequency = Frequency.days;
        Assert.assertEquals(newStart, OozieUtils.getNextStartTime(start,
                frequency, 7, "UTC", now));
        Assert.assertEquals(OozieUtils.getNextStartTime(start, frequency, 7, "UTC", now),
                OozieUtils.getNextStartTimeOld(start, frequency, 7, "UTC", now));
    }

    @Test
    public void testGetNextStartTime2() throws Exception {
        Date now = getDate("2010-05-02 04:45 UTC");
        Date start = getDate("2010-02-01 03:00 UTC");
        Date newStart = getDate("2010-05-03 03:00 UTC");

        Frequency frequency = Frequency.days;
        Assert.assertEquals(newStart, OozieUtils.getNextStartTime(start,
                frequency, 7, "UTC", now));
        Assert.assertEquals(OozieUtils.getNextStartTime(start, frequency, 7, "UTC", now),
                OozieUtils.getNextStartTimeOld(start, frequency, 7, "UTC", now));
    }

    @Test
    public void testGetNextStartTime3() throws Exception {
        Date now = getDate("2010-05-02 04:45 UTC");
        Date start = getDate("1980-02-01 03:00 UTC");
        Date newStart = getDate("2010-05-07 03:00 UTC");

        Frequency frequency = Frequency.days;
        Assert.assertEquals(newStart, OozieUtils.getNextStartTime(start,
                frequency, 7, "UTC", now));
        Assert.assertEquals(newStart,
                OozieUtils.getNextStartTimeOld(start, frequency, 7, "UTC", now));
    }
}
