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

package org.apache.falcon.entity;

import org.apache.falcon.Pair;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LateArrival;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.Process;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Test for validating Entity util helper methods.
 */
public class EntityUtilTest extends AbstractTestBase {
    private static TimeZone tz = TimeZone.getTimeZone("UTC");

    @Test
    public void testProcessView() throws Exception {
        Process process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(
                getClass().getResourceAsStream(PROCESS_XML));
        Cluster cluster = new Cluster();
        cluster.setName("newCluster");
        cluster.setValidity(process.getClusters().getClusters().get(0).getValidity());
        process.getClusters().getClusters().add(cluster);
        Assert.assertEquals(process.getClusters().getClusters().size(), 2);
        String currentCluster = process.getClusters().getClusters().get(0).getName();
        Process newProcess = EntityUtil.getClusterView(process, currentCluster);
        Assert.assertFalse(EntityUtil.equals(process, newProcess));
        Assert.assertEquals(newProcess.getClusters().getClusters().size(), 1);
        Assert.assertEquals(newProcess.getClusters().getClusters().get(0).getName(), currentCluster);
    }

    @Test
    public void testFeedView() throws Exception {
        Feed feed = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
                getClass().getResourceAsStream(FEED_XML));
        Feed view = EntityUtil.getClusterView(feed, "testCluster");
        Assert.assertEquals(view.getClusters().getClusters().size(), 1);
        Assert.assertEquals(view.getClusters().getClusters().get(0).getName(), "testCluster");

        view = EntityUtil.getClusterView(feed, "backupCluster");
        Assert.assertEquals(view.getClusters().getClusters().size(), 2);
    }

    @Test
    public void testEquals() throws Exception {
        Process process1 = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(
                getClass().getResourceAsStream(PROCESS_XML));
        Process process2 = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(
                getClass().getResourceAsStream(PROCESS_XML));
        Assert.assertTrue(EntityUtil.equals(process1, process2));
        Assert.assertTrue(EntityUtil.md5(process1).equals(EntityUtil.md5(process2)));

        process2.getClusters().getClusters().get(0).getValidity().setEnd(
                SchemaHelper.parseDateUTC("2013-04-21T00:00Z"));
        Assert.assertFalse(EntityUtil.equals(process1, process2));
        Assert.assertFalse(EntityUtil.md5(process1).equals(EntityUtil.md5(process2)));
        Assert.assertTrue(EntityUtil.equals(process1, process2, new String[]{"clusters.clusters[\\d+].validity.end"}));
    }

    private static Date getDate(String date) throws Exception {
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm Z");
        return format.parse(date);
    }

    @Test
    public void testGetNextStartTime() throws Exception {
        Date now = getDate("2012-04-03 02:45 UTC");
        Date start = getDate("2012-04-02 03:00 UTC");
        Date newStart = getDate("2012-04-03 03:00 UTC");

        Frequency frequency = new Frequency("hours(1)");
        Assert.assertEquals(newStart, EntityUtil.getNextStartTime(start,
                frequency, tz, now));
    }

    @Test
    public void testgetNextStartTimeOld() throws Exception {
        Date now = getDate("2012-05-02 02:45 UTC");
        Date start = getDate("2012-02-01 03:00 UTC");
        Date newStart = getDate("2012-05-02 03:00 UTC");

        Frequency frequency = new Frequency("days(7)");
        Assert.assertEquals(newStart, EntityUtil.getNextStartTime(start,
                frequency, tz, now));
    }

    @Test
    public void testGetNextStartTime2() throws Exception {
        Date now = getDate("2010-05-02 04:45 UTC");
        Date start = getDate("2010-02-01 03:00 UTC");
        Date newStart = getDate("2010-05-03 03:00 UTC");

        Frequency frequency = new Frequency("days(7)");
        Assert.assertEquals(newStart, EntityUtil.getNextStartTime(start,
                frequency, tz, now));
    }

    @Test
    public void testGetNextStartTime3() throws Exception {
        Date now = getDate("2010-05-02 04:45 UTC");
        Date start = getDate("1980-02-01 03:00 UTC");
        Date newStart = getDate("2010-05-07 03:00 UTC");

        Frequency frequency = new Frequency("days(7)");
        Assert.assertEquals(newStart, EntityUtil.getNextStartTime(start,
                frequency, tz, now));
    }


    @Test
    public void testGetInstanceSequence() throws Exception {
        Date instance = getDate("2012-05-22 13:40 UTC");
        Date start = getDate("2012-05-14 07:40 UTC");

        Frequency frequency = new Frequency("hours(1)");
        Assert.assertEquals(199, EntityUtil.getInstanceSequence(start,
                frequency, tz, instance));
    }

    @Test
    public void testGetInstanceSequence1() throws Exception {
        Date instance = getDate("2012-05-22 12:40 UTC");
        Date start = getDate("2012-05-14 07:40 UTC");

        Frequency frequency = Frequency.fromString("hours(1)");
        Assert.assertEquals(198, EntityUtil.getInstanceSequence(start,
                frequency, tz, instance));
    }

    @Test
    public void testGetInstanceSequence2() throws Exception {
        Date instance = getDate("2012-05-22 12:41 UTC");
        Date start = getDate("2012-05-14 07:40 UTC");

        Frequency frequency = Frequency.fromString("hours(1)");
        Assert.assertEquals(199, EntityUtil.getInstanceSequence(start,
                frequency, tz, instance));
    }

    @Test
    public void testGetInstanceSequence3() throws Exception {
        Date instance = getDate("2010-01-02 01:01 UTC");
        Date start = getDate("2010-01-02 01:00 UTC");

        Frequency frequency = Frequency.fromString("minutes(1)");
        Assert.assertEquals(2, EntityUtil.getInstanceSequence(start,
                frequency, tz, instance));
    }

    @Test
    public void testGetInstanceSequence4() throws Exception {
        Date instance = getDate("2010-01-01 01:03 UTC");
        Date start = getDate("2010-01-01 01:01 UTC");

        Frequency frequency = Frequency.fromString("minutes(2)");
        Assert.assertEquals(2, EntityUtil.getInstanceSequence(start,
                frequency, tz, instance));
    }

    @Test
    public void testGetInstanceSequence5() throws Exception {
        Date instance = getDate("2010-01-01 02:01 UTC");
        Date start = getDate("2010-01-01 01:01 UTC");

        Frequency frequency = Frequency.fromString("hours(1)");
        Assert.assertEquals(2, EntityUtil.getInstanceSequence(start,
                frequency, tz, instance));
    }

    @Test
    public void testGetInstanceSequence6() throws Exception {
        Date instance = getDate("2010-01-01 01:04 UTC");
        Date start = getDate("2010-01-01 01:01 UTC");

        Frequency frequency = Frequency.fromString("minutes(3)");
        Assert.assertEquals(2, EntityUtil.getInstanceSequence(start,
                frequency, tz, instance));
    }

    @Test
    public void testGetInstanceSequence7() throws Exception {
        Date instance = getDate("2010-01-01 01:03 UTC");
        Date start = getDate("2010-01-01 01:01 UTC");

        Frequency frequency = Frequency.fromString("minutes(1)");
        Assert.assertEquals(3, EntityUtil.getInstanceSequence(start,
                frequency, tz, instance));
    }

    @Test
    public void testGetNextStartTimeMonthly() throws Exception {
        Date startDate = getDate("2012-06-02 10:00 UTC");
        Date nextAfter = getDate("2136-06-02 10:00 UTC");
        Frequency frequency = Frequency.fromString("months(1)");
        Date expectedResult = nextAfter;
        Date result = EntityUtil.getNextStartTime(startDate, frequency, tz, nextAfter);
        Assert.assertEquals(result, expectedResult);
    }

    @Test
    public void testGetEntityStartEndDates() throws Exception {
        Process process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(
                getClass().getResourceAsStream(PROCESS_XML));

        Cluster cluster = new Cluster();
        cluster.setName("testCluster");
        cluster.setValidity(process.getClusters().getClusters().get(0).getValidity());

        process.getClusters().getClusters().add(cluster);

        Date expectedStartDate = new SimpleDateFormat("yyyy-MM-dd z").parse("2011-11-02 UTC");
        Date expectedEndDate = new SimpleDateFormat("yyyy-MM-dd z").parse("2091-12-30 UTC");

        Pair<Date, Date> startEndDates = EntityUtil.getEntityStartEndDates(process);
        Assert.assertEquals(startEndDates.first, expectedStartDate);
        Assert.assertEquals(startEndDates.second, expectedEndDate);
    }

    @Test
    public void testGetLateProcessFeed() throws FalconException {
        Feed feed = new Feed();

        Assert.assertNull(EntityUtil.getLateProcess(feed));
        LateArrival lateArrival = new LateArrival();
        lateArrival.setCutOff(Frequency.fromString("days(1)"));
        feed.setLateArrival(lateArrival);
        Assert.assertNotNull(EntityUtil.getLateProcess(feed));
    }

    @Test(dataProvider = "NextInstanceExpressions")
    public void testGetNextInstances(String instanceTimeStr, String frequencyStr, int instanceIncrementCount,
                                     String expectedInstanceTimeStr) throws Exception {

        Date instanceTime = getDate(instanceTimeStr);
        Frequency frequency = Frequency.fromString(frequencyStr);

        Date nextInstanceTime = EntityUtil.getNextInstanceTime(instanceTime, frequency, tz, instanceIncrementCount);

        Assert.assertEquals(nextInstanceTime, getDate(expectedInstanceTimeStr));

    }

    @DataProvider(name = "NextInstanceExpressions")
    public Object[][] nextInstanceExpressions() throws ParseException {
        String instanceTimeStr = "2014-01-01 00:00 UTC";
        return new Object[][] {
            {instanceTimeStr, "minutes(1)", 1, "2014-01-01 00:01 UTC"},
            {instanceTimeStr, "minutes(1)", 25, "2014-01-01 00:25 UTC"},

            {instanceTimeStr, "hours(1)", 1, "2014-01-01 01:00 UTC"},
            {instanceTimeStr, "hours(1)", 5, "2014-01-01 05:00 UTC"},

            {instanceTimeStr, "days(1)", 1, "2014-01-02 00:00 UTC"},
            {instanceTimeStr, "days(1)", 10, "2014-01-11 00:00 UTC"},

            {instanceTimeStr, "months(1)", 1, "2014-02-01 00:00 UTC"},
            {instanceTimeStr, "months(1)", 7, "2014-08-01 00:00 UTC"},
        };
    }

}
