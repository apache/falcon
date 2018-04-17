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
import org.apache.falcon.entity.parser.ClusterEntityParser;
import org.apache.falcon.entity.parser.EntityParserFactory;
import org.apache.falcon.entity.parser.ProcessEntityParser;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LateArrival;
import org.apache.falcon.entity.v0.feed.Property;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
    public void  testGetInstancesInBetween(){
        Date startTime = SchemaHelper.parseDateUTC("2016-09-30T15:24Z");
        Date endTime = SchemaHelper.parseDateUTC("2016-09-30T17:04Z");
        Frequency frequency = new Frequency("minutes(5)");
        Date startRange = SchemaHelper.parseDateUTC("2016-09-30T15:25Z");
        Date endRange = SchemaHelper.parseDateUTC("2016-09-30T15:30Z");
        List<Date> instances = EntityUtil.getInstancesInBetween(startTime, endTime, frequency, tz, startRange,
                endRange);
        startRange = SchemaHelper.parseDateUTC("2016-09-30T15:18Z");
        endRange = SchemaHelper.parseDateUTC("2016-09-30T15:24Z");
        instances.addAll(EntityUtil.getInstancesInBetween(startTime, endTime, frequency, tz, startRange, endRange));
        Assert.assertEquals(instances.size(), 2);
        startRange = SchemaHelper.parseDateUTC("2016-09-30T15:24Z");
        endRange = SchemaHelper.parseDateUTC("2016-09-30T15:25Z");
        instances = EntityUtil.getInstancesInBetween(startTime, endTime, frequency, tz, startRange, endRange);
        Assert.assertEquals(instances.size(), 1);

        frequency = new Frequency("minutes(2)");
        startRange = SchemaHelper.parseDateUTC("2016-09-30T16:32Z");
        endRange = SchemaHelper.parseDateUTC("2016-09-30T17:02Z");
        instances = EntityUtil.getInstancesInBetween(startTime, endTime, frequency, tz, startRange, endRange);
        Assert.assertEquals(instances.size(), 16);
        startRange = SchemaHelper.parseDateUTC("2016-09-30T15:24Z");
        endRange = SchemaHelper.parseDateUTC("2016-09-30T17:05Z");
        instances = EntityUtil.getInstancesInBetween(startTime, endTime, frequency, tz, startRange, endRange);
        Assert.assertEquals(instances.size(), 50);

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
    public void testGetFeedProperties() {
        Feed feed = new Feed();
        org.apache.falcon.entity.v0.feed.Properties props = new org.apache.falcon.entity.v0.feed.Properties();
        Property queue = new Property();
        String name = "Q";
        String value = "head of Q division!";
        queue.setName(name);
        queue.setValue(value);
        props.getProperties().add(queue);
        feed.setProperties(props);
        Properties actual = EntityUtil.getEntityProperties(feed);
        Assert.assertEquals(actual.size(), 1);
        Assert.assertEquals(actual.getProperty(name), value);
    }

    @Test
    public void testGetProcessProperties() {
        org.apache.falcon.entity.v0.cluster.Cluster cluster = new org.apache.falcon.entity.v0.cluster.Cluster();
        org.apache.falcon.entity.v0.cluster.Properties props = new org.apache.falcon.entity.v0.cluster.Properties();
        org.apache.falcon.entity.v0.cluster.Property priority = new org.apache.falcon.entity.v0.cluster.Property();
        String name = "priority";
        String value = "Sister of Moriarity!";
        priority.setName(name);
        priority.setValue(value);
        props.getProperties().add(priority);
        cluster.setProperties(props);
        Properties actual = EntityUtil.getEntityProperties(cluster);
        Assert.assertEquals(actual.size(), 1);
        Assert.assertEquals(actual.getProperty(name), value);
    }

    @Test
    public void testGetClusterProperties() {
        Process process = new Process();
        org.apache.falcon.entity.v0.process.Properties props = new org.apache.falcon.entity.v0.process.Properties();
        org.apache.falcon.entity.v0.process.Property priority = new org.apache.falcon.entity.v0.process.Property();
        String name = "M";
        String value = "Minions!";
        priority.setName(name);
        priority.setValue(value);
        props.getProperties().add(priority);
        process.setProperties(props);
        Properties actual = EntityUtil.getEntityProperties(process);
        Assert.assertEquals(actual.size(), 1);
        Assert.assertEquals(actual.getProperty(name), value);

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

    @Test(dataProvider = "bundlePaths")
    public void testIsStagingPath(Path path, boolean createPath, boolean expected) throws Exception {
        ClusterEntityParser parser = (ClusterEntityParser) EntityParserFactory.getParser(EntityType.CLUSTER);
        InputStream stream = this.getClass().getResourceAsStream(CLUSTER_XML);
        org.apache.falcon.entity.v0.cluster.Cluster cluster = parser.parse(stream);

        ProcessEntityParser processParser = (ProcessEntityParser) EntityParserFactory.getParser(EntityType.PROCESS);
        stream = this.getClass().getResourceAsStream(PROCESS_XML);
        Process process = processParser.parse(stream);

        FileSystem fs = HadoopClientFactory.get().
                createFalconFileSystem(ClusterHelper.getConfiguration(cluster));
        if (createPath && !fs.exists(path)) {
            fs.create(path);
        }

        Assert.assertEquals(EntityUtil.isStagingPath(cluster, process, path), expected);
        fs.delete(path);
    }

    @DataProvider(name = "bundlePaths")
    public Object[][] getBundlePaths() {
        return new Object[][] {
            {new Path("/projects/falcon/staging/ivory/workflows/process/sample/"), true, true},
            {new Path("/projects/falcon/staging/falcon/workflows/process/sample/"), true, true},
            {new Path("/projects/abc/falcon/workflows/process/sample/"), true, false},
            {new Path("/projects/falcon/staging/falcon/workflows/process/test-process/"), false, false},
            {new Path("/projects/falcon/staging/falcon/workflows/process/test-process/"), true, false},
        };
    }

    @Test
    public void testStringToProps() {
        String testPropsString = "key1:value1,key2 : value2 , key3: value3, key4:value4:test";
        Map<String, String> props = EntityUtil.getPropertyMap(testPropsString);
        Assert.assertEquals(props.size(), 4);
        for (int i = 1; i <= 3; i++) {
            Assert.assertEquals(props.get("key" + i), "value" + i);
        }
        Assert.assertEquals(props.get("key4"), "value4:test");
    }

    @Test (expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Found invalid property .*",
            dataProvider = "InvalidProps")
    public void testInvalidStringToProps(String propString) {
        String[] invalidProps = {"key1", "key1=value1", "key1:value1,key2=value2, :value"};
        EntityUtil.getPropertyMap(propString);
    }

    @DataProvider(name = "InvalidProps")
    public Object[][] getInvalidProps() {
        return new Object[][]{
            {"key1"},
            {"key1=value1"},
            {"key1:value1,key2=value2"},
            {":value"},
        };
    }

    @Test
    public void testGetLatestStagingPath() throws FalconException, IOException {
        ClusterEntityParser parser = (ClusterEntityParser) EntityParserFactory.getParser(EntityType.CLUSTER);
        InputStream stream = this.getClass().getResourceAsStream(CLUSTER_XML);
        org.apache.falcon.entity.v0.cluster.Cluster cluster = parser.parse(stream);

        ProcessEntityParser processParser = (ProcessEntityParser) EntityParserFactory.getParser(EntityType.PROCESS);
        stream = this.getClass().getResourceAsStream(PROCESS_XML);
        Process process = processParser.parse(stream);
        process.setName("staging-test");

        String md5 = EntityUtil.md5(EntityUtil.getClusterView(process, "testCluster"));
        FileSystem fs = HadoopClientFactory.get().
                createFalconFileSystem(ClusterHelper.getConfiguration(cluster));

        String basePath = "/projects/falcon/staging/falcon/workflows/process/staging-test/";
        Path[] paths = {
            new Path(basePath + "5a8100dc460b44db2e7bfab84b24cb92_1436441045003"),
            new Path(basePath + "6b3a1b6c7cf9de62c78b125415ffb70c_1436504488677"),
            new Path(basePath + md5 + "_1436344303117"),
            new Path(basePath + md5 + "_1436347924846"),
            new Path(basePath + md5 + "_1436357052992"),
            new Path(basePath + "logs"),
            new Path(basePath + "random_dir"),
        };

        // Ensure exception is thrown when there are no staging dirs.
        fs.delete(new Path(basePath), true);
        try {
            EntityUtil.getLatestStagingPath(cluster, process);
            Assert.fail("Exception expected");
        } catch (FalconException e) {
            // Do nothing
        }

        // Now create paths
        for (Path path : paths) {
            fs.create(path);
        }

        // Ensure latest is returned.
        Assert.assertEquals(EntityUtil.getLatestStagingPath(cluster, process).getName(), md5 + "_1436357052992");
    }

    @Test
    public void testIsClusterUsedByEntity() throws Exception {
        Process process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(
                getClass().getResourceAsStream(PROCESS_XML));
        Feed feed = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(
                getClass().getResourceAsStream(FEED_XML));
        org.apache.falcon.entity.v0.cluster.Cluster cluster =
                (org.apache.falcon.entity.v0.cluster.Cluster) EntityType.CLUSTER.getUnmarshaller().unmarshal(
                        getClass().getResourceAsStream(CLUSTER_XML));

        Assert.assertTrue(EntityUtil.isEntityDependentOnCluster(cluster, "testCluster"));
        Assert.assertTrue(EntityUtil.isEntityDependentOnCluster(feed, "testCluster"));
        Assert.assertTrue(EntityUtil.isEntityDependentOnCluster(feed, "backupCluster"));
        Assert.assertTrue(EntityUtil.isEntityDependentOnCluster(process, "testCluster"));

        Assert.assertFalse(EntityUtil.isEntityDependentOnCluster(cluster, "fakeCluster"));
        Assert.assertFalse(EntityUtil.isEntityDependentOnCluster(feed, "fakeCluster"));
        Assert.assertFalse(EntityUtil.isEntityDependentOnCluster(process, "fakeCluster"));
    }

    @Test
    public void testGetNextInstanceTimeWithDelay() throws Exception {
        Date date = getDate("2016-08-10 03:00 UTC");
        Frequency delay = new Frequency("hours(2)");
        Date nextInstanceWithDelay = EntityUtil.getNextInstanceTimeWithDelay(date, delay, TimeZone.getTimeZone("UTC"));
        Assert.assertEquals(nextInstanceWithDelay, getDate("2016-08-10 05:00 UTC"));
    }

}
