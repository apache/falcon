package org.apache.ivory.entity;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.Frequency;
import org.apache.ivory.entity.v0.SchemaHelper;
import org.apache.ivory.entity.v0.process.Process;
import org.testng.Assert;
import org.testng.annotations.Test;

public class EntityUtilTest {
    private static TimeZone tz = TimeZone.getTimeZone("UTC");
    private static final String PROCESS_XML = "/config/process/process-0.1.xml";

    @Test
    public void testEquals() throws Exception {
        Process process1 = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(
                getClass().getResourceAsStream(PROCESS_XML));
        Process process2 = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(
                getClass().getResourceAsStream(PROCESS_XML));
        Assert.assertTrue(EntityUtil.equals(process1, process2));
        Assert.assertTrue(EntityUtil.md5(process1).equals(EntityUtil.md5(process2)));

        process2.getClusters().getClusters().get(0).getValidity().setEnd(SchemaHelper.parseDateUTC("2013-04-21T00:00Z"));
        Assert.assertFalse(EntityUtil.equals(process1, process2));
        Assert.assertFalse(EntityUtil.md5(process1).equals(EntityUtil.md5(process2)));
        Assert.assertTrue(EntityUtil.equals(process1, process2, new String[] {"clusters.clusters[\\d+].validity.end"}));
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
                frequency,tz, instance));
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

}
