package org.apache.ivory.util;

import org.apache.ivory.entity.parser.Frequency;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class OozieUtilsTest {

    private Date getDate(String date) throws Exception {
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm Z");
        return format.parse(date);
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
