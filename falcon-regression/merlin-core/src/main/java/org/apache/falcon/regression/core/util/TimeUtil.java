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

import org.apache.falcon.regression.core.enumsAndConstants.FeedType;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
all time / date related util methods for merlin . need to move methods from
instanceUtil to here , pending item.
 */

public final class TimeUtil {

    private TimeUtil() {
        throw new AssertionError("Instantiating utility class...");
    }
    private static final Logger LOGGER = Logger.getLogger(TimeUtil.class);

    public static void sleepSeconds(double seconds) {
        long ms = (long) (seconds * 1000);
        try {
            TimeUnit.MILLISECONDS.sleep(ms);
        } catch (InterruptedException e) {
            LOGGER.info("Sleep was interrupted");
        }
    }

    public static String get20roundedTime(String oozieBaseTime) {
        DateTime startTime =
            new DateTime(oozieDateToDate(oozieBaseTime), DateTimeZone.UTC);
        if (startTime.getMinuteOfHour() < 20) {
            startTime = startTime.minusMinutes(startTime.getMinuteOfHour());
        } else if (startTime.getMinuteOfHour() < 40) {
            startTime = startTime.minusMinutes(startTime.getMinuteOfHour() + 20);
        } else {
            startTime = startTime.minusMinutes(startTime.getMinuteOfHour() + 40);
        }
        return dateToOozieDate(startTime.toDate());
    }

    public static List<String> getMinuteDatesOnEitherSide(int interval, int minuteSkip) {
        DateTime today = new DateTime(DateTimeZone.UTC);
        LOGGER.info("today is: " + today.toString());
        return getMinuteDatesOnEitherSide(today.minusMinutes(interval),
            today.plusMinutes(interval), minuteSkip);
    }

    public static List<String> getMinuteDatesOnEitherSide(String startOozieDate, String endOozieDate,
                                                          int minuteSkip) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");
        formatter.withZoneUTC();
        return getMinuteDatesOnEitherSide(TimeUtil.oozieDateToDate(startOozieDate),
            TimeUtil.oozieDateToDate(endOozieDate), minuteSkip, formatter);
    }

    public static List<String> getMinuteDatesOnEitherSide(DateTime startDate, DateTime endDate,
                                                          int minuteSkip) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm");
        formatter.withZoneUTC();
        return getMinuteDatesOnEitherSide(startDate, endDate, minuteSkip, formatter);
    }

    public static List<String> getMinuteDatesOnEitherSide(String startOozieDate, String endOozieDate,
                                                          int minuteSkip,
                                                          DateTimeFormatter formatter) {
        return getMinuteDatesOnEitherSide(TimeUtil.oozieDateToDate(startOozieDate),
            TimeUtil.oozieDateToDate(endOozieDate), minuteSkip, formatter);
    }

    public static List<String> getMinuteDatesOnEitherSide(DateTime startDate, DateTime endDate,
                                                          int minuteSkip,
                                                          DateTimeFormatter formatter) {
        LOGGER.info("generating data between " + formatter.print(startDate) + " and "
                + formatter.print(endDate));
        if (minuteSkip == 0) {
            minuteSkip = 1;
        }
        List<String> dates = new ArrayList<String>();
        while (!startDate.isAfter(endDate)) {
            dates.add(formatter.print(startDate));
            startDate = startDate.plusMinutes(minuteSkip);
        }
        return dates;
    }

    /**
     * Convert list of dates to list of string according to the supplied format.
     *
     * @param dates        list of dates
     * @param formatter formatter to be used for converting dates
     * @return list of strings corresponding to given dates
     */
    public static List<String> convertDatesToString(List<DateTime> dates,
                                                    DateTimeFormatter formatter) {
        List<String> dateString = new ArrayList<String>();
        formatter.withZoneUTC();
        for (DateTime date : dates) {
            dateString.add(formatter.print(date));
        }
        return dateString;
    }

    /**
     * Get all possible dates between start and end date gap between subsequent dates be one unit.
     * of feedType
     *
     * @param startDate start date
     * @param endDate   end date
     * @param feedType  type of the feed
     * @return list of dates
     */
    public static List<DateTime> getDatesOnEitherSide(DateTime startDate, DateTime endDate,
                                                      FeedType feedType) {
        return getDatesOnEitherSide(startDate, endDate, 1, feedType);
    }

    /**
     * Get all possible dates between start and end date gap between subsequent dates be one unit.
     * of feedType
     *
     * @param startDate start date
     * @param endDate   end date
     * @param skip      amount of skipping
     * @param feedType  type of the feed
     * @return list of dates
     */
    public static List<DateTime> getDatesOnEitherSide(DateTime startDate, DateTime endDate,
                                                      int skip, FeedType feedType) {
        final List<DateTime> dates = new ArrayList<DateTime>();
        if (!startDate.isAfter(endDate)) {
            dates.add(startDate);
        }
        for (int counter = 0; !startDate.isAfter(endDate) && counter < 1000; ++counter) {
            startDate = feedType.addTime(startDate, skip);
            dates.add(startDate);
        }
        return dates;
    }

    public static String getTimeWrtSystemTime(int minutes) {

        DateTime jodaTime = new DateTime(DateTimeZone.UTC);
        if (minutes > 0) {
            jodaTime = jodaTime.plusMinutes(minutes);
        } else {
            jodaTime = jodaTime.minusMinutes(-1 * minutes);
        }
        DateTimeFormatter fmt = OozieUtil.getOozieDateTimeFormatter();
        DateTimeZone tz = DateTimeZone.getDefault();
        return fmt.print(tz.convertLocalToUTC(jodaTime.getMillis(), false));
    }

    public static String addMinsToTime(String time, int minutes) {

        DateTimeFormatter fmt = OozieUtil.getOozieDateTimeFormatter();
        DateTime jodaTime = fmt.parseDateTime(time);
        jodaTime = jodaTime.plusMinutes(minutes);
        return fmt.print(jodaTime);
    }

    public static DateTime oozieDateToDate(String time) {
        DateTimeFormatter fmt = OozieUtil.getOozieDateTimeFormatter();
        fmt = fmt.withZoneUTC();
        return fmt.parseDateTime(time);
    }

    public static String dateToOozieDate(Date dt) {

        DateTime jodaTime = new DateTime(dt, DateTimeZone.UTC);
        LOGGER.info("SystemTime: " + jodaTime);
        DateTimeFormatter fmt = OozieUtil.getOozieDateTimeFormatter();
        return fmt.print(jodaTime);
    }

    public static void sleepTill(String startTimeOfLateCoord) {

        DateTime finalDate = new DateTime(oozieDateToDate(startTimeOfLateCoord));
        while (true) {
            DateTime sysDate = oozieDateToDate(getTimeWrtSystemTime(0));
            sysDate.withZoneRetainFields(DateTimeZone.UTC);
            LOGGER.info("sysDate: " + sysDate + "  finalDate: " + finalDate);
            if (sysDate.compareTo(finalDate) > 0) {
                break;
            }
            TimeUtil.sleepSeconds(15);
        }
    }

    public static Date getMinutes(String expression, Calendar time) {
        int hr;
        int mins;
        int day;
        int month;
        Calendar cal = Calendar.getInstance();
        cal.setTime(time.getTime());
        if (expression.contains("now")) {
            hr = getInt(expression, 0);
            mins = getInt(expression, 1);
            cal.add(Calendar.HOUR, hr);
            cal.add(Calendar.MINUTE, mins);
        } else if (expression.contains("today")) {
            hr = getInt(expression, 0);
            mins = getInt(expression, 1);
            cal.add(Calendar.HOUR, hr - (cal.get(Calendar.HOUR_OF_DAY)));
            cal.add(Calendar.MINUTE, mins);
        } else if (expression.contains("yesterday")) {
            hr = getInt(expression, 0);
            mins = getInt(expression, 1);
            cal.add(Calendar.HOUR, hr - (cal.get(Calendar.HOUR_OF_DAY)) - 24);
            cal.add(Calendar.MINUTE, mins);
        } else if (expression.contains("currentMonth")) {
            day = getInt(expression, 0);
            hr = getInt(expression, 1);
            mins = getInt(expression, 2);
            cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), 1, 0, 0);
            cal.add(Calendar.HOUR, 24 * day + hr);
            cal.add(Calendar.MINUTE, mins);
        } else if (expression.contains("lastMonth")) {
            day = getInt(expression, 0);
            hr = getInt(expression, 1);
            mins = getInt(expression, 2);
            cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) - 1, 1, 0, 0);
            cal.add(Calendar.HOUR, 24 * day + hr);
            cal.add(Calendar.MINUTE, mins);
        } else if (expression.contains("currentYear")) {
            month = getInt(expression, 0);
            day = getInt(expression, 1);
            hr = getInt(expression, 2);
            mins = getInt(expression, 3);
            cal.set(cal.get(Calendar.YEAR), 1, 1, 0, 0);
            cal.add(Calendar.MONTH, month - 1);
            cal.add(Calendar.HOUR, 24 * day + hr);
            cal.add(Calendar.MINUTE, mins);
        } else if (expression.contains("lastYear")) {
            month = getInt(expression, 0);
            day = getInt(expression, 1);
            hr = getInt(expression, 2);
            mins = getInt(expression, 3);
            cal.set(cal.get(Calendar.YEAR) - 1, 1, 1, 0, 0);
            cal.add(Calendar.MONTH, month - 1);
            cal.add(Calendar.HOUR, 24 * day + hr);
            cal.add(Calendar.MINUTE, mins);
        }
        return cal.getTime();
    }

    private static int getInt(String expression, int position) {
        String numbers = expression.substring(expression.indexOf('(') + 1, expression.indexOf(')'));
        return Integer.parseInt(numbers.split(",")[position]);
    }
}
