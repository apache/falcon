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
package org.apache.falcon.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.Frequency;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper to get date operations.
 */
public final class DateUtil {

    private static final long MINUTE_IN_MS = 60 * 1000L;
    private static final long HOUR_IN_MS = 60 * MINUTE_IN_MS;
    private static final long DAY_IN_MS = 24 * HOUR_IN_MS;
    private static final long MONTH_IN_MS = 31 * DAY_IN_MS;

    //Friday, April 16, 9999 7:12:55 AM UTC corresponding date
    public static final Date NEVER = new Date(Long.parseLong("253379862775000"));

    public static final long HOUR_IN_MILLIS = 60 * 60 * 1000;

    private static final Pattern GMT_OFFSET_COLON_PATTERN = Pattern.compile("^GMT(\\-|\\+)(\\d{2})(\\d{2})$");

    public static final TimeZone UTC = getTimeZone("UTC");

    public static final String ISO8601_UTC_MASK = "yyyy-MM-dd'T'HH:mm'Z'";
    private static String activeTimeMask = ISO8601_UTC_MASK;
    private static TimeZone activeTimeZone = UTC;

    private static final Pattern VALID_TIMEZONE_PATTERN = Pattern.compile("^UTC$|^GMT(\\+|\\-)\\d{4}$");

    private static final String ISO8601_TZ_MASK_WITHOUT_OFFSET = "yyyy-MM-dd'T'HH:mm";
    private static boolean entityInUTC = true;

    private DateUtil() {}

    /**
     * Configures the Datetime parsing with  process timezone.
     *
     */
    public static void setTimeZone(String tz) throws FalconException {
        if (StringUtils.isBlank(tz)) {
            tz = "UTC";
        }
        if (!VALID_TIMEZONE_PATTERN.matcher(tz).matches()) {
            throw new FalconException("Invalid entity timezone, it must be 'UTC' or 'GMT(+/-)####");
        }
        activeTimeZone = TimeZone.getTimeZone(tz);
        entityInUTC = activeTimeZone.equals(UTC);
        activeTimeMask = (entityInUTC) ? ISO8601_UTC_MASK : ISO8601_TZ_MASK_WITHOUT_OFFSET + tz.substring(3);
    }

    public static Date getNextMinute(Date time) {
        Calendar insCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        insCal.setTime(time);
        insCal.add(Calendar.MINUTE, 1);
        return insCal.getTime();

    }

    public static String getDateFormatFromTime(long milliSeconds) {
        return SchemaHelper.getDateFormat().format((new Date(milliSeconds)));
    }

    /**
     * This function should not be used for scheduling related functions as it may cause correctness issues in those
     * scenarios.
     * @param frequency
     * @return
     */
    public static Long getFrequencyInMillis(Frequency frequency){
        switch (frequency.getTimeUnit()) {

        case months:
            return MONTH_IN_MS * frequency.getFrequencyAsInt();

        case days:
            return DAY_IN_MS * frequency.getFrequencyAsInt();

        case hours:
            return HOUR_IN_MS * frequency.getFrequencyAsInt();

        case minutes:
            return MINUTE_IN_MS * frequency.getFrequencyAsInt();

        default:
            return null;
        }
    }

    /**
     * Returns the current time, with seconds and milliseconds reset to 0.
     * @return
     */
    public static Date now() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    /**
     * Adds the supplied number of seconds to the given date and returns the new Date.
     * @param date
     * @param seconds
     * @return
     */
    public static Date offsetTime(Date date, int seconds) {
        return new Date(1000L * seconds + date.getTime());
    }

    /**
     * Parses a datetime in ISO8601 format in the  process timezone.
     *
     * @param s string with the datetime to parse.
     * @return the corresponding {@link java.util.Date} instance for the parsed date.
     * @throws java.text.ParseException thrown if the given string was
     * not an ISO8601 value for the  process timezone.
     */
    public static Date parseDateFalconTZ(String s) throws ParseException {
        s = s.trim();
        ParsePosition pos = new ParsePosition(0);
        Date d = getISO8601DateFormat(activeTimeZone, activeTimeMask).parse(s, pos);
        if (d == null) {
            throw new ParseException("Could not parse [" + s + "] using [" + activeTimeMask + "] mask",
                    pos.getErrorIndex());
        }
        if (s.length() > pos.getIndex()) {
            throw new ParseException("Correct datetime string is followed by invalid characters: " + s, pos.getIndex());
        }
        return d;
    }

    private static DateFormat getISO8601DateFormat(TimeZone tz, String mask) {
        DateFormat dateFormat = new SimpleDateFormat(mask);
        // Stricter parsing to prevent dates such as 2011-12-50T01:00Z (December 50th) from matching
        dateFormat.setLenient(false);
        dateFormat.setTimeZone(tz);
        return dateFormat;
    }

    private static DateFormat getSpecificDateFormat(String format) {
        DateFormat dateFormat = new SimpleDateFormat(format);
        dateFormat.setTimeZone(activeTimeZone);
        return dateFormat;
    }

    /**
     * Formats a {@link java.util.Date} as a string using the specified format mask.
     * <p/>
     * The format mask must be a {@link java.text.SimpleDateFormat} valid format mask.
     *
     * @param d {@link java.util.Date} to format.
     * @return the string for the given date using the specified format mask,
     * <code>NULL</code> if the {@link java.util.Date} instance was <code>NULL</code>
     */
    public static String formatDateCustom(Date d, String format) {
        return (d != null) ? getSpecificDateFormat(format).format(d) : "NULL";
    }

    /**
     * Formats a {@link java.util.Date} as a string in ISO8601 format using process timezone.
     *
     * @param d {@link java.util.Date} to format.
     * @return the ISO8601 string for the given date, <code>NULL</code> if the {@link java.util.Date} instance was
     * <code>NULL</code>
     */
    public static String formatDateFalconTZ(Date d) {
        return (d != null) ? getISO8601DateFormat(activeTimeZone, activeTimeMask).format(d) : "NULL";
    }

    /**
     * Returns the {@link java.util.TimeZone} for the given timezone ID.
     *
     * @param tzId timezone ID.
     * @return  the {@link java.util.TimeZone} for the given timezone ID.
     */
    public static TimeZone getTimeZone(String tzId) {
        if (tzId == null) {
            throw new IllegalArgumentException("Timezone cannot be null");
        }
        tzId = handleGMTOffsetTZNames(tzId);    // account for GMT-####
        TimeZone tz = TimeZone.getTimeZone(tzId);
        // If these are not equal, it means that the tzId is not valid (invalid tzId's return GMT)
        if (!tz.getID().equals(tzId)) {
            throw new IllegalArgumentException("Invalid TimeZone: " + tzId);
        }
        return tz;
    }

    /**
     * {@link java.util.TimeZone#getTimeZone(String)} takes the timezone ID as an argument; for invalid IDs
     * it returns the <code>GMT</code> TimeZone.  A timezone ID formatted like <code>GMT-####</code> is not a valid ID,
     * however, it will actually map this to the <code>GMT-##:##</code> TimeZone, instead of returning the
     * <code>GMT</code> TimeZone.  We check (later) check that a timezone ID is valid by calling
     * {@link java.util.TimeZone#getTimeZone(String)} and seeing if the returned
     * TimeZone ID is equal to the original; because we want to allow <code>GMT-####</code>, while still
     * disallowing actual invalid IDs, we have to manually replace <code>GMT-####</code>
     * with <code>GMT-##:##</code> first.
     *
     * @param tzId The timezone ID
     * @return If tzId matches <code>GMT-####</code>, then we return <code>GMT-##:##</code>; otherwise,
     * we return tzId unaltered
     */
    private static String handleGMTOffsetTZNames(String tzId) {
        Matcher m = GMT_OFFSET_COLON_PATTERN.matcher(tzId);
        if (m.matches() && m.groupCount() == 3) {
            tzId = "GMT" + m.group(1) + m.group(2) + ":" + m.group(3);
        }
        return tzId;
    }

    /**
     * Create a Calendar instance for UTC time zone using the specified date.
     * @param dateString
     * @return appropriate Calendar object
     * @throws Exception
     */
    public static Calendar getCalendar(String dateString) throws Exception {
        return getCalendar(dateString, activeTimeZone);
    }

    /**
     * Create a Calendar instance using the specified date and Time zone.
     * @param dateString
     * @param tz : TimeZone
     * @return appropriate Calendar object
     * @throws Exception
     */
    public static Calendar getCalendar(String dateString, TimeZone tz) throws Exception {
        Date date = DateUtil.parseDateFalconTZ(dateString);
        Calendar calDate = Calendar.getInstance();
        calDate.setTime(date);
        calDate.setTimeZone(tz);
        return calDate;
    }

    /**
     * Formats a {@link java.util.Calendar} as a string in ISO8601 format process timezone.
     *
     * @param c {@link java.util.Calendar} to format.
     * @return the ISO8601 string for the given date, <code>NULL</code> if the {@link java.util.Calendar} instance was
     * <code>NULL</code>
     */
    public static String formatDateFalconTZ(Calendar c) {
        return (c != null) ? formatDateFalconTZ(c.getTime()) : "NULL";
    }

}
