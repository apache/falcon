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

import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.util.RuntimeProperties;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Contains utility methods.
 */
public final class SchedulerUtil {

    private static final long MINUTE_IN_MS = 60 * 1000L;
    private static final long HOUR_IN_MS = 60 * MINUTE_IN_MS;
    private static final long DAY_IN_MS = 24 * HOUR_IN_MS;
    private static final long MONTH_IN_MS = 30 * HOUR_IN_MS;
    public static final String MINUTELY_PROCESS_FREQUENCY_POLLING_IN_MILLIS =
            "falcon.scheduler.minutely.process.polling.frequency.millis";
    public static final String HOURLY_PROCESS_FREQUENCY_POLLING_IN_MILLIS =
            "falcon.scheduler.hourly.process.polling.frequency.millis";
    public static final String DAILY_PROCESS_FREQUENCY_POLLING_IN_MILLIS =
            "falcon.scheduler.daily.process.polling.frequency.millis";
    public static final String MONTHLY_PROCESS_FREQUENCY_POLLING_IN_MILLIS =
            "falcon.scheduler.monthly.process.polling.frequency.millis";

    private static final Pattern LATEST_PATTERN = Pattern.compile("latest\\(\\s*(-*\\d+)\\)");
    private static final Pattern FUTURE_PATTERN = Pattern.compile("future\\(\\s*(\\d+),\\s*(\\d+)\\)");


    /**
     * Type of EL Expressions.
     */
    public enum EXPTYPE {
        ABSOLUTE,
        LATEST,
        FUTURE
    }

    private SchedulerUtil(){};

    /**
     * Returns the frequency in millis from the given time.
     * Needs to take the calender into account.
     * @param referenceTime
     * @param frequency
     * @return
     */
    public static long getFrequencyInMillis(DateTime referenceTime, Frequency frequency) {
        switch (frequency.getTimeUnit()) {
        case minutes:
            return MINUTE_IN_MS * frequency.getFrequencyAsInt();
        case hours:
            return HOUR_IN_MS * frequency.getFrequencyAsInt();
        case days:
            return referenceTime.plusDays(frequency.getFrequencyAsInt()).getMillis() - referenceTime.getMillis();
        case months:
            return referenceTime.plusMonths(frequency.getFrequencyAsInt()).getMillis() - referenceTime.getMillis();
        default:
            throw new IllegalArgumentException("Invalid time unit " + frequency.getTimeUnit().name());
        }
    }

    public static long getFrequencyInMillis(Frequency frequency) {
        switch (frequency.getTimeUnit()) {
        case minutes:
            return MINUTE_IN_MS * frequency.getFrequencyAsInt();
        case hours:
            return HOUR_IN_MS * frequency.getFrequencyAsInt();
        case days:
            return DAY_IN_MS * frequency.getFrequencyAsInt();
        case months:
            return MONTH_IN_MS * frequency.getFrequencyAsInt();
        default:
            throw new IllegalArgumentException("Invalid time unit " + frequency.getTimeUnit().name());
        }
    }

    /**
     *
     * @param start
     * @param frequency
     * @param timezone
     * @param referenceTime
     * @return
     */
    public static long getEndTimeInMillis(Date start, Frequency frequency, TimeZone timezone,
                                          Date referenceTime, EXPTYPE exptype, int limit) {
        Date startTime = new Date(getStartTimeInMillis(start, frequency, timezone, referenceTime, exptype));
        if (exptype == EXPTYPE.LATEST) {
            return start.getTime();
        } else if (exptype == EXPTYPE.FUTURE) {
            return EntityUtil.getNextInstanceTime(startTime, frequency, timezone, limit).getTime();
        } else {
            throw new IllegalArgumentException("End time cannot be obtained for exp " + exptype);
        }
    }

    /**
     *
     * @param start
     * @param frequency
     * @param timezone
     * @param referenceTime
     * @param exptype
     * @return
     */
    public static long getStartTimeInMillis(Date start, Frequency frequency, TimeZone timezone,
                                             Date referenceTime, EXPTYPE exptype) {
        if (exptype == EXPTYPE.LATEST) {
            return EntityUtil.getPreviousInstanceTime(start, frequency, timezone, referenceTime).getTime();
        } else if (exptype == EXPTYPE.FUTURE) {
            return EntityUtil.getNextStartTime(start, frequency, timezone, referenceTime).getTime();
        } else {
            throw new IllegalArgumentException("Start time cannot be obtained for exp " + exptype);
        }
    }


    /**
     *
     * @param frequency
     * @return
     */
    public static long getPollingFrequencyinMillis(Frequency frequency) {
        switch (frequency.getTimeUnit()) {
        case minutes:
            return Long.parseLong(RuntimeProperties.get().getProperty(MINUTELY_PROCESS_FREQUENCY_POLLING_IN_MILLIS,
                    "20000"));
        case hours:
            return Long.parseLong(RuntimeProperties.get().getProperty(HOURLY_PROCESS_FREQUENCY_POLLING_IN_MILLIS,
                    "60000"));
        case days:
            return Long.parseLong(RuntimeProperties.get().getProperty(DAILY_PROCESS_FREQUENCY_POLLING_IN_MILLIS,
                    "120000"));
        case months:
            return Long.parseLong(RuntimeProperties.get().getProperty(MONTHLY_PROCESS_FREQUENCY_POLLING_IN_MILLIS,
                    "180000"));
        default:
            throw new IllegalArgumentException("Unhandled frequency time unit " + frequency.getTimeUnit());
        }
    }

    /**
     * Retrives expression type from given feed input expression.
     * @param expression
     * @return
     */
    public static EXPTYPE getExpType(String expression) {
        if (expression.contains("latest")) {
            return EXPTYPE.LATEST;
        } else if (expression.contains("future")) {
            return EXPTYPE.FUTURE;
        } else {
            return EXPTYPE.ABSOLUTE;
        }
    }

    /**
     * Retrieves index inside Latest EL Expression.
     * @param expression
     * @return
     */
    public static int getLatestInstance(String expression) {
        Matcher matcher = LATEST_PATTERN.matcher(expression);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        throw new IllegalArgumentException("Expression not valid " + expression);
    }

    /**
     * Retrieves index inside Input Expression based on expression type.
     * @param expression
     * @param exptype
     * @return
     */
    public static int getExpInstance(String expression, EXPTYPE exptype) {
        if (exptype == EXPTYPE.LATEST) {
            return getLatestInstance(expression);
        } else if (exptype == EXPTYPE.FUTURE) {
            return getFutureExpInstance(expression);
        } else {
            throw new IllegalArgumentException("Expression not valid " + expression);
        }
    }

    /**
     * Retrieves index inside Future EL Expression.
     * @param expression
     * @return
     */
    public static int getFutureExpInstance(String expression) {
        Matcher matcher = FUTURE_PATTERN.matcher(expression);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        throw new IllegalArgumentException("Expression not valid " + expression);
    }

    public static int getExpLimit(String expression, EXPTYPE exptype) {
        if (exptype == EXPTYPE.FUTURE) {
            Matcher matcher = FUTURE_PATTERN.matcher(expression);
            if (matcher.find()) {
                return Integer.parseInt(matcher.group(2));
            }
            throw new IllegalArgumentException("Expression not valid " + expression);
        }
        return 0;
    }

    /**
     * Validates Start and End Expressions for given input feed.
     * @param startTimeExp
     * @param endTimeExp
     * @param inputName
     */
    public static void validateELExpType(String startTimeExp, String endTimeExp, String inputName) {
        EXPTYPE startExpType = getExpType(startTimeExp);
        EXPTYPE endExpType = getExpType(endTimeExp);
        if (startExpType != endExpType) {
            throw new IllegalArgumentException("Start exp and End exp for given input: " + inputName
                    + " should be of same type. Type can be of:  "
                    + Arrays.toString(EXPTYPE.values()));
        }
    }

    /**
     * Validates Start and End Times for given input feed.
     * @param startTime
     * @param endTime
     */
    public static void validateStartAndEndTime(Date startTime, Date endTime) {
        if (startTime.after(endTime)) {
            throw new IllegalArgumentException("Specified End time "
                    + SchemaHelper.getDateFormat().format(endTime)
                    + " is before the start time of given input"
                    + SchemaHelper.getDateFormat().format(startTime));
        }
    }

    /**
     * Validates Start and End expressions for latest and future expressions.
     * @param startTimeExp
     * @param endTimeExp
     * @param inputName
     * @param processName
     */
    public static void validateStartEndForNonAbsExp(String startTimeExp, String endTimeExp, String inputName,
                                                    String processName) {
        EXPTYPE exptype = getExpType(startTimeExp); // we can take any exptype since both exp's should have same type
        int startExpInstance = getExpInstance(startTimeExp, exptype);
        int endExpInstance = getExpInstance(endTimeExp, exptype);
        if (exptype == EXPTYPE.LATEST) {
            if ((startExpInstance > 0) || (endExpInstance > 0) || (startExpInstance > endExpInstance)) {
                throw new IllegalArgumentException(" Start el expression  and End el expression should not be positive"
                        + " and  start should be before end for expType: " + exptype.name() + "for input: " + inputName
                        + "of process: " + processName);
            }
        } else if (exptype == EXPTYPE.FUTURE) {
            if ((startExpInstance < 0) ||  (endExpInstance < 0) || (getExpLimit(startTimeExp, exptype) <=0)
                    || (startExpInstance > endExpInstance)) {
                throw new IllegalArgumentException(" Start el expression  and End el expression should not be negative"
                        + " and  start should be before end for expType: " + exptype.name() + "for input: " + inputName
                        + "of process: " + processName);
            }
        }
    }

}
