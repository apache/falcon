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

import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.Frequency;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

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

    private DateUtil() {}

    public static Date getNextMinute(Date time) throws Exception {
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
}
