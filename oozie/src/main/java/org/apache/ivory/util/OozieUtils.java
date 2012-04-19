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

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.parser.Frequency;
import org.apache.oozie.coord.TimeUnit;

public final class OozieUtils {

    @Deprecated
	public static Date getNextStartTimeOld(Date startTime, Frequency frequency,
			int periodicity, String timzone, Date now) {

		Calendar startCal = Calendar.getInstance(EntityUtil
				.getTimeZone(timzone));
		startCal.setTime(startTime);

		while (startCal.getTime().before(now)) {
			startCal.add(frequency.getTimeUnit().getCalendarUnit(), periodicity);
		}
		return startCal.getTime();
	}

    public static Date getNextStartTime(Date startTime, Frequency frequency,
            int periodicity, String timzone, Date now) {

        if (startTime.after(now)) return startTime;

        Calendar startCal = Calendar.getInstance(EntityUtil
                .getTimeZone(timzone));
        startCal.setTime(startTime);

        int count = 0;
        switch (frequency.getTimeUnit()) {
            case MONTH:
                count = (int)((now.getTime() - startTime.getTime()) / 2592000000L);
                break;
            case DAY:
                count = (int)((now.getTime() - startTime.getTime()) / 86400000L);
                break;
            case HOUR:
                count = (int)((now.getTime() - startTime.getTime()) / 3600000L);
                break;
            case MINUTE:
                count = (int)((now.getTime() - startTime.getTime()) / 60000L);
                break;
            case END_OF_MONTH:
            case END_OF_DAY:
            case NONE:
            default:
        }

        if (count > 2) {
            startCal.add(frequency.getTimeUnit().getCalendarUnit(),
                    ((count - 2) / periodicity) * periodicity);
        }
        while (startCal.getTime().before(now)) {
            startCal.add(frequency.getTimeUnit().getCalendarUnit(), periodicity);
        }
        return startCal.getTime();
    }
}
