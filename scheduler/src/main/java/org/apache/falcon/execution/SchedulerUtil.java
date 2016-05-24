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

import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.util.RuntimeProperties;
import org.joda.time.DateTime;

/**
 * Contains utility methods.
 */
public final class SchedulerUtil {

    private static final long MINUTE_IN_MS = 60 * 1000L;
    private static final long HOUR_IN_MS = 60 * MINUTE_IN_MS;
    public static final String MINUTELY_PROCESS_FREQUENCY_POLLING_IN_MILLIS =
            "falcon.scheduler.minutely.process.polling.frequency.millis";
    public static final String HOURLY_PROCESS_FREQUENCY_POLLING_IN_MILLIS =
            "falcon.scheduler.hourly.process.polling.frequency.millis";
    public static final String DAILY_PROCESS_FREQUENCY_POLLING_IN_MILLIS =
            "falcon.scheduler.daily.process.polling.frequency.millis";
    public static final String MONTHLY_PROCESS_FREQUENCY_POLLING_IN_MILLIS =
            "falcon.scheduler.monthly.process.polling.frequency.millis";

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

}
