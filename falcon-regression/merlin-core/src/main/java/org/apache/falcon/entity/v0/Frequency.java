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

package org.apache.falcon.entity.v0;

import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Frequency as supported in the xsd definitions.
 */
public class Frequency {
    private static final Pattern PATTERN = Pattern.compile("(minutes|hours|days|months)\\((\\d+)\\)");

    /**
     * TimeUnit corresponding to the frequency.
     */
    public static enum TimeUnit {
        minutes(Calendar.MINUTE), hours(Calendar.HOUR), days(Calendar.DATE), months(Calendar.MONTH);

        private int calendarUnit;

        private TimeUnit(int calendarUnit) {
            this.calendarUnit = calendarUnit;
        }

        public int getCalendarUnit() {
            return calendarUnit;
        }
    }

    private TimeUnit timeUnit;
    private String frequency;

    public Frequency(String freq, TimeUnit timeUnit) {
        this.frequency = freq;
        this.timeUnit = timeUnit;
    }

    public Frequency(String strValue) {
        Matcher matcher = PATTERN.matcher(strValue);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid frequency: " + strValue);
        }

        timeUnit = TimeUnit.valueOf(matcher.group(1));
        frequency = matcher.group(2);
    }

    public static Frequency fromString(String strValue) {
        return new Frequency(strValue);
    }

    public static String toString(Frequency freq) {
        return freq==null? null:freq.toString();
    }

    @Override
    public String toString() {
        return timeUnit.name() + "(" + frequency + ")";
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public String getFrequency() {
        return frequency;
    }

    public int getFrequencyAsInt() {
        return Integer.valueOf(frequency);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof Frequency)) {
            return false;
        }

        Frequency freq = (Frequency) obj;
        return this == freq || this.getFrequency().equals(freq.getFrequency())
                && this.getTimeUnit() == freq.getTimeUnit();

    }

    @Override
    public int hashCode() {
        int result = timeUnit.hashCode();
        result = 31 * result + frequency.hashCode();
        return result;
    }
}
