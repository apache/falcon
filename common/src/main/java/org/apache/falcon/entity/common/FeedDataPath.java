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
package org.apache.falcon.entity.common;

import java.util.Calendar;
import java.util.regex.Pattern;

/**
 * Helper to map feed path and the time component.
 */
public final class FeedDataPath {

    private FeedDataPath() {}

    /**
     * Standard variables for feed time components.
     */
    public enum VARS {
        YEAR("([0-9]{4})", Calendar.YEAR, 4),
        MONTH("(0[1-9]|1[0-2])", Calendar.MONTH, 2),
        DAY("(0[1-9]|1[0-9]|2[0-9]|3[0-1])", Calendar.DAY_OF_MONTH, 2),
        HOUR("([0-1][0-9]|2[0-4])", Calendar.HOUR_OF_DAY, 2),
        MINUTE("([0-5][0-9]|60)", Calendar.MINUTE, 2);

        private final Pattern pattern;
        private final String valuePattern;
        private final int calendarField;
        private final int valueSize;

        private VARS(String patternRegularExpression, int calField, int numDigits) {
            pattern = Pattern.compile("\\$\\{" + name() + "\\}");
            this.valuePattern = patternRegularExpression;
            this.calendarField = calField;
            this.valueSize = numDigits;
        }

        public String getValuePattern() {
            return valuePattern;
        }

        public String regex() {
            return pattern.pattern();
        }

        public int getCalendarField() {
            return calendarField;
        }

        public int getValueSize() {
            return valueSize;
        }

        public void setCalendar(Calendar cal, int value) {
            if (this == MONTH) {
                cal.set(calendarField, value - 1);
            } else {
                cal.set(calendarField, value);
            }
        }

        public static VARS from(String str) {
            for (VARS var : VARS.values()) {
                if (var.pattern.matcher(str).matches()) {
                    return var;
                }
            }
            return null;
        }
    }

    public static final Pattern PATTERN = Pattern.compile(VARS.YEAR.regex()
            + "|" + VARS.MONTH.regex() + "|" + VARS.DAY.regex() + "|"
            + VARS.HOUR.regex() + "|" + VARS.MINUTE.regex());
}
