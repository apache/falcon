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

import java.util.regex.Pattern;

/**
 * Helper to map feed path and the time component.
 */
public final class FeedDataPath {

    private FeedDataPath() {}

    /**
     * Standard variables for feed time components.
     */
    public static enum VARS {
        YEAR("yyyy", "([0-9]{4})"), MONTH("MM", "(0[1-9]|1[0-2])"), DAY("dd", "(0[1-9]|1[0-9]|2[0-9]|3[0-1])"),
        HOUR("HH", "([0-1][0-9]|2[0-4])"), MINUTE("mm", "([0-5][0-9]|60)");

        private final Pattern pattern;
        private final String datePattern;
        private final String patternRegularExpression;

        private VARS(String datePattern, String patternRegularExpression) {
            pattern = Pattern.compile("\\$\\{" + name() + "\\}");
            this.datePattern = datePattern;
            this.patternRegularExpression = patternRegularExpression;
        }

        public String getPatternRegularExpression() {
            return patternRegularExpression;
        }

        public String getDatePattern() {
            return datePattern;
        }

        public String regex() {
            return pattern.pattern();
        }

        public static VARS from(String str) {
            for (VARS var : VARS.values()) {
                if (var.datePattern.equals(str)) {
                    return var;
                }
            }
            return null;
        }

        public static VARS presentIn(String str) {
            for (VARS var : VARS.values()) {
                if (str.contains(var.datePattern)) {
                    return var;
                }
            }
            return null;
        }
    }

    public static final Pattern PATTERN = Pattern.compile(VARS.YEAR.regex()
            + "|" + VARS.MONTH.regex() + "|" + VARS.DAY.regex() + "|"
            + VARS.HOUR.regex() + "|" + VARS.MINUTE.regex());

    public static final Pattern DATE_FIELD_PATTERN = Pattern
            .compile("yyyy|MM|dd|HH|mm");

}
