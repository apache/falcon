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

package org.apache.falcon.regression.core.enumsAndConstants;

import org.joda.time.DateTime;

/** Enum to represent different Retention Units. */
public enum RetentionUnit {
    MINUTES("minutes") {
        @Override
        public DateTime minusTime(DateTime dateTime, int amount) {
            return dateTime.minusMinutes(amount);
        }
    }, HOURS("hours") {
        @Override
        public DateTime minusTime(DateTime dateTime, int amount) {
            return dateTime.minusHours(amount);
        }
    }, DAYS("days") {
        @Override
        public DateTime minusTime(DateTime dateTime, int amount) {
            return dateTime.minusDays(amount);
        }
    }, MONTHS("months") {
        @Override
        public DateTime minusTime(DateTime dateTime, int amount) {
            return dateTime.minusMonths(amount);
        }
    }, YEARS("years") {
        @Override
        public DateTime minusTime(DateTime dateTime, int amount) {
            return dateTime.minusYears(amount);
        }
    };

    private String value;

    private RetentionUnit(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public abstract DateTime minusTime(DateTime dateTime, int amount);

}
