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

package org.apache.ivory.entity.parser;

import org.apache.oozie.coord.TimeUnit;

public enum Frequency {

    minutes(TimeUnit.MINUTE, TimeUnit.NONE), hours(TimeUnit.HOUR, TimeUnit.NONE), days(TimeUnit.DAY, TimeUnit.NONE), months(
            TimeUnit.MONTH, TimeUnit.NONE), endOfDays(TimeUnit.DAY, TimeUnit.END_OF_DAY), endOfMonths(TimeUnit.MONTH,
            TimeUnit.END_OF_MONTH);

    private TimeUnit timeUnit;
    private TimeUnit endOfDuration;

    private Frequency(TimeUnit timeUnit, TimeUnit endOfDuration) {
        this.timeUnit = timeUnit;
        this.endOfDuration = endOfDuration;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public TimeUnit getEndOfDuration() {
        return endOfDuration;
    }

}
