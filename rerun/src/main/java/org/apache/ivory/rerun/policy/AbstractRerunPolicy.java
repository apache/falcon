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
package org.apache.ivory.rerun.policy;

import java.util.Calendar;
import java.util.Date;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.v0.Frequency;

public abstract class AbstractRerunPolicy {

    private static final long MINUTES = 60 * 1000L;
    private static final long HOURS = 60 * MINUTES;
    private static final long DAYS = 24 * HOURS;
    private static final long MONTHS = 31 * DAYS;

    public long getDurationInMilliSec(Frequency delay) throws IvoryException {
        switch (delay.getTimeUnit()) {
            case minutes:
                return MINUTES * delay.getFrequency();

            case hours:
                return HOURS * delay.getFrequency();

            case days:
                return DAYS * delay.getFrequency();

            case months:
                return MONTHS * delay.getFrequency();
        }
        throw new IvoryException("Unknown delayUnit:" + delay.getTimeUnit());
    }

    public static Date addTime(Date date, int milliSecondsToAdd) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.MILLISECOND, milliSecondsToAdd);
        return cal.getTime();
    }

    public abstract long getDelay(Frequency delay, int eventNumber) throws IvoryException;

    public abstract long getDelay(Frequency delay, Date nominaltime, Date cutOffTime) throws IvoryException;
}
