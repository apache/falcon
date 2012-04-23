package org.apache.ivory.entity.parser;

import org.apache.ivory.entity.common.TimeUnit;

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
