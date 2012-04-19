package org.apache.ivory.entity.common;

import java.util.Calendar;

public enum TimeUnit {
    MINUTE(Calendar.MINUTE), HOUR(Calendar.HOUR), DAY(Calendar.DATE), MONTH(Calendar.MONTH), END_OF_DAY(Calendar.DATE), END_OF_MONTH(
        Calendar.MONTH), NONE(-1);

    private int calendarUnit;

    private TimeUnit(int calendarUnit) {
        this.calendarUnit = calendarUnit;
    }

    public int getCalendarUnit() {
        return calendarUnit;
    }
}
