package org.apache.ivory.entity.v0;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class SchemaHelper {
    public static String getTimeZoneId(TimeZone tz) {
        return tz.getID();
    }

    private static DateFormat getDateFormat() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateFormat;
    }

    public static String formatDateUTC(Date date) {
        return (date != null) ? getDateFormat().format(date) : null;
    }

    public static Date parseDateUTC(String dateStr) {
        if(!DateValidator.validate(dateStr))
            throw new IllegalArgumentException(dateStr + " is not a valid UTC string");
        try {
            return getDateFormat().parse(dateStr);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
