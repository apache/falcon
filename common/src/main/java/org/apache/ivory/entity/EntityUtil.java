package org.apache.ivory.entity;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.common.DateValidator;

public class EntityUtil {
	
	private static final DateValidator DateValidator = new DateValidator();
    private static DateFormat getDateFormat() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateFormat;
    }
    
    public static TimeZone getTimeZone(String tzId) {
        if (tzId == null) {
            throw new IllegalArgumentException("Invalid TimeZone: " + tzId);
        }
        TimeZone tz = TimeZone.getTimeZone(tzId);
        if (!tzId.equals("GMT") && tz.getID().equals("GMT")) {
            throw new IllegalArgumentException("Invalid TimeZone: " + tzId);
        }
        return tz;
    }

    public static Date parseDateUTC(String s) throws IvoryException {
        try {
            return getDateFormat().parse(s);
        } catch (ParseException e) {
            throw new IvoryException(e);
        }
    }

    public static String formatDateUTC(Date d) {
        return (d != null) ? getDateFormat().format(d) : null;
    }
    
    public static boolean isValidUTCData(String date) {
        return DateValidator.validate(date);
    }    
}
