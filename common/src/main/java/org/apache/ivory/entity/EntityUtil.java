package org.apache.ivory.entity;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.common.DateValidator;
import org.apache.ivory.entity.parser.Frequency;

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
    
    public static boolean isValidUTCDate(String date){
    		return DateValidator.validate(date);
    }
    
    @Deprecated
    public static Date getNextStartTimeOld(Date startTime, Frequency frequency,
            int periodicity, String timzone, Date now) {
        Calendar startCal = Calendar.getInstance(EntityUtil
                .getTimeZone(timzone));
        startCal.setTime(startTime);

        while (startCal.getTime().before(now)) {
            startCal.add(frequency.getTimeUnit().getCalendarUnit(), periodicity);
        }
        return startCal.getTime();
    }
    
    public static Date getNextStartTime(Date startTime, Frequency frequency,
            int periodicity, String timzone, Date now) {

        if (startTime.after(now)) return startTime;

        Calendar startCal = Calendar.getInstance(EntityUtil
                .getTimeZone(timzone));
        startCal.setTime(startTime);

        int count = 0;
        switch (frequency.getTimeUnit()) {
            case MONTH:
                count = (int)((now.getTime() - startTime.getTime()) / 2592000000L);
                break;
            case DAY:
                count = (int)((now.getTime() - startTime.getTime()) / 86400000L);
                break;
            case HOUR:
                count = (int)((now.getTime() - startTime.getTime()) / 3600000L);
                break;
            case MINUTE:
                count = (int)((now.getTime() - startTime.getTime()) / 60000L);
                break;
            case END_OF_MONTH:
            case END_OF_DAY:
            case NONE:
            default:
        }

        if (count > 2) {
            startCal.add(frequency.getTimeUnit().getCalendarUnit(),
                    ((count - 2) / periodicity) * periodicity);
        }
        while (startCal.getTime().before(now)) {
            startCal.add(frequency.getTimeUnit().getCalendarUnit(), periodicity);
        }
        return startCal.getTime();
    }
}
