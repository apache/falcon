package org.apache.ivory.entity.v0;

import java.util.TimeZone;

public class SchemaHelper {
    public static String getTimeZoneId(TimeZone tz) {
        return tz.getID();
    }
}
