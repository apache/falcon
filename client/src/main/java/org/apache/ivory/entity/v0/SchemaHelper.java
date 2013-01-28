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
