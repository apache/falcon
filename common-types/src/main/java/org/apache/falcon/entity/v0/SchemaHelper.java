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

package org.apache.falcon.entity.v0;

import javax.xml.stream.XMLInputFactory;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Support function to parse and format date in xsd string.
 */
public final class SchemaHelper {

    public static final String ISO8601_FORMAT = "yyyy-MM-dd'T'HH:mm'Z'";

    private SchemaHelper() {}

    public static String getTimeZoneId(TimeZone tz) {
        return tz.getID();
    }

    public static DateFormat getDateFormat() {
        DateFormat dateFormat = new SimpleDateFormat(ISO8601_FORMAT);
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateFormat;
    }

    public static String formatDateUTC(Date date) {
        return (date != null) ? getDateFormat().format(date) : null;
    }

    public static Date parseDateUTC(String dateStr) {
        if (!DateValidator.validate(dateStr)) {
            throw new IllegalArgumentException(dateStr + " is not a valid UTC string");
        }
        try {
            return getDateFormat().parse(dateStr);
        } catch (ParseException e) {
            throw new RuntimeException("Unable to parse date: " + dateStr, e);
        }
    }

    public static String formatDateUTCToISO8601(final String dateString, final String dateStringFormat) {

        try {
            DateFormat dateFormat = new SimpleDateFormat(dateStringFormat.substring(0, dateString.length()));
            dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            return SchemaHelper.formatDateUTC(dateFormat.parse(dateString));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Return the xml input factory that has the properties set for secure handling of data.
     * @return xif
     */
    public static XMLInputFactory createXmlInputFactory() {
        XMLInputFactory xif = XMLInputFactory.newFactory();
        xif.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        xif.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        return xif;
    }
}
