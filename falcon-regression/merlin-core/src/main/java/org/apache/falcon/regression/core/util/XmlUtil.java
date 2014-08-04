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

package org.apache.falcon.regression.core.util;

import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.ActionType;
import org.apache.falcon.entity.v0.feed.Retention;
import org.apache.falcon.entity.v0.feed.Validity;
import org.custommonkey.xmlunit.Diff;
import org.custommonkey.xmlunit.XMLUnit;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import java.io.IOException;

/**
 * Util methods for XML.
 */
public final class XmlUtil {

    private XmlUtil() {
        throw new AssertionError("Instantiating utility class...");
    }
    private static final Logger LOGGER = Logger.getLogger(XmlUtil.class);

    public static Validity createValidity(String start, String end) {
        Validity v = new Validity();
        v.setStart(TimeUtil.oozieDateToDate(start).toDate());
        v.setEnd(TimeUtil.oozieDateToDate(end).toDate());
        return v;
    }

    public static Retention createRtention(String limit, ActionType action) {
        Retention r = new Retention();
        r.setLimit(new Frequency(limit));
        r.setAction(action);
        return r;
    }

    public static org.apache.falcon.entity.v0.process.Validity
    createProcessValidity(
        String startTime, String endTime) {
        org.apache.falcon.entity.v0.process.Validity v =
            new org.apache.falcon.entity.v0.process.Validity();
        LOGGER.info("instanceUtil.oozieDateToDate(endTime).toDate(): "
            + TimeUtil.oozieDateToDate(endTime).toDate());
        v.setEnd(TimeUtil.oozieDateToDate(endTime).toDate());
        v.setStart(TimeUtil.oozieDateToDate(startTime).toDate());
        return v;
    }

    public static boolean isIdentical(String expected, String actual)
        throws IOException, SAXException {
        XMLUnit.setIgnoreWhitespace(true);
        XMLUnit.setIgnoreAttributeOrder(true);
        Diff diff = XMLUnit.compareXML(expected, actual);
        LOGGER.info(diff);
        return diff.identical();
    }
}
