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

package org.apache.falcon.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.EntityNotification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.Address;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utils class for notification.
 */
public final class NotificationUtil {
    public static final Logger LOG = LoggerFactory.getLogger(NotificationUtil.class);

    private NotificationUtil() {
    }

    private static final String EMAIL_PATTERN = "^[_A-Za-z0-9-\\+]+(\\.[_A-Za-z0-9-]+)*@"
            + "[A-Za-z0-9-]+(\\.[A-Za-z0-9]+)*$";
    private static final String COMMA = ",";

    public static boolean isEmailAddressValid(EntityNotification entityNotification) throws FalconException {
        Pattern pattern = Pattern.compile(EMAIL_PATTERN);
        if (StringUtils.isEmpty(entityNotification.getTo())) {
            return false;
        }

        for (String address : entityNotification.getTo().split(COMMA)) {
            Matcher matcher = pattern.matcher(address.trim());
            if (!(matcher.matches())) {
                return false;
            }
        }

        return true;
    }

    public static Address[] getToAddress(String toAddress) throws FalconException {
        List<InternetAddress> toAddrs = new ArrayList<InternetAddress>();
        String []tos = toAddress.split(COMMA);
        try {
            for (String toStr : tos) {
                toAddrs.add(new InternetAddress(toStr.trim()));
            }
        } catch (AddressException e) {
            throw new FalconException("Exception in to address:"+e);
        }

        return toAddrs.toArray(new InternetAddress[0]);
    }
}
