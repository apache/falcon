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

import javax.servlet.http.HttpServletRequest;

/**
 * Utility functions for dealing with servlets.
 */
public final class Servlets {

    public static final String REQUEST_ID = "requestId";

    private Servlets() {
        /* singleton */
    }

    /**
     * Returns the user of the given request.
     *
     * @param httpRequest    an HTTP servlet request
     * @return the user
     */
    public static String getUserFromRequest(HttpServletRequest httpRequest) {
        String user = httpRequest.getParameter("user.name"); // available in query-param
        if (!StringUtils.isEmpty(user)) {
            return user;
        }

        user = httpRequest.getRemoteUser();
        if (!StringUtils.isEmpty(user)) {
            return user;
        }

        user = httpRequest.getHeader("Remote-User"); // backwards-compatibility
        if (!StringUtils.isEmpty(user)) {
            return user;
        }

        return null;
    }

    /**
     * Returns the URI of the given request.
     *
     * @param httpRequest    an HTTP servlet request
     * @return the URI, including the query string
     */
    public static String getRequestURI(HttpServletRequest httpRequest) {
        final StringBuilder url = new StringBuilder(100).append(httpRequest.getRequestURI());
        if (httpRequest.getQueryString() != null) {
            url.append('?').append(httpRequest.getQueryString());
        }

        return url.toString();
    }

    /**
     * Returns the full URL of the given request.
     *
     * @param httpRequest    an HTTP servlet request
     * @return the full URL, including the query string
     */
    public static String getRequestURL(HttpServletRequest httpRequest) {
        final StringBuilder url = new StringBuilder(100).append(httpRequest.getRequestURL());
        if (httpRequest.getQueryString() != null) {
            url.append('?').append(httpRequest.getQueryString());
        }

        return url.toString();
    }
}
