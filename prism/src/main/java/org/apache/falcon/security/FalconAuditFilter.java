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

package org.apache.falcon.security;

import org.apache.falcon.aspect.GenericAlert;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.util.Servlets;
import org.apache.log4j.NDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Date;
import java.util.UUID;

/**
 * This records audit information as part of the filter after processing the request
 * and also introduces a UUID into request and response for tracing requests in logs.
 */
public class FalconAuditFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger(FalconAuditFilter.class);

    public static final String REQUEST_ID = "requestId";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        LOG.info("FalconAuditFilter initialization started");
    }

    @Override
    public void doFilter(ServletRequest request,
                         ServletResponse response,
                         FilterChain filterChain) throws IOException, ServletException {
        final String requestTimeISO9601 = SchemaHelper.formatDateUTC(new Date());
        // generate a unique id and shove it into NDC
        final String requestId = UUID.randomUUID().toString();
        NDC.push(requestId);

        try {
            filterChain.doFilter(request, response);
        } finally {
            recordAudit((HttpServletRequest) request, requestTimeISO9601);

            // put the request id into the response so users can trace logs for this request
            ((HttpServletResponse) response).setHeader(REQUEST_ID, requestId);
            NDC.pop();
            CurrentUser.clear();
        }
    }

    private void recordAudit(HttpServletRequest httpRequest, String whenISO9601) {
        final String who = getUserFromRequest(httpRequest);
        final String fromHost = httpRequest.getRemoteHost();
        final String fromAddress = httpRequest.getRemoteAddr();
        final String whatURL = Servlets.getRequestURL(httpRequest);
        final String whatAddrs = httpRequest.getLocalAddr();

        LOG.debug("Audit: {}/{} performed request {} ({}) at time {}",
                who, fromAddress, whatURL, whatAddrs, whenISO9601);
        GenericAlert.audit(who, fromAddress, fromHost, whatURL, whatAddrs, whenISO9601);
    }

    private String getUserFromRequest(HttpServletRequest httpRequest) {
        if (CurrentUser.isAuthenticated()) {
            return CurrentUser.getAuthenticatedUser();
        } else {
            // look for the user in the request
            final String userFromRequest = Servlets.getUserFromRequest(httpRequest);
            return userFromRequest == null ? "UNKNOWN" : userFromRequest;
        }
    }

    @Override
    public void destroy() {
        // do nothing
    }
}
