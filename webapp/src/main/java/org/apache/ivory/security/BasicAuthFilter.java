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

package org.apache.ivory.security;

import org.apache.ivory.util.StartupProperties;
import org.apache.log4j.NDC;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import java.io.IOException;

public class BasicAuthFilter implements Filter {

    private static final String GUEST = "guest";

    private boolean secure;

    @Override
    public void init(FilterConfig filterConfig)
            throws ServletException {
        String secure = StartupProperties.get().getProperty("security.enabled",
                "false");
        this.secure = Boolean.parseBoolean(secure);
    }

    @Override
    public void doFilter(ServletRequest request,
                         ServletResponse response,
                         FilterChain chain)
            throws IOException, ServletException {

        if (!(request instanceof HttpServletRequest) ||
                !(response instanceof HttpServletResponse)) {
            throw new IllegalStateException("Invalid request/response object");
        }
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        String user;

        if (!secure) {
            user = GUEST;
        } else {
            user = httpRequest.getHeader("Remote-User");
        }

        if (user == null || user.isEmpty()) {
            httpResponse.sendError(Response.Status.BAD_REQUEST.getStatusCode(),
                    "Remote user header can't be empty");
        } else {
            CurrentUser.authenticate(user);
            try {
                NDC.push(user + ":" + httpRequest.getPathInfo());
                chain.doFilter(request, response);
            } finally {
                NDC.pop();
            }
        }
    }

    @Override
    public void destroy() {
    }
}
