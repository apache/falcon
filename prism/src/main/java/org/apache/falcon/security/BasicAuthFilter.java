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

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.log4j.Logger;
import org.apache.log4j.NDC;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/**
 * This enforces authentication as part of the filter before processing the request.
 * Subclass of {@link AuthenticationFilter}.
 */
public class BasicAuthFilter extends AuthenticationFilter {

    private static final Logger LOG = Logger.getLogger(BasicAuthFilter.class);

    /**
     * Constant for the configuration property that indicates the prefix.
     */
    private static final String FALCON_PREFIX = "falcon.http.authentication.";

    /**
     * Constant for the configuration property that indicates the blacklisted super users for falcon.
     */
    private static final String BLACK_LISTED_USERS_KEY = FALCON_PREFIX + "blacklisted.users";

    /**
     * An options servlet is used to authenticate users. OPTIONS method is used for triggering authentication
     * before invoking the actual resource.
     */
    private HttpServlet optionsServlet;
    private Set<String> blackListedUsers;

    /**
     * Initialize the filter.
     *
     * @param filterConfig filter configuration.
     * @throws ServletException thrown if the filter could not be initialized.
     */
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        LOG.info("BasicAuthFilter initialization started");
        super.init(filterConfig);

        optionsServlet = new HttpServlet() {};
        optionsServlet.init();

        initializeBlackListedUsers();
    }

    private void initializeBlackListedUsers() {
        blackListedUsers = new HashSet<String>();
        String blackListedUserConfig = StartupProperties.get().getProperty(BLACK_LISTED_USERS_KEY);
        if (!StringUtils.isEmpty(blackListedUserConfig)) {
            blackListedUsers.addAll(Arrays.asList(blackListedUserConfig.split(",")));
        }
    }

    /**
     * Returns the configuration from Oozie configuration to be used by the authentication filter.
     * <p/>
     * All properties from Oozie configuration which name starts with {@link #FALCON_PREFIX} will
     * be returned. The keys of the returned properties are trimmed from the {@link #FALCON_PREFIX}
     * prefix, for example the Oozie configuration property name 'oozie.authentication.type' will
     * be just 'type'.
     *
     * @param configPrefix configuration prefix, this parameter is ignored by this implementation.
     * @param filterConfig filter configuration, this parameter is ignored by this implementation.
     * @return all Oozie configuration properties prefixed with {@link #FALCON_PREFIX}, without the
     * prefix.
     */
    @Override
    protected Properties getConfiguration(String configPrefix, FilterConfig filterConfig) {
        Properties authProperties = new Properties();
        Properties configProperties = StartupProperties.get();

        // setting the cookie path to root '/' so it is used for all resources.
        authProperties.setProperty(AuthenticationFilter.COOKIE_PATH, "/");

        for (Map.Entry entry : configProperties.entrySet()) {
            String name = (String) entry.getKey();
            if (name.startsWith(FALCON_PREFIX)) {
                String value = (String) entry.getValue();
                name = name.substring(FALCON_PREFIX.length());
                authProperties.setProperty(name, value);
            }
        }

        return authProperties;
    }

    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response,
                         final FilterChain filterChain) throws IOException, ServletException {

        FilterChain filterChainWrapper = new FilterChain() {

            @Override
            public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse)
                throws IOException, ServletException {
                HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;

                if (httpRequest.getMethod().equals("OPTIONS")) { // option request meant only for authentication
                    optionsServlet.service(request, response);
                } else {
                    final String user = getUserFromRequest(httpRequest);
                    if (StringUtils.isEmpty(user)) {
                        ((HttpServletResponse) response).sendError(Response.Status.BAD_REQUEST.getStatusCode(),
                                "User can't be empty");
                    } else if (blackListedUsers.contains(user)) {
                        ((HttpServletResponse) response).sendError(Response.Status.BAD_REQUEST.getStatusCode(),
                                "User can't be a superuser:" + BLACK_LISTED_USERS_KEY);
                    } else {
                        try {
                            String requestId = UUID.randomUUID().toString();
                            NDC.push(user + ":" + httpRequest.getMethod() + "/" + httpRequest.getPathInfo());
                            NDC.push(requestId);
                            CurrentUser.authenticate(user);
                            LOG.info("Request from user: " + user + ", URL=" + getRequestUrl(httpRequest));

                            filterChain.doFilter(servletRequest, servletResponse);
                        } finally {
                            NDC.pop();
                            NDC.pop();
                        }
                    }
                }
            }

            private String getUserFromRequest(HttpServletRequest httpRequest) {
                String user = httpRequest.getRemoteUser(); // this is available from wrapper in super class
                if (!StringUtils.isEmpty(user)) {
                    return user;
                }

                user = httpRequest.getParameter("user.name"); // available in query-param
                if (!StringUtils.isEmpty(user)) {
                    return user;
                }

                user = httpRequest.getHeader("Remote-User"); // backwards-compatibility
                if (!StringUtils.isEmpty(user)) {
                    return user;
                }

                return null;
            }

            private String getRequestUrl(HttpServletRequest request) {
                StringBuffer url = request.getRequestURL();
                if (request.getQueryString() != null) {
                    url.append("?").append(request.getQueryString());
                }

                return url.toString();
            }
        };

        super.doFilter(request, response, filterChainWrapper);
    }

    @Override
    public void destroy() {
        if (optionsServlet != null) {
            optionsServlet.destroy();
        }

        super.destroy();
    }
}
