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

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Source code forked from Hadoop 2.8.0+ org.apache.hadoop.security.http.RestCsrfPreventionFilter.
 */
@Public
@Evolving
public class RestCsrfPreventionFilter implements Filter {
    private static final Logger LOG = LoggerFactory.getLogger(RestCsrfPreventionFilter.class);
    public static final String HEADER_USER_AGENT = "User-Agent";
    public static final String BROWSER_USER_AGENT_PARAM = "browser-useragents-regex";
    public static final String CUSTOM_HEADER_PARAM = "custom-header";
    public static final String CUSTOM_METHODS_TO_IGNORE_PARAM = "methods-to-ignore";
    static final String BROWSER_USER_AGENTS_DEFAULT = "^Mozilla.*,^Opera.*";
    public static final String HEADER_DEFAULT = "X-XSRF-HEADER";
    static final String METHODS_TO_IGNORE_DEFAULT = "GET,OPTIONS,HEAD,TRACE";
    public static final String CSRF_ERROR_MESSAGE = "Missing Required Header for CSRF Vulnerability Protection";
    protected String headerName = "X-XSRF-HEADER";
    protected Set<String> methodsToIgnore = null;
    protected Set<Pattern> browserUserAgents;

    public RestCsrfPreventionFilter() {
    }

    public void init(FilterConfig filterConfig) throws ServletException {
        String customHeader = filterConfig.getInitParameter(CUSTOM_HEADER_PARAM);
        if (customHeader != null) {
            this.headerName = customHeader;
        }

        String customMethodsToIgnore = filterConfig.getInitParameter(CUSTOM_METHODS_TO_IGNORE_PARAM);
        if (customMethodsToIgnore != null) {
            this.parseMethodsToIgnore(customMethodsToIgnore);
        } else {
            this.parseMethodsToIgnore(METHODS_TO_IGNORE_DEFAULT);
        }

        String agents = filterConfig.getInitParameter(BROWSER_USER_AGENT_PARAM);
        if (agents == null) {
            agents = BROWSER_USER_AGENTS_DEFAULT;
        }

        this.parseBrowserUserAgents(agents);
    }

    void parseBrowserUserAgents(String userAgents) {
        String[] agentsArray = userAgents.split(",");
        this.browserUserAgents = new HashSet();
        String[] arr = agentsArray;
        int len = agentsArray.length;

        for (int i = 0; i < len; ++i) {
            String patternString = arr[i];
            this.browserUserAgents.add(Pattern.compile(patternString));
        }

    }

    void parseMethodsToIgnore(String mti) {
        String[] methods = mti.split(",");
        this.methodsToIgnore = new HashSet();

        for (int i = 0; i < methods.length; ++i) {
            this.methodsToIgnore.add(methods[i]);
        }

    }

    protected boolean isBrowser(String userAgent) {
        if (userAgent == null) {
            return false;
        } else {
            Iterator iterator = this.browserUserAgents.iterator();

            Matcher matcher;
            do {
                if (!iterator.hasNext()) {
                    return false;
                }

                Pattern pattern = (Pattern)iterator.next();
                matcher = pattern.matcher(userAgent);
            } while(!matcher.matches());

            return true;
        }
    }

    public void handleHttpInteraction(RestCsrfPreventionFilter.HttpInteraction httpInteraction)
        throws IOException, ServletException {
        if (this.isBrowser(httpInteraction.getHeader(HEADER_USER_AGENT))
                && !this.methodsToIgnore.contains(httpInteraction.getMethod())
                && httpInteraction.getHeader(this.headerName) == null) {
            httpInteraction.sendError(HttpServletResponse.SC_FORBIDDEN, CSRF_ERROR_MESSAGE);
        } else {
            httpInteraction.proceed();
        }
    }

    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
        throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest)request;
        HttpServletResponse httpResponse = (HttpServletResponse)response;
        this.handleHttpInteraction(new RestCsrfPreventionFilter.ServletFilterHttpInteraction(
                httpRequest, httpResponse, chain));
    }

    public void destroy() {
    }

    private static final class ServletFilterHttpInteraction implements RestCsrfPreventionFilter.HttpInteraction {
        private final FilterChain chain;
        private final HttpServletRequest httpRequest;
        private final HttpServletResponse httpResponse;

        public ServletFilterHttpInteraction(HttpServletRequest httpRequest,
                                            HttpServletResponse httpResponse, FilterChain chain) {
            this.httpRequest = httpRequest;
            this.httpResponse = httpResponse;
            this.chain = chain;
        }

        public String getHeader(String header) {
            return this.httpRequest.getHeader(header);
        }

        public String getMethod() {
            return this.httpRequest.getMethod();
        }

        public void proceed() throws IOException, ServletException {
            this.chain.doFilter(this.httpRequest, this.httpResponse);
        }

        public void sendError(int code, String message) throws IOException {
            this.httpResponse.sendError(code, message);
        }
    }

    /**
     * Interface for HttpInteraction.
     */
    public interface HttpInteraction {
        String getHeader(String var1);

        String getMethod();

        void proceed() throws IOException, ServletException;

        void sendError(int var1, String var2) throws IOException;
    }
}
