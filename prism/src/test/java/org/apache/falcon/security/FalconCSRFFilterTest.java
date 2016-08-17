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

import org.apache.falcon.util.StartupProperties;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;

/**
 * Test for FalconCSRFFilter using mock objects.
 */
public class FalconCSRFFilterTest {
    private static final String FALCON_CSRF_HEADER_DEFAULT = "FALCON-CSRF-FILTER";

    @Mock
    private HttpServletRequest mockRequest;

    @Mock
    private HttpServletResponse mockResponse;

    @Mock
    private FilterChain mockChain;

    @Mock
    private FilterConfig mockConfig;

    @BeforeClass
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testCSRFEnabledAllowedMethodFromBrowser() throws Exception {
        StartupProperties.get().setProperty("falcon.security.csrf.enabled", "true");
        StartupProperties.get().setProperty("falcon.security.csrf.header", FALCON_CSRF_HEADER_DEFAULT);
        mockHeader("Mozilla/5.0", null);
        mockGetMethod();
        mockRunFilter();
        Mockito.verify(mockResponse, Mockito.never()).sendError(HttpServletResponse.SC_FORBIDDEN,
                RestCsrfPreventionFilter.CSRF_ERROR_MESSAGE);
    }

    @Test
    public void testCSRFEnabledNoCustomHeaderFromBrowser() throws Exception {
        StartupProperties.get().setProperty("falcon.security.csrf.enabled", "true");
        StartupProperties.get().setProperty("falcon.security.csrf.header", FALCON_CSRF_HEADER_DEFAULT);
        mockHeader("Mozilla/5.0", null);
        mockDeleteMethod();
        mockRunFilter();
        Mockito.verify(mockResponse).sendError(HttpServletResponse.SC_FORBIDDEN,
                RestCsrfPreventionFilter.CSRF_ERROR_MESSAGE);
    }

    @Test
    public void testCSRFEnabledIncludeCustomHeaderFromBrowser() throws Exception {
        StartupProperties.get().setProperty("falcon.security.csrf.enabled", "true");
        StartupProperties.get().setProperty("falcon.security.csrf.header", FALCON_CSRF_HEADER_DEFAULT);
        mockHeader("Mozilla/5.0", "");
        mockDeleteMethod();
        mockRunFilter();
        Mockito.verify(mockResponse, Mockito.never()).sendError(HttpServletResponse.SC_FORBIDDEN,
                RestCsrfPreventionFilter.CSRF_ERROR_MESSAGE);
    }

    @Test
    public void testCSRFEnabledAllowNonBrowserInteractionWithoutHeader() throws Exception {
        StartupProperties.get().setProperty("falcon.security.csrf.enabled", "true");
        StartupProperties.get().setProperty("falcon.security.csrf.header", FALCON_CSRF_HEADER_DEFAULT);
        mockHeader(null, null);
        mockDeleteMethod();
        mockRunFilter();
        Mockito.verify(mockResponse, Mockito.never()).sendError(HttpServletResponse.SC_FORBIDDEN,
                RestCsrfPreventionFilter.CSRF_ERROR_MESSAGE);
    }

    @Test
    public void testCSRFDisabledAllowAnyMethodFromBrowser() throws Exception {
        StartupProperties.get().setProperty("falcon.security.csrf.enabled", "false");
        StartupProperties.get().setProperty("falcon.security.csrf.header", FALCON_CSRF_HEADER_DEFAULT);
        mockHeader("Mozilla/5.0", null);
        mockDeleteMethod();
        mockRunFilter();
        Mockito.verify(mockResponse, Mockito.never()).sendError(HttpServletResponse.SC_FORBIDDEN,
                RestCsrfPreventionFilter.CSRF_ERROR_MESSAGE);
    }

    private void mockGetMethod() {
        mockMethod(HttpMethod.GET, "/entities/list");
    }

    private void mockDeleteMethod() {
        mockMethod(HttpMethod.DELETE, "/entities/delete/cluster/primaryCluster");
    }

    private void mockMethod(String method, String resource) {
        StringBuffer requestUrl = new StringBuffer("http://localhost" + resource);
        Mockito.when(mockRequest.getRequestURL()).thenReturn(requestUrl);
        Mockito.when(mockRequest.getRequestURI()).thenReturn("/api" + resource);
        Mockito.when(mockRequest.getPathInfo()).thenReturn(resource);
        Mockito.when(mockRequest.getMethod()).thenReturn(method);
    }

    private void mockHeader(String userAgent, String customHeader) {
        Mockito.when(mockRequest.getHeader(RestCsrfPreventionFilter.HEADER_USER_AGENT)).thenReturn(userAgent);
        Mockito.when(mockRequest.getHeader(FALCON_CSRF_HEADER_DEFAULT)).thenReturn(customHeader);
    }

    private void mockRunFilter() throws Exception {
        Mockito.when(mockConfig.getInitParameter("methods-to-ignore")).thenReturn("GET");
        FalconCSRFFilter filter = new FalconCSRFFilter();
        filter.init(mockConfig);
        try {
            filter.doFilter(mockRequest, mockResponse, mockChain);
        } finally {
            filter.destroy();
        }
    }
}
