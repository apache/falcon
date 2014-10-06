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
import org.apache.hadoop.security.UserGroupInformation;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Test for FalconAuditFilterTest using mock objects.
 */
public class FalconAuditFilterTest {

    @Mock
    private HttpServletRequest mockRequest;

    @Mock
    private HttpServletResponse mockResponse;

    @Mock
    private FilterChain mockChain;

    @Mock
    private FilterConfig mockConfig;

    @Mock
    private UserGroupInformation mockUgi;

    @BeforeClass
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testDoFilter() throws Exception {
        Filter filter = new FalconAuditFilter();
        synchronized (StartupProperties.get()) {
            filter.init(mockConfig);
        }

        Mockito.when(mockRequest.getRemoteUser()).thenReturn("falcon");
        Mockito.when(mockRequest.getRemoteHost()).thenReturn("http://remotehost");
        Mockito.when(mockRequest.getRequestURI()).thenReturn("http://127.0.0.1:15000");
        filter.doFilter(mockRequest, mockResponse, mockChain);
    }

    @Test
    public void testDoFilterNoUserHostInRequest() throws Exception {
        Filter filter = new FalconAuditFilter();
        synchronized (StartupProperties.get()) {
            filter.init(mockConfig);
        }

        Mockito.when(mockRequest.getRequestURI()).thenReturn("http://127.0.0.1:15000");
        Mockito.when(mockRequest.getQueryString()).thenReturn("user.name=guest");
        filter.doFilter(mockRequest, mockResponse, mockChain);
    }

    @Test
    public void testDoFilterNoUserInRequest() throws Exception {
        Filter filter = new FalconAuditFilter();
        synchronized (StartupProperties.get()) {
            filter.init(mockConfig);
        }

        Mockito.when(mockRequest.getRemoteHost()).thenReturn("http://remotehost");
        Mockito.when(mockRequest.getRequestURI()).thenReturn("http://127.0.0.1:15000");
        Mockito.when(mockRequest.getQueryString()).thenReturn("user.name=guest");
        filter.doFilter(mockRequest, mockResponse, mockChain);
    }

    @Test
    public void testDoFilterNoHostInRequest() throws Exception {
        Filter filter = new FalconAuditFilter();
        synchronized (StartupProperties.get()) {
            filter.init(mockConfig);
        }

        Mockito.when(mockRequest.getRemoteUser()).thenReturn("falcon");
        Mockito.when(mockRequest.getRequestURI()).thenReturn("http://127.0.0.1:15000");
        Mockito.when(mockRequest.getQueryString()).thenReturn("user.name=guest");
        filter.doFilter(mockRequest, mockResponse, mockChain);
    }

    @Test
    public void testDoFilterEmptyRequest() throws Exception {
        Filter filter = new FalconAuditFilter();
        synchronized (StartupProperties.get()) {
            filter.init(mockConfig);
        }

        filter.doFilter(mockRequest, mockResponse, mockChain);
    }

    @Test (expectedExceptions = ServletException.class)
    public void testDoFilterErrorInChain() throws Exception {
        Filter filter = new FalconAuditFilter();
        synchronized (StartupProperties.get()) {
            filter.init(mockConfig);
        }

        FilterChain chain = new FilterChain() {
            @Override
            public void doFilter(ServletRequest request,
                                 ServletResponse response) throws IOException, ServletException {
                throw new ServletException("Something bad happened down the road");
            }
        };
        Mockito.when(mockRequest.getRemoteUser()).thenReturn("bad-user");
        Mockito.when(mockRequest.getRequestURI()).thenReturn("http://bad-host:15000");
        Mockito.when(mockRequest.getQueryString()).thenReturn("bad=param");
        filter.doFilter(mockRequest, mockResponse, chain);
    }
}
