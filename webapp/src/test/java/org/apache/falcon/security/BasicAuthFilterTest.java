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
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;

import static org.mockito.Mockito.*;

public class BasicAuthFilterTest {

    @Mock
    private HttpServletRequest mockRequest;

    @Mock
    private HttpServletResponse mockResponse;

    @Mock
    private FilterChain mockChain;

    @Mock
    private FilterConfig mockConfig;

    @BeforeClass
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testDoFilter() throws Exception {
        Filter filter = new BasicAuthFilter();
        synchronized (StartupProperties.get()) {
            StartupProperties.get().setProperty("security.enabled", "false");
            filter.init(mockConfig);
        }

        CurrentUser.authenticate("nouser");
        Assert.assertEquals(CurrentUser.getUser(), "nouser");
        when(mockRequest.getHeader("Remote-User")).thenReturn("testuser");
        filter.doFilter(mockRequest, mockResponse, mockChain);
        Assert.assertEquals(CurrentUser.getUser(), "guest");

        synchronized (StartupProperties.get()) {
            StartupProperties.get().remove("security.enabled");
            filter.init(mockConfig);
        }

        CurrentUser.authenticate("nouser");
        Assert.assertEquals(CurrentUser.getUser(), "nouser");
        when(mockRequest.getHeader("Remote-User")).thenReturn("testuser");
        filter.doFilter(mockRequest, mockResponse, mockChain);
        Assert.assertEquals(CurrentUser.getUser(), "testuser");
    }

    @Test
    public void testAnonymous() throws Exception {
        Filter filter = new BasicAuthFilter();

        synchronized (StartupProperties.get()) {
            StartupProperties.get().setProperty("security.enabled", "true");
            filter.init(mockConfig);
        }

        CurrentUser.authenticate("nouser");
        Assert.assertEquals(CurrentUser.getUser(), "nouser");
        when(mockRequest.getHeader("Remote-User")).thenReturn("testuser");
        filter.doFilter(mockRequest, mockResponse, mockChain);
        Assert.assertEquals(CurrentUser.getUser(), "testuser");
    }

    @Test
    public void testEmptyUser() throws Exception {
        Filter filter = new BasicAuthFilter();

        synchronized (StartupProperties.get()) {
            StartupProperties.get().setProperty("security.enabled", "true");
            filter.init(mockConfig);
        }

        HttpServletResponse errorResponse = mock(HttpServletResponse.class);
        when(mockRequest.getHeader("Remote-User")).thenReturn(null);
        filter.doFilter(mockRequest, errorResponse, mockChain);
        verify(errorResponse).sendError(Response.Status.BAD_REQUEST.getStatusCode(),
                "Remote user header can't be empty");
    }
}
