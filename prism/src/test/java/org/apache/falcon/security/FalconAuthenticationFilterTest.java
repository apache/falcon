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

import org.apache.falcon.service.GroupsService;
import org.apache.falcon.service.ProxyUserService;
import org.apache.falcon.service.Services;
import org.apache.falcon.util.FalconTestUtil;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.security.AccessControlException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Test for FalconAuthenticationFilter using mock objects.
 */
public class FalconAuthenticationFilterTest {
    private ProxyUserService proxyUserService;

    private GroupsService groupsService;

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

    @Mock
    private HostnameFilter mockHostnameFilter;

    @BeforeClass
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        Services.get().register(new ProxyUserService());
        Services.get().register(new GroupsService());
        groupsService = Services.get().getService(GroupsService.SERVICE_NAME);
        proxyUserService = Services.get().getService(ProxyUserService.SERVICE_NAME);
        groupsService.init();

        RuntimeProperties.get().setProperty("falcon.service.ProxyUserService.proxyuser.foo.hosts", "*");
        RuntimeProperties.get().setProperty("falcon.service.ProxyUserService.proxyuser.foo.groups", "*");
        proxyUserService.init();
    }

    @AfterClass
    public void tearDown() throws Exception {
        proxyUserService.destroy();
        groupsService.destroy();
        Services.get().reset();
    }

    @BeforeMethod
    private void initAuthType() {
        ConcurrentHashMap<String, String> conf = new ConcurrentHashMap<String, String>();
        conf.put("type", "simple");
        conf.put("config.prefix.type", "");
        conf.put("anonymous.allowed", "true");
        ServletContext servletContext = Mockito.mock(ServletContext.class);
        Mockito.when(mockConfig.getInitParameterNames()).thenReturn(conf.keys());
        Mockito.when(mockConfig.getServletContext()).thenReturn(servletContext);

        for (Map.Entry<String, String> entry : conf.entrySet()) {
            Mockito.when(mockConfig.getInitParameter(entry.getKey())).thenReturn(entry.getValue());
        }

        Mockito.when(mockRequest.getMethod()).thenReturn("OPTIONS");

        StringBuffer requestUrl = new StringBuffer("http://localhost");
        Mockito.when(mockRequest.getRequestURL()).thenReturn(requestUrl);
    }

    @Test
    public void testDoFilter() throws Exception {
        Filter filter = new FalconAuthenticationFilter();
        synchronized (StartupProperties.get()) {
            filter.init(mockConfig);
        }

        CurrentUser.authenticate("nouser");
        Assert.assertEquals(CurrentUser.getUser(), "nouser");

        CurrentUser.authenticate("guest");
        Mockito.when(mockRequest.getQueryString()).thenReturn("user.name=guest");
        filter.doFilter(mockRequest, mockResponse, mockChain);
        Assert.assertEquals(CurrentUser.getUser(), "guest");

        CurrentUser.authenticate("nouser");
        Assert.assertEquals(CurrentUser.getUser(), "nouser");
        CurrentUser.authenticate(FalconTestUtil.TEST_USER_2);
        Mockito.when(mockRequest.getRemoteUser()).thenReturn(FalconTestUtil.TEST_USER_2);
        filter.doFilter(mockRequest, mockResponse, mockChain);
        Assert.assertEquals(CurrentUser.getUser(), FalconTestUtil.TEST_USER_2);
    }

    @Test
    public void testAnonymous() throws Exception {
        Filter filter = new FalconAuthenticationFilter();

        synchronized (StartupProperties.get()) {
            filter.init(mockConfig);
        }

        CurrentUser.authenticate("nouser");
        Assert.assertEquals(CurrentUser.getUser(), "nouser");

        CurrentUser.authenticate(FalconTestUtil.TEST_USER_2);
        Mockito.when(mockRequest.getRemoteUser()).thenReturn(FalconTestUtil.TEST_USER_2);
        filter.doFilter(mockRequest, mockResponse, mockChain);
        Assert.assertEquals(CurrentUser.getUser(), FalconTestUtil.TEST_USER_2);
    }

    @Test
    public void testEmptyUser() throws Exception {
        Filter filter = new FalconAuthenticationFilter();

        synchronized (StartupProperties.get()) {
            filter.init(mockConfig);
        }

        final String userName = System.getProperty("user.name");
        try {
            System.setProperty("user.name", "");

            Mockito.when(mockRequest.getMethod()).thenReturn("POST");
            Mockito.when(mockRequest.getQueryString()).thenReturn("");
            Mockito.when(mockRequest.getRemoteUser()).thenReturn(null);

            HttpServletResponse errorResponse = Mockito.mock(HttpServletResponse.class);
            filter.doFilter(mockRequest, errorResponse, mockChain);
        } finally {
            System.setProperty("user.name", userName);
        }
    }

    @Test
    public void testDoFilterForClientBackwardsCompatibility() throws Exception {
        Filter filter = new FalconAuthenticationFilter();

        final String userName = System.getProperty("user.name");
        final String httpAuthType =
                StartupProperties.get().getProperty("falcon.http.authentication.type", "simple");
        try {
            System.setProperty("user.name", "");
            StartupProperties.get().setProperty("falcon.http.authentication.type",
                    "org.apache.falcon.security.RemoteUserInHeaderBasedAuthenticationHandler");

            synchronized (StartupProperties.get()) {
                filter.init(mockConfig);
            }

            Mockito.when(mockRequest.getMethod()).thenReturn("POST");
            Mockito.when(mockRequest.getQueryString()).thenReturn("");
            Mockito.when(mockRequest.getRemoteUser()).thenReturn(null);
            Mockito.when(mockRequest.getHeader("Remote-User")).thenReturn("remote-user");

            filter.doFilter(mockRequest, mockResponse, mockChain);

            Assert.assertEquals(CurrentUser.getUser(), "remote-user");

        } finally {
            System.setProperty("user.name", userName);
            StartupProperties.get().setProperty("falcon.http.authentication.type", httpAuthType);
        }
    }

    @Test
    public void testGetKerberosPrincipalWithSubstitutedHostSecure() throws Exception {
        String principal = StartupProperties.get().getProperty(FalconAuthenticationFilter.KERBEROS_PRINCIPAL);

        String expectedPrincipal = "falcon/" + SecurityUtil.getLocalHostName().toLowerCase() + "@Example.com";
        try {
            Configuration conf = new Configuration(false);
            conf.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(conf);
            Assert.assertTrue(UserGroupInformation.isSecurityEnabled());

            StartupProperties.get().setProperty(
                    FalconAuthenticationFilter.KERBEROS_PRINCIPAL, "falcon/_HOST@Example.com");
            FalconAuthenticationFilter filter = new FalconAuthenticationFilter();
            Properties properties = filter.getConfiguration(FalconAuthenticationFilter.FALCON_PREFIX, null);
            Assert.assertEquals(
                    properties.get(KerberosAuthenticationHandler.PRINCIPAL), expectedPrincipal);
        } finally {
            StartupProperties.get().setProperty(FalconAuthenticationFilter.KERBEROS_PRINCIPAL, principal);
        }
    }

    @Test
    public void testGetKerberosPrincipalWithSubstitutedHostNonSecure() throws Exception {
        String principal = StartupProperties.get().getProperty(FalconAuthenticationFilter.KERBEROS_PRINCIPAL);
        Configuration conf = new Configuration(false);
        conf.set("hadoop.security.authentication", "simple");
        UserGroupInformation.setConfiguration(conf);
        Assert.assertFalse(UserGroupInformation.isSecurityEnabled());

        FalconAuthenticationFilter filter = new FalconAuthenticationFilter();
        Properties properties = filter.getConfiguration(FalconAuthenticationFilter.FALCON_PREFIX, null);
        Assert.assertEquals(properties.get(KerberosAuthenticationHandler.PRINCIPAL), principal);
    }

    @Test
    public void testDoFilterWithEmptyDoAsUser() throws Exception {
        Filter filter = new FalconAuthenticationFilter();
        synchronized (StartupProperties.get()) {
            filter.init(mockConfig);
        }

        CurrentUser.authenticate("testuser");
        Mockito.when(mockRequest.getMethod()).thenReturn("POST");
        Mockito.when(mockRequest.getQueryString()).thenReturn("user.name=testuser");
        Mockito.when(mockRequest.getRemoteUser()).thenReturn("testuser");
        Mockito.when(mockRequest.getParameter(FalconAuthenticationFilter.DO_AS_PARAM)).thenReturn("");
        filter.doFilter(mockRequest, mockResponse, mockChain);
        Assert.assertEquals(CurrentUser.getUser(), "testuser");
    }

    @Test
    public void testDoFilterWithDoAsUser() throws Exception {
        Filter filter = new FalconAuthenticationFilter();
        HostnameFilter.HOSTNAME_TL.set("localhost");
        synchronized (StartupProperties.get()) {
            filter.init(mockConfig);
        }

        CurrentUser.authenticate("foo");
        Mockito.when(mockRequest.getMethod()).thenReturn("POST");
        Mockito.when(mockRequest.getQueryString()).thenReturn("user.name=foo");
        Mockito.when(mockRequest.getRemoteUser()).thenReturn("foo");
        Mockito.when(mockRequest.getParameter(FalconAuthenticationFilter.DO_AS_PARAM)).thenReturn("doAsProxyUser");
        Mockito.when(mockRequest.getMethod()).thenReturn("POST");
        filter.doFilter(mockRequest, mockResponse, mockChain);
        Assert.assertEquals(CurrentUser.getUser(), "doAsProxyUser");
    }

    @Test (expectedExceptions = AccessControlException.class,
           expectedExceptionsMessageRegExp = "User .* not defined as proxyuser.*")
    public void testDoFilterWithInvalidProxyUser() throws Exception {
        Filter filter = new FalconAuthenticationFilter();
        HostnameFilter.HOSTNAME_TL.set("localhost");
        synchronized (StartupProperties.get()) {
            filter.init(mockConfig);
        }

        CurrentUser.authenticate("testuser");
        Mockito.when(mockRequest.getMethod()).thenReturn("POST");
        Mockito.when(mockRequest.getQueryString()).thenReturn("user.name=testuser");
        Mockito.when(mockRequest.getRemoteUser()).thenReturn("testuser");
        Mockito.when(mockRequest.getParameter(FalconAuthenticationFilter.DO_AS_PARAM)).thenReturn("doAsProxyUser");
        Mockito.when(mockRequest.getMethod()).thenReturn("POST");
        filter.doFilter(mockRequest, mockResponse, mockChain);
        Assert.assertEquals(CurrentUser.getUser(), "doAsProxyUser");
    }
}
