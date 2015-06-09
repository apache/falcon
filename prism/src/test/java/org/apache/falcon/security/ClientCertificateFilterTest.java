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

import org.apache.log4j.Logger;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Test for Client Certificate auth.
 */
public class ClientCertificateFilterTest extends ClientCertificateFilter {
    private static final Logger LOG = Logger.getLogger(ClientCertificateFilterTest.class);

    @Mock
    private HttpServletRequest mockRequest;

    @Mock
    private HttpServletResponse mockResponseNoCert;

    @Mock
    private HttpServletResponse mockResponseNoCertSecurityDisabled;

    @Mock
    private HttpServletResponse mockResponseExpired;

    @Mock
    private HttpServletResponse mockResponseInvalid;

    @Mock
    private HttpServletResponse mockResponseSuccess;

    @Mock
    private FilterChain mockChain;

    @Mock
    private FilterConfig mockConfig;

    @Mock
    private X509Certificate mockCertificateSuccess;

    @Mock
    private X509Certificate mockCertificateExpired;

    @Mock
    private X509Certificate mockCertificateNotValid;

    @BeforeClass
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testNoCert() throws Exception {
        init(mockConfig);

        this.enableTLS = true;
        when(mockRequest.getAttribute("javax.servlet.request.X509Certificate")).
                thenReturn(null);
        doFilter(mockRequest, mockResponseNoCert, mockChain);
        verify(mockResponseNoCert).sendError(Response.Status.FORBIDDEN.getStatusCode(),
                "Request not authorized, valid certificates not presented");
    }

    @Test
    public void testNoCertSecurityDisabled() throws Exception {
        this.enableTLS = false;
        when(mockRequest.getAttribute("javax.servlet.request.X509Certificate")).
                thenReturn(null);
        doFilter(mockRequest, mockResponseNoCertSecurityDisabled, mockChain);
    }

    @Test
    public void testValidCertificate() throws Exception {
        this.enableTLS = true;
        when(mockRequest.getAttribute("javax.servlet.request.X509Certificate")).
                thenReturn(new X509Certificate[]{mockCertificateSuccess});
        doFilter(mockRequest, mockResponseSuccess, mockChain);
    }

    @Test
    public void testExpiredCertificate() throws Exception {
        this.enableTLS = true;
        doThrow(new CertificateExpiredException()).when(mockCertificateExpired).checkValidity();
        when(mockRequest.getAttribute("javax.servlet.request.X509Certificate")).
                thenReturn(new X509Certificate[]{mockCertificateExpired});
        doFilter(mockRequest, mockResponseExpired, mockChain);
        verify(mockResponseExpired).sendError(Response.Status.FORBIDDEN.getStatusCode(),
                "Request not authorized, valid certificates not presented");
    }

    @Test
    public void testInvalidCertificate() throws Exception {
        this.enableTLS = true;
        doThrow(new CertificateNotYetValidException()).when(mockCertificateNotValid).checkValidity();
        when(mockRequest.getAttribute("javax.servlet.request.X509Certificate")).
                thenReturn(new X509Certificate[] {mockCertificateNotValid});
        doFilter(mockRequest, mockResponseInvalid, mockChain);
        verify(mockResponseInvalid).sendError(Response.Status.FORBIDDEN.getStatusCode(),
                "Request not authorized, valid certificates not presented");
    }
}
