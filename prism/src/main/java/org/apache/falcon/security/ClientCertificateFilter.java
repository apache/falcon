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
import org.apache.log4j.Logger;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;

/**
 * This verifies that the request has a valid client certificate.
 * Used by falcon server to talk to prism.
 */
public class ClientCertificateFilter implements Filter {

    private static final Logger LOG = Logger.getLogger(ClientCertificateFilter.class);

    protected boolean enableTLS = false;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        this.enableTLS = Boolean.parseBoolean(StartupProperties.get().getProperty("falcon.enableTLS", "false"));
    }

    @Override
    public void doFilter(ServletRequest request,
                         ServletResponse response,
                         FilterChain chain) throws IOException, ServletException {

        if (!(request instanceof HttpServletRequest) || !(response instanceof HttpServletResponse)) {
            throw new IllegalStateException("Invalid request/response object");
        }
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        X509Certificate[] certificates = (X509Certificate[]) request.
                getAttribute("javax.servlet.request.X509Certificate");

        if (!enableTLS || isValid(certificates)) {
            chain.doFilter(request, response);
        } else {
            httpResponse.sendError(Response.Status.FORBIDDEN.getStatusCode(),
                    "Request not authorized, valid certificates not presented");
        }
    }

    private boolean isValid(X509Certificate[] certificates) {
        boolean valid = false;
        if (certificates != null) {
            for (X509Certificate certificate : certificates) {
                LOG.debug("Issuer: " + certificate.getIssuerDN() + ", Subject: " + certificate.getSubjectDN());
                try {
                    certificate.checkValidity();
                    valid = true;
                    break;
                } catch (CertificateExpiredException e) {
                    LOG.error("Certificate " + certificate + " expired", e);
                } catch (CertificateNotYetValidException e) {
                    LOG.error("Certificate " + certificate + " not valid", e);
                }
            }
        } else {
            LOG.warn("No valid certificates present");
        }
        return valid;
    }

    @Override
    public void destroy() {
    }
}
