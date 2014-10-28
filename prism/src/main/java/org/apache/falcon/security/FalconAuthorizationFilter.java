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

import org.apache.falcon.FalconException;
import org.apache.falcon.FalconWebException;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;

/**
 * This enforces authorization as part of the filter before processing the request.
 */
public class FalconAuthorizationFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger(FalconAuthorizationFilter.class);

    private boolean isAuthorizationEnabled;
    private AuthorizationProvider authorizationProvider;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        try {
            isAuthorizationEnabled = SecurityUtil.isAuthorizationEnabled();
            if (isAuthorizationEnabled) {
                LOG.info("Falcon is running with authorization enabled");
            }

            authorizationProvider = SecurityUtil.getAuthorizationProvider();
        } catch (FalconException e) {
            throw new ServletException(e);
        }
    }

    @Override
    public void doFilter(ServletRequest request,
                         ServletResponse response,
                         FilterChain filterChain) throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        RequestParts requestParts = getUserRequest(httpRequest);

        if (isAuthorizationEnabled) {
            LOG.info("Authorizing user={} against request={}", CurrentUser.getUser(), requestParts);
            try {
                authorizationProvider.authorizeResource(requestParts.getResource(),
                        requestParts.getAction(), requestParts.getEntityType(),
                        requestParts.getEntityName(), CurrentUser.getProxyUGI());
            } catch (AuthorizationException e) {
                throw FalconWebException.newException(e.getMessage(), Response.Status.FORBIDDEN);
            }
        }

        filterChain.doFilter(request, response);
    }

    @Override
    public void destroy() {
        authorizationProvider = null;
    }

    /**
     * Returns the resource and action for the given request.
     *
     * @param httpRequest    an HTTP servlet request
     * @return the parts of a path
     */
    private static RequestParts getUserRequest(HttpServletRequest httpRequest) {
        String pathInfo = httpRequest.getPathInfo();
        final String[] pathSplits = pathInfo.substring(1).split("/");
        final String resource = pathSplits[0];

        ArrayList<String> splits = new ArrayList<String>();
        if (resource.equals("graphs")) {
            splits.add(pathSplits[1]);  // resource
            splits.add(pathSplits[2]);  // action
        } else {
            splits.add(pathSplits[0]);  // resource
            splits.add(pathSplits[1]);  // action
            if (pathSplits.length > 2) {  // entity type
                splits.add(pathSplits[2]);
            }
            if (pathSplits.length > 3) {  // entity name
                splits.add(pathSplits[3]);
            }
        }

        final String entityType = splits.size() > 2 ? splits.get(2) : null;
        final String entityName = splits.size() > 3 ? splits.get(3) : null;

        return new RequestParts(splits.get(0), splits.get(1), entityName, entityType);
    }

    private static class RequestParts {
        private final String resource;
        private final String action;
        private final String entityName;
        private final String entityType;

        public RequestParts(String resource, String action,
                            String entityName, String entityType) {
            this.resource = resource;
            this.action = action;
            this.entityName = entityName;
            this.entityType = entityType;
        }

        public String getResource() {
            return resource;
        }

        public String getAction() {
            return action;
        }

        public String getEntityName() {
            return entityName;
        }

        public String getEntityType() {
            return entityType;
        }

        @Override
        public String toString() {
            return "RequestParts{"
                    + "resource='" + resource + '\''
                    + ", action='" + action + '\''
                    + ", entityName='" + entityName + '\''
                    + ", entityType='" + entityType + '\''
                    + '}';
        }
    }
}
