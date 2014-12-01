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
import org.apache.falcon.entity.EntityNotRegisteredException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.util.Servlets;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

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
        if (isAuthorizationEnabled) {
            HttpServletRequest httpRequest = (HttpServletRequest) request;
            RequestParts requestParts = getUserRequest(httpRequest);
            LOG.info("Authorizing user={} against request={}", CurrentUser.getUser(), requestParts);

            try {
                final UserGroupInformation authenticatedUGI = CurrentUser.getAuthenticatedUGI();
                authorizationProvider.authorizeResource(requestParts.getResource(),
                        requestParts.getAction(), requestParts.getEntityType(),
                        requestParts.getEntityName(), authenticatedUGI);
                tryProxy(authenticatedUGI,
                    requestParts.getEntityType(), requestParts.getEntityName());
                LOG.info("Authorization succeeded for user={}, proxy={}",
                    authenticatedUGI.getShortUserName(), CurrentUser.getUser());
            } catch (AuthorizationException e) {
                sendError((HttpServletResponse) response,
                    HttpServletResponse.SC_FORBIDDEN, e.getMessage());
                return; // do not continue processing
            } catch (EntityNotRegisteredException e) {
                if (!httpRequest.getMethod().equals(HttpMethod.DELETE)) {
                    sendError((HttpServletResponse) response,
                            HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
                    return; // do not continue processing
                } // else Falcon deletes a non-existing entity and returns success (idempotent operation).
            } catch (IllegalArgumentException e) {
                sendError((HttpServletResponse) response,
                    HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
                return; // do not continue processing
            }
        }

        // continue processing if there was no authorization error
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
        final String action = pathSplits[1];
        if (resource.equalsIgnoreCase("entities") || resource.equalsIgnoreCase("instance")) {
            final String entityType = pathSplits.length > 2 ? pathSplits[2] : null;
            final String entityName = pathSplits.length > 3 ? pathSplits[3] : null;
            return new RequestParts(resource, action, entityName, entityType);
        } else {
            return new RequestParts(resource, action, null, null);
        }
    }

    private void tryProxy(UserGroupInformation authenticatedUGI,
                          String entityType, String entityName) throws IOException {
        if (entityType == null || entityName == null) {
            return;
        }

        try {
            EntityType type = EntityType.valueOf(entityType.toUpperCase());
            Entity entity = EntityUtil.getEntity(type, entityName);
            if (entity != null && entity.getACL() != null) {
                final String aclOwner = entity.getACL().getOwner();
                final String aclGroup = entity.getACL().getGroup();
                if (authorizationProvider.shouldProxy(
                        authenticatedUGI, aclOwner, aclGroup)) {
                    CurrentUser.proxy(aclOwner, aclGroup);
                }
            }
        } catch (FalconException ignore) {
            // do nothing
        }
    }

    private void sendError(HttpServletResponse httpResponse,
                           int errorCode, String errorMessage) throws IOException {
        LOG.error("Authorization failed : {}/{}", errorCode, errorMessage);
        if (!httpResponse.isCommitted()) { // handle authorization error
            httpResponse.setStatus(errorCode);
            httpResponse.setContentType(MediaType.APPLICATION_JSON);
            httpResponse.getOutputStream().print(getJsonResponse(errorCode, errorMessage));
        }
    }

    private String getJsonResponse(int errorCode, String errorMessage) throws IOException {
        try {
            JSONObject response = new JSONObject();
            response.put("errorCode", errorCode);
            response.put("errorMessage", errorMessage);
            response.put(Servlets.REQUEST_ID, Thread.currentThread().getName());
            return response.toString();
        } catch (JSONException e) {
            throw new IOException(e);
        }
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
            StringBuilder sb = new StringBuilder();
            sb.append("RequestParts{")
                    .append("resource='").append(resource).append("'")
                    .append(", action='").append(action).append("'");
            if (entityName != null) {
                sb.append(", entityName='").append(entityName).append("'");
            }
            if (entityType != null) {
                sb.append(", entityType='").append(entityType).append("'");
            }
            sb.append("}");
            return sb.toString();
        }
    }
}
