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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

        String pathInfo = httpRequest.getPathInfo();
        String[] paths = getResourcesAndActions(pathInfo);
        final String resource = paths[0];
        final String action = paths[1];
        final String entityType = paths.length > 2 ? paths[2] : null;
        final String entityName = paths.length > 3 ? paths[3] : null;

        if (isAuthorizationEnabled) {
            LOG.info("Authorizing user={} against resource={}, action={}, entity name={}, "
                + "entity type={}", CurrentUser.getUser(), resource, action, entityName, entityType);
            authorizationProvider.authorizeResource(resource, action,
                    entityType, entityName, CurrentUser.getProxyUgi());
        }

        filterChain.doFilter(request, response);
    }

    private static String[] getResourcesAndActions(String pathInfo) {
        List<String> splits = new ArrayList<String>();
        final String[] pathSplits = pathInfo.substring(1).split("/");
        final String resource = pathSplits[0];
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

        return splits.toArray(new String[splits.size()]);
    }

    @Override
    public void destroy() {
        authorizationProvider = null;
    }
}
