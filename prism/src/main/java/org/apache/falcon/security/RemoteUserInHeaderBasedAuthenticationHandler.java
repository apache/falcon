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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * This class is for backwards compatibility with clients who send Remote-User in the request
 * header else delegates to PseudoAuthenticationHandler.
 *
 * This is a temporary solution until Falcon clients (0.3) are deprecated.
 */
public class RemoteUserInHeaderBasedAuthenticationHandler extends PseudoAuthenticationHandler {

    @Override
    public AuthenticationToken authenticate(HttpServletRequest request, HttpServletResponse response)
        throws IOException, AuthenticationException {

        String userName = request.getHeader("Remote-User");
        if (StringUtils.isEmpty(userName)) {
            return super.authenticate(request, response);
        } else {
            return new AuthenticationToken(userName, userName, getType());
        }
    }
}
