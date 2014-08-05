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

import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.ConcurrentHashMap;

/** Class for obtaining authorization token. */
public final class FalconAuthorizationToken {
    private static final String AUTH_URL = "api/options";
    private static final KerberosAuthenticator AUTHENTICATOR = new KerberosAuthenticator();
    private static final FalconAuthorizationToken INSTANCE = new FalconAuthorizationToken();
    private static final Logger LOGGER = Logger.getLogger(FalconAuthorizationToken.class);

    // Use a hashmap so that we can cache the tokens.
    private final ConcurrentHashMap<String, AuthenticatedURL.Token> tokens =
        new ConcurrentHashMap<String, AuthenticatedURL.Token>();

    private FalconAuthorizationToken() {
    }

    public static void authenticate(String user, String protocol, String host,
                                    int port)
        throws IOException, AuthenticationException {
        URL url = new URL(String.format("%s://%s:%d/%s", protocol, host, port,
            AUTH_URL + "?" + PseudoAuthenticator.USER_NAME + "=" + user));
        LOGGER.info("Authorize using url: " + url.toString());
        AuthenticatedURL.Token currentToken = new AuthenticatedURL.Token();

        /*using KerberosAuthenticator which falls back to PsuedoAuthenticator
        instead of passing authentication type from the command line - bad factory*/
        new AuthenticatedURL(AUTHENTICATOR).openConnection(url, currentToken);
        String key = getKey(user, protocol, host, port);

        // initialize a hash map if its null.
        LOGGER.info("Authorization Token: " + currentToken.toString());
        INSTANCE.tokens.put(key, currentToken);
    }

    public static AuthenticatedURL.Token getToken(String user, String protocol, String host,
                                                  int port, boolean overWrite)
        throws IOException, AuthenticationException {
        String key = getKey(user, protocol, host, port);

        /*if the tokens are null or if token is not found then we will go ahead and authenticate
        or if we are asked to overwrite*/
        if (!INSTANCE.tokens.containsKey(key) || overWrite) {
            authenticate(user, protocol, host, port);
        }
        return INSTANCE.tokens.get(key);
    }

    public static AuthenticatedURL.Token getToken(String user, String protocol, String host,
                                                  int port)
        throws IOException, AuthenticationException {
        return getToken(user, protocol, host, port, false);
    }

    /*spnego token will be unique to the user and uri its being requested for.
    Hence the key of the hash map is the combination of user, protocol, host and port.*/
    private static String getKey(String user, String protocol, String host, int port) {
        return String.format("%s-%s-%s-%d", user, protocol, host, port);
    }
}
