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

package org.apache.falcon.aspect;

/**
 * Message to be sent to the auditing system.
 */
public class AuditMessage {

    private final String user;               // who
    private final String remoteAddress;      // who
    private final String remoteHost;         // who
    private final String requestUrl;         // what
    private final String serverAddress;      // what
    private final String requestTimeISO9601; // when

    public AuditMessage(String user, String remoteAddress, String remoteHost,
                        String requestUrl, String serverAddress, String requestTimeISO9601) {
        this.user = user;
        this.remoteAddress = remoteAddress;
        this.remoteHost = remoteHost;
        this.requestUrl = requestUrl;
        this.serverAddress = serverAddress;
        this.requestTimeISO9601 = requestTimeISO9601;
    }

    public String getUser() {
        return user;
    }

    public String getRemoteAddress() {
        return remoteAddress;
    }

    public String getRemoteHost() {
        return remoteHost;
    }

    public String getRequestUrl() {
        return requestUrl;
    }

    public String getServerAddress() {
        return serverAddress;
    }

    public String getRequestTimeISO9601() {
        return requestTimeISO9601;
    }

    @Override
    public String toString() {
        return "Audit: "
                + user + "@" + remoteHost                                   // who
                + " performed " + requestUrl + " (" + serverAddress + ")"   // what
                + " at " + requestTimeISO9601;                              // when
    }
}
