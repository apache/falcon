/*
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

import javax.security.auth.Subject;

public final class CurrentUser {

    private static Logger LOG = Logger.getLogger(CurrentUser.class);

    private static final CurrentUser instance = new CurrentUser();

    public static CurrentUser get() {
        return instance;
    }

    private final ThreadLocal<Subject> currentSubject =
            new ThreadLocal<Subject>();

    public static void authenticate(String user) {
        if (user == null || user.isEmpty()) {
            throw new IllegalStateException
                    ("Bad user name sent for authentication");
        }
        if (user.equals(getUserInternal())) return;

        Subject subject = new Subject();
        subject.getPrincipals().add(new FalconPrincipal(user));
        LOG.info("Logging in " + user);
        instance.currentSubject.set(subject);
    }

    public static Subject getSubject() {
        return instance.currentSubject.get();
    }

    public static String getUser() {
        String user = getUserInternal();
        if (user == null) {
            throw new IllegalStateException("No user logged into the system");
        } else {
            return user;
        }
    }

    private static String getUserInternal() {
        Subject subject = getSubject();
        if (subject == null) {
            return null;
        } else {
            for(FalconPrincipal principal: subject.
                    getPrincipals(FalconPrincipal.class)) {
                return principal.getName();
            }
            return null;
        }
    }
}
