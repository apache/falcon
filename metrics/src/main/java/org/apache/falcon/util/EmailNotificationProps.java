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

package org.apache.falcon.util;

/**
 * Email Notification Argument list.
 */
public enum EmailNotificationProps {
    SMTP_HOST("falcon.email.smtp.host", "SMTP host server name", true),
    SMTP_PORT("falcon.email.smtp.port", "SMTP port number", true),
    SMTP_FROM("falcon.email.from.address", "SMTP from address", true),
    SMTP_AUTH("falcon.email.smtp.auth", "SMTP authorization details", false),
    SMTP_USER("falcon.email.smtp.user", "SMTP username", false),
    SMTP_PASSWORD("falcon.email.smtp.password", "SMTP password", false);

    private final String name;
    private final String description;
    private final boolean isRequired;

    EmailNotificationProps(String name, String description, boolean isRequired) {
        this.name = name;
        this.description = description;
        this.isRequired = isRequired;
    }

    public String getName() {
        return this.name;
    }

    public String getDescription() {
        return this.description;
    }

    public boolean isRequired() {
        return this.isRequired;
    }

    public String toString() {
        return getName();
    }

}
