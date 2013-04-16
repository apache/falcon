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

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.util.HashMap;

public class FalconSecurityConfiguration extends Configuration {

    private static final AppConfigurationEntry OS_SPECIFIC_LOGIN =
            new AppConfigurationEntry(SecurityConstants.OS_LOGIN_MODULE_NAME,
                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                    new HashMap<String, String>());

    private static final AppConfigurationEntry[] SIMPLE_CONF =
            new AppConfigurationEntry[]{OS_SPECIFIC_LOGIN};

    private final Configuration parent;

    public FalconSecurityConfiguration(Configuration parent) {
        this.parent = parent;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
        if (parent == null || appName.equals(SecurityConstants.FALCON_LOGIN)) {
            return SIMPLE_CONF;
        } else {
            return parent.getAppConfigurationEntry(appName);
        }
    }
}
