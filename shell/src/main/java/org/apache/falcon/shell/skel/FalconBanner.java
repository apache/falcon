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


package org.apache.falcon.shell.skel;

import org.apache.falcon.shell.commands.BaseFalconCommands;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.support.DefaultBannerProvider;
import org.springframework.shell.support.util.OsUtils;
import org.springframework.stereotype.Component;

/**
 * The Class FalconBanner.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class FalconBanner extends DefaultBannerProvider {

    @Override
    public String getBanner() {
        return new StringBuilder()
                .append("=======================================").append(OsUtils.LINE_SEPARATOR)
                .append("*                                     *").append(OsUtils.LINE_SEPARATOR)
                .append("*            Falcon CLI               *").append(OsUtils.LINE_SEPARATOR)
                .append("*                                     *").append(OsUtils.LINE_SEPARATOR)
                .append("=======================================").append(OsUtils.LINE_SEPARATOR)
                .append("falcon.url:"+ BaseFalconCommands.getShellProperties().get("falcon.url"))
                .append(OsUtils.LINE_SEPARATOR)
                .toString();

    }

    @Override
    public String getWelcomeMessage() {
        return "Welcome to Falcon CLI";
    }

    @Override
    public String getVersion() {
        return getClass().getPackage().getImplementationVersion();
    }

    @Override
    public String getProviderName() {
        return "Falcon CLI";
    }
}
