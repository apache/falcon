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

package org.apache.falcon.shell.commands;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.stereotype.Component;

import static org.apache.falcon.client.FalconCLIConstants.STACK_OPTION;
import static org.apache.falcon.client.FalconCLIConstants.STACK_OPTION_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.STATUS_OPT;
import static org.apache.falcon.client.FalconCLIConstants.STATUS_OPT_DESCRIPTION;
import static org.apache.falcon.client.FalconCLIConstants.VERSION_OPT;
import static org.apache.falcon.client.FalconCLIConstants.VERSION_OPT_DESCRIPTION;

/**
 * Admin commands.
 */
@Component
public class FalconAdminCommands extends BaseFalconCommands {
    public static final String ADMIN_PREFIX = "admin";
    public static final String ADMIN_COMMAND_PREFIX = ADMIN_PREFIX + " ";

    @CliCommand(value = {STATUS_OPT, ADMIN_COMMAND_PREFIX + STATUS_OPT}, help = STATUS_OPT_DESCRIPTION)
    public String status(
    ) {
        int status = getFalconClient().getStatus(getDoAs());
        String url = getShellProperties().getProperty(BaseFalconCommands.FALCON_URL_PROPERTY);
        if (status != 200) {
            throw new RuntimeException("Falcon server is not fully operational (on "
                    + url + "). "
                    + "Please check log files.");
        } else {
            return ("Falcon server is running (on " + url + ")");
        }
    }

    @CliCommand(value = {ADMIN_COMMAND_PREFIX + STACK_OPTION}, help = STACK_OPTION_DESCRIPTION)
    public String stack(
    ) {
        return getFalconClient().getThreadDump(getDoAs());
    }

    @CliCommand(value = {ADMIN_COMMAND_PREFIX + VERSION_OPT}, help = VERSION_OPT_DESCRIPTION)
    public String version(
    ) {
        return getFalconClient().getVersion(getDoAs());
    }
}
