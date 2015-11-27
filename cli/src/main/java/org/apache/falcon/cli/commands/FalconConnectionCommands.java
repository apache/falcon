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


package org.apache.falcon.cli.commands;


import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import javax.annotation.Nonnull;

/**
 * Connection Commands.
 */
public class FalconConnectionCommands extends BaseFalconCommands {

    @CliCommand(value = "get", help = "get properties")
    public String getParameter(@CliOption(key = {"", "key"}, mandatory = false, help = "<key>") final String key) {
        if (StringUtils.isBlank(key)) {
            return getClientProperties().toString();
        }
        return getClientProperties().getProperty(key);
    }

    @CliCommand(value = "set", help = "set properties")
    public void setParameter(@CliOption(key = {"", "keyval"}, mandatory = true, help = "<key-val>")
                             @Nonnull final String keyVal) {
        String[] kvArray = keyVal.split("=");
        String key = "";
        String value = "";
        if (kvArray.length > 0) {
            key = kvArray[0];
        }
        if (kvArray.length > 1) {
            value = kvArray[1];
        }
        setClientProperty(key, value);
    }
}
