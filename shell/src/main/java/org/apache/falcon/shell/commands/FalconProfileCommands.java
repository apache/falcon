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

import org.apache.commons.io.IOUtils;
import org.apache.falcon.client.FalconCLIException;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.shell.support.util.OsUtils;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;


/**
 * To update falcon.url.
 */
@Component
public class FalconProfileCommands extends BaseFalconCommands{

    public static final String LIST_PROFILE = "listProfile";
    public static final String LIST_HELP = "lists the colos available to set in falcon.url";
    public static final String SET_PROFILE = "updateProfile";
    public static final String SET_HELP = "update falcon.url with new url";
    public static final String PROFILE = "profile";
    private static final String CLIENT_PROPERTIES = "/shell.properties";

    @CliCommand(value = LIST_PROFILE , help = LIST_HELP)
    public String listProfile() {
        StringBuilder stringBuilder = new StringBuilder();
        Properties prop = new Properties();
        InputStream inputStream = null;
        try {
            inputStream = BaseFalconCommands.class.getResourceAsStream(CLIENT_PROPERTIES);
            if (inputStream != null) {
                try {
                    prop.load(inputStream);
                } catch (IOException e) {
                    throw new FalconCLIException(e);
                }
            }
        } finally {
            IOUtils.closeQuietly(inputStream);
        }

        Enumeration e =  prop.propertyNames();
        while(e.hasMoreElements()){
            Object o = e.nextElement();
            stringBuilder.append(o.toString()).append(OsUtils.LINE_SEPARATOR);
        }
        return stringBuilder.toString();
    }

    @CliCommand(value = SET_PROFILE , help = SET_HELP)
    public String setProfile(@CliOption(key = {PROFILE}, mandatory = true, help = "key")
        @Nonnull final String key){
        setClientProperty(FALCON_URL_PROPERTY, PROFILE);
        return FALCON_URL_PROPERTY +"="+ PROFILE;
    }
}
