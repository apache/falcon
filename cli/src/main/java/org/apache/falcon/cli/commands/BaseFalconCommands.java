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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.cli.FalconCLIRuntimeException;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.client.FalconClient;
import org.springframework.shell.core.ExecutionProcessor;
import org.springframework.shell.event.ParseResult;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Common code for all falcon command classes.
 */
public class BaseFalconCommands implements ExecutionProcessor {
    private static final String FALCON_URL = "FALCON_URL";
    private static final String FALCON_URL_PROPERTY = "falcon.url";
    private static final String DO_AS = "DO_AS";
    private static final String DO_AS_PROPERTY = "do.as";
    private static final String CLIENT_PROPERTIES = "/client.properties";
    private static Properties clientProperties;
    private static Properties backupProperties = new Properties();
    private static FalconClient client;

    protected static Properties getClientProperties() {
        if (clientProperties == null) {
            InputStream inputStream = null;
            Properties prop = new Properties(System.getProperties());
            prop.putAll(backupProperties);
            try {
                inputStream = BaseFalconCommands.class.getResourceAsStream(CLIENT_PROPERTIES);
                if (inputStream != null) {
                    try {
                        prop.load(inputStream);
                    } catch (IOException e) {
                        throw new FalconCLIRuntimeException(e);
                    }
                }
            } finally {
                IOUtils.closeQuietly(inputStream);
            }
            String urlOverride = System.getenv(FALCON_URL);
            if (urlOverride != null) {
                prop.setProperty(FALCON_URL_PROPERTY, urlOverride);
            }
            if (prop.getProperty(FALCON_URL_PROPERTY) == null) {
                throw new FalconCLIRuntimeException("Failed to get falcon url from environment or client properties");
            }
            String doAsOverride = System.getenv(DO_AS);
            if (doAsOverride != null) {
                prop.setProperty(DO_AS_PROPERTY, doAsOverride);
            }
            clientProperties = prop;
            backupProperties.clear();
        }
        return clientProperties;
    }

    static void setClientProperty(String key, String value) {
        Properties props;
        try {
            props = getClientProperties();
        } catch (FalconCLIRuntimeException e) {
            props = backupProperties;
        }
        if (StringUtils.isBlank(value)) {
            props.remove(key);
        } else {
            props.setProperty(key, value);
        }
        // Re-load client in the next call
        client = null;
    }

    public static FalconClient getFalconClient() {
        if (client == null) {
            try {
                client = new FalconClient(getClientProperties().getProperty(FALCON_URL_PROPERTY),
                        getClientProperties());
            } catch (FalconCLIException e) {
                throw new FalconCLIRuntimeException(e.getMessage(), e.getCause());
            }
        }
        return client;
    }

    @Override
    public ParseResult beforeInvocation(ParseResult parseResult) {
        Object[] args = parseResult.getArguments();
        if (args != null) {
            boolean allEqual = true;
            for (int i = 1; i < args.length; i++) {
                allEqual &= args[0].equals(args[i]);
            }
            if (allEqual) {
                if (args[0] instanceof String) {
                    String[] split = ((String) args[0]).split("\\s+");
                    Object[] newArgs = new String[args.length];
                    System.arraycopy(split, 0, newArgs, 0, split.length);
                    parseResult = new ParseResult(parseResult.getMethod(), parseResult.getInstance(), newArgs);
                }
            }
        }
        return parseResult;
    }

    @Override
    public void afterReturningInvocation(ParseResult parseResult, Object o) {

    }

    @Override
    public void afterThrowingInvocation(ParseResult parseResult, Throwable throwable) {
    }
}
