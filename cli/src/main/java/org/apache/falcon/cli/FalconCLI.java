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

package org.apache.falcon.cli;

import com.sun.jersey.api.client.ClientHandlerException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.client.FalconCLIConstants;
import org.apache.falcon.cliParser.CLIParser;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.client.FalconClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import static org.apache.falcon.client.FalconCLIConstants.FALCON_URL;


/**
 * Falcon Command Line Interface - wraps the RESTful API.
 */
public class FalconCLI {

    public static final AtomicReference<PrintStream> ERR = new AtomicReference<PrintStream>(System.err);
    public static final AtomicReference<PrintStream> OUT = new AtomicReference<PrintStream>(System.out);

    private final Properties clientProperties;

    public FalconCLI() throws Exception {
        clientProperties = getClientProperties();
    }

    /**
     * Entry point for the Falcon CLI when invoked from the command line. Upon
     * completion this method exits the JVM with '0' (success) or '-1'
     * (failure).
     *
     * @param args options and arguments for the Falcon CLI.
     */
    public static void main(final String[] args) throws Exception {
        System.exit(new FalconCLI().run(args));
    }

    // TODO help and headers
    private static final String[] FALCON_HELP = { "the env variable '" + FALCON_URL
            + "' is used as default value for the '-"
            + FalconCLIConstants.URL_OPTION + "' option",
                                                  "custom headers for Falcon web services can be specified using '-D"
                                                          + FalconClient.WS_HEADER_PREFIX + "NAME=VALUE'", };
    /**
     * Run a CLI programmatically.
     * <p/>
     * It does not exit the JVM.
     * <p/>
     * A CLI instance can be used only once.
     *
     * @param args options and arguments for the Oozie CLI.
     * @return '0' (success), '-1' (failure).
     */
    public synchronized int run(final String[] args) throws Exception {

        CLIParser parser = new CLIParser("falcon", FALCON_HELP);

        FalconAdminCLI adminCLI = new FalconAdminCLI();
        FalconEntityCLI entityCLI = new FalconEntityCLI();
        FalconInstanceCLI instanceCLI = new FalconInstanceCLI();
        FalconMetadataCLI metadataCLI = new FalconMetadataCLI();
        FalconExtensionCLI extensionCLI = new FalconExtensionCLI();

        parser.addCommand(FalconCLIConstants.ADMIN_CMD, "", "admin operations", adminCLI.createAdminOptions(), true);
        parser.addCommand(FalconCLIConstants.HELP_CMD, "", "display usage", new Options(), false);
        parser.addCommand(FalconCLIConstants.ENTITY_CMD, "",
                "Entity operations like submit, suspend, resume, delete, status, definition, submitAndSchedule",
                entityCLI.createEntityOptions(), false);
        parser.addCommand(FalconCLIConstants.INSTANCE_CMD, "",
                "Process instances operations like running, status, kill, suspend, resume, rerun, logs, search",
                instanceCLI.createInstanceOptions(), false);
        parser.addCommand(FalconCLIConstants.METADATA_CMD, "", "Metadata operations like list, relations",
                metadataCLI.createMetadataOptions(), true);
        parser.addCommand(FalconCLIConstants.EXTENSION_CMD, "",
                "Extension operations like enumerate, definition, describe, list, instances, "
                        + "submit, submitAndSchedule, schedule, suspend, resume, delete, update, validate,unregister"
                        + ",detail,register",
                extensionCLI.createExtensionOptions(), true);
        parser.addCommand(FalconCLIConstants.VERSION_OPT, "", "show client version", new Options(), false);

        try {
            CLIParser.Command command = parser.parse(args);
            int exitValue = 0;
            if (command.getName().equals(FalconCLIConstants.HELP_CMD)) {
                parser.showHelp();
            } else {
                CommandLine commandLine = command.getCommandLine();
                String falconUrl = getFalconEndpoint(commandLine);
                FalconClient client = new FalconClient(falconUrl, clientProperties);

                setDebugMode(client, commandLine.hasOption(FalconCLIConstants.DEBUG_OPTION));
                if (command.getName().equals(FalconCLIConstants.ADMIN_CMD)) {
                    exitValue = adminCLI.adminCommand(commandLine, client, falconUrl);
                } else if (command.getName().equals(FalconCLIConstants.ENTITY_CMD)) {
                    entityCLI.entityCommand(commandLine, client);
                } else if (command.getName().equals(FalconCLIConstants.INSTANCE_CMD)) {
                    instanceCLI.instanceCommand(commandLine, client);
                } else if (command.getName().equals(FalconCLIConstants.METADATA_CMD)) {
                    metadataCLI.metadataCommand(commandLine, client);
                } else if (command.getName().equals(FalconCLIConstants.EXTENSION_CMD)) {
                    extensionCLI.extensionCommand(commandLine, client);
                }
            }
            return exitValue;
        } catch (ParseException ex) {
            ERR.get().println("Invalid sub-command: " + ex.getMessage());
            ERR.get().println();
            ERR.get().println(parser.shortHelp());
            ERR.get().println("Stacktrace:");
            ex.printStackTrace();
            return -1;
        } catch (ClientHandlerException ex) {
            ERR.get().print("Unable to connect to Falcon server, "
                    + "please check if the URL is correct and Falcon server is up and running\n");
            ERR.get().println("Stacktrace:");
            ex.printStackTrace();
            return -1;
        } catch (FalconCLIException e) {
            ERR.get().println("ERROR: " + e.getMessage());
            return -1;
        } catch (Exception ex) {
            ERR.get().println("Stacktrace:");
            ex.printStackTrace();
            return -1;
        }
    }

    protected Integer parseIntegerInput(String optionValue, Integer defaultVal, String optionName) {
        Integer integer = defaultVal;
        if (optionValue != null) {
            try {
                return Integer.parseInt(optionValue);
            } catch (NumberFormatException e) {
                throw new FalconCLIException("Input value provided for queryParam \""+ optionName
                        +"\" is not a valid Integer");
            }
        }
        return integer;
    }

    protected String getColo(String colo) throws IOException {
        if (colo == null) {
            Properties prop = getClientProperties();
            colo = prop.getProperty(FalconCLIConstants.CURRENT_COLO, "*");
        }
        return colo;
    }

    protected String getFalconEndpoint(CommandLine commandLine) throws IOException {
        String url = commandLine.getOptionValue(FalconCLIConstants.URL_OPTION);
        if (url == null) {
            url = System.getenv(FALCON_URL);
        }
        if (url == null) {
            if (clientProperties.containsKey("falcon.url")) {
                url = clientProperties.getProperty("falcon.url");
            }
        }
        if (url == null) {
            throw new FalconCLIException("Failed to get falcon url from cmdline, or environment or client properties");
        }

        return url;
    }

    private void setDebugMode(FalconClient client, boolean debugOpt) {
        String debug = System.getenv(FalconCLIConstants.ENV_FALCON_DEBUG);
        if (debugOpt) {  // CLI argument "-debug" used
            client.setDebugMode(true);
        } else if (StringUtils.isNotBlank(debug)) {
            System.out.println(FalconCLIConstants.ENV_FALCON_DEBUG + ": " + debug);
            if (debug.trim().toLowerCase().equals("true")) {
                client.setDebugMode(true);
            }
        }
    }

    private Properties getClientProperties() throws IOException {
        InputStream inputStream = null;
        try {
            inputStream = FalconCLI.class.getResourceAsStream(FalconCLIConstants.CLIENT_PROPERTIES);
            Properties prop = new Properties();
            if (inputStream != null) {
                prop.load(inputStream);
            }
            return prop;
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
    }
}
