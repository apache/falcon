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
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.client.FalconClient;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.resource.EntityList;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesSummaryResult;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Falcon Command Line Interface - wraps the RESTful API.
 */
public class FalconCLI {

    public static final AtomicReference<PrintStream> ERR = new AtomicReference<PrintStream>(System.err);
    public static final AtomicReference<PrintStream> OUT = new AtomicReference<PrintStream>(System.out);

    public static final String ENV_FALCON_DEBUG = "FALCON_DEBUG";
    public static final String DEBUG_OPTION = "debug";
    public static final String URL_OPTION = "url";
    private static final String FALCON_URL = "FALCON_URL";

    public static final String ADMIN_CMD = "admin";
    public static final String HELP_CMD = "help";
    public static final String METADATA_CMD = "metadata";
    public static final String ENTITY_CMD = "entity";
    public static final String INSTANCE_CMD = "instance";
    public static final String RECIPE_CMD = "recipe";

    public static final String TYPE_OPT = "type";
    public static final String COLO_OPT = "colo";
    public static final String CLUSTER_OPT = "cluster";
    public static final String ENTITY_NAME_OPT = "name";
    public static final String FILE_PATH_OPT = "file";
    public static final String VERSION_OPT = "version";
    public static final String SCHEDULE_OPT = "schedule";
    public static final String SUSPEND_OPT = "suspend";
    public static final String RESUME_OPT = "resume";
    public static final String STATUS_OPT = "status";
    public static final String SUMMARY_OPT = "summary";
    public static final String DEPENDENCY_OPT = "dependency";
    public static final String LIST_OPT = "list";
    public static final String SKIPDRYRUN_OPT = "skipDryRun";
    public static final String FILTER_BY_OPT = "filterBy";
    public static final String ORDER_BY_OPT = "orderBy";
    public static final String SORT_ORDER_OPT = "sortOrder";
    public static final String OFFSET_OPT = "offset";
    public static final String NUM_RESULTS_OPT = "numResults";
    public static final String START_OPT = "start";
    public static final String END_OPT = "end";
    public static final String CURRENT_COLO = "current.colo";
    public static final String CLIENT_PROPERTIES = "/client.properties";
    public static final String DO_AS_OPT = "doAs";

    private final Properties clientProperties;

    public FalconCLI() throws Exception {
        clientProperties = getClientProperties();
    }

    /**
     * Recipe operation enum.
     */
    public enum RecipeOperation {
        HDFS_REPLICATION,
        HIVE_DISASTER_RECOVERY
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
                                                          + "' is used as default value for the '-" + URL_OPTION
                                                          + "' option",
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
        FalconRecipeCLI recipeCLI = new FalconRecipeCLI();

        parser.addCommand(ADMIN_CMD, "", "admin operations", adminCLI.createAdminOptions(), true);
        parser.addCommand(HELP_CMD, "", "display usage", new Options(), false);
        parser.addCommand(ENTITY_CMD, "",
                "Entity operations like submit, suspend, resume, delete, status, definition, submitAndSchedule",
                entityCLI.createEntityOptions(), false);
        parser.addCommand(INSTANCE_CMD, "",
                "Process instances operations like running, status, kill, suspend, resume, rerun, logs",
                instanceCLI.createInstanceOptions(), false);
        parser.addCommand(METADATA_CMD, "", "Metadata operations like list, relations",
                metadataCLI.createMetadataOptions(), true);
        parser.addCommand(RECIPE_CMD, "", "recipe operations", recipeCLI.createRecipeOptions(), true);
        parser.addCommand(VERSION_OPT, "", "show client version", new Options(), false);

        try {
            CLIParser.Command command = parser.parse(args);
            int exitValue = 0;
            if (command.getName().equals(HELP_CMD)) {
                parser.showHelp();
            } else {
                CommandLine commandLine = command.getCommandLine();
                String falconUrl = getFalconEndpoint(commandLine);
                FalconClient client = new FalconClient(falconUrl, clientProperties);

                setDebugMode(client, commandLine.hasOption(DEBUG_OPTION));
                if (command.getName().equals(ADMIN_CMD)) {
                    exitValue = adminCLI.adminCommand(commandLine, client, falconUrl);
                } else if (command.getName().equals(ENTITY_CMD)) {
                    entityCLI.entityCommand(commandLine, client);
                } else if (command.getName().equals(INSTANCE_CMD)) {
                    instanceCLI.instanceCommand(commandLine, client);
                } else if (command.getName().equals(METADATA_CMD)) {
                    metadataCLI.metadataCommand(commandLine, client);
                } else if (command.getName().equals(RECIPE_CMD)) {
                    recipeCLI.recipeCommand(commandLine, client);
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

    protected Integer parseIntegerInput(String optionValue, Integer defaultVal, String optionName)
        throws FalconCLIException {
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

    protected void validateEntityTypeForSummary(String type) throws FalconCLIException {
        EntityType entityType = EntityType.getEnum(type);
        if (!entityType.isSchedulable()) {
            throw new FalconCLIException("Invalid entity type " + entityType
                    + " for EntitySummary API. Valid options are feed or process");
        }
    }

    protected void validateNotEmpty(String paramVal, String paramName) throws FalconCLIException {
        if (StringUtils.isBlank(paramVal)) {
            throw new FalconCLIException("Missing argument : " + paramName);
        }
    }

    protected void validateSortOrder(String sortOrder) throws FalconCLIException {
        if (!StringUtils.isBlank(sortOrder)) {
            if (!sortOrder.equalsIgnoreCase("asc") && !sortOrder.equalsIgnoreCase("desc")) {
                throw new FalconCLIException("Value for param sortOrder should be \"asc\" or \"desc\". It is  : "
                        + sortOrder);
            }
        }
    }

    protected String getColo(String colo) throws FalconCLIException, IOException {
        if (colo == null) {
            Properties prop = getClientProperties();
            colo = prop.getProperty(CURRENT_COLO, "*");
        }
        return colo;
    }

    protected void validateFilterBy(String filterBy, String filterType) throws FalconCLIException {
        if (StringUtils.isEmpty(filterBy)) {
            return;
        }
        String[] filterSplits = filterBy.split(",");
        for (String s : filterSplits) {
            String[] tempKeyVal = s.split(":", 2);
            try {
                if (filterType.equals("entity")) {
                    EntityList.EntityFilterByFields.valueOf(tempKeyVal[0].toUpperCase());
                } else if (filterType.equals("instance")) {
                    InstancesResult.InstanceFilterFields.valueOf(tempKeyVal[0].toUpperCase());
                }else if (filterType.equals("summary")) {
                    InstancesSummaryResult.InstanceSummaryFilterFields.valueOf(tempKeyVal[0].toUpperCase());
                } else {
                    throw new IllegalArgumentException("Invalid API call: filterType is not valid");
                }
            } catch (IllegalArgumentException ie) {
                throw new FalconCLIException("Invalid filterBy argument : " + tempKeyVal[0] + " in : " + s);
            }
        }
    }

    protected void validateOrderBy(String orderBy, String action) throws FalconCLIException {
        if (StringUtils.isBlank(orderBy)) {
            return;
        }
        if (action.equals("instance")) {
            if (Arrays.asList(new String[]{"status", "cluster", "starttime", "endtime"})
                .contains(orderBy.toLowerCase())) {
                return;
            }
        } else if (action.equals("entity")) {
            if (Arrays.asList(new String[] {"type", "name"}).contains(orderBy.toLowerCase())) {
                return;
            }
        } else if (action.equals("summary")) {
            if (Arrays.asList(new String[]{"cluster"})
                    .contains(orderBy.toLowerCase())) {
                return;
            }
        }
        throw new FalconCLIException("Invalid orderBy argument : " + orderBy);
    }

    protected String getFalconEndpoint(CommandLine commandLine) throws FalconCLIException, IOException {
        String url = commandLine.getOptionValue(URL_OPTION);
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
        String debug = System.getenv(ENV_FALCON_DEBUG);
        if (debugOpt) {  // CLI argument "-debug" used
            client.setDebugMode(true);
        } else if (StringUtils.isNotBlank(debug)) {
            System.out.println(ENV_FALCON_DEBUG + ": " + debug);
            if (debug.trim().toLowerCase().equals("true")) {
                client.setDebugMode(true);
            }
        }
    }

    private Properties getClientProperties() throws IOException {
        InputStream inputStream = null;
        try {
            inputStream = FalconCLI.class.getResourceAsStream(CLIENT_PROPERTIES);
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
