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
package org.apache.falcon.tools;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.falcon.cliParser.CLIParser;
import org.apache.falcon.service.FalconJPAService;
import org.apache.falcon.util.BuildProperties;
import org.apache.falcon.util.StateStoreProperties;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Command Line utility for Table Creation, Update.
 */
public class FalconStateStoreDBCLI {
    public static final String HELP_CMD = "help";
    public static final String VERSION_CMD = "version";
    public static final String CREATE_CMD = "create";
    public static final String SQL_FILE_OPT = "sqlfile";
    public static final String RUN_OPT = "run";
    public static final String UPGRADE_CMD = "upgrade";

    // Represents whether DB instance exists or not.
    private boolean instanceExists;
    private static final String[] FALCON_HELP =
    {"Falcon DB initialization tool currently supports Derby DB/ Mysql/ PostgreSQL"};

    public static void main(String[] args) {
        new FalconStateStoreDBCLI().run(args);
    }

    public FalconStateStoreDBCLI() {
        instanceExists = false;
    }

    protected Options getOptions() {
        Option sqlfile = new Option(SQL_FILE_OPT, true,
                "Generate SQL script instead of creating/upgrading the DB schema");
        Option run = new Option(RUN_OPT, false, "Confirmation option regarding DB schema creation/upgrade");
        Options options = new Options();
        options.addOption(sqlfile);
        options.addOption(run);
        return options;
    }

    public synchronized int run(String[] args) {
        if (instanceExists) {
            throw new IllegalStateException("CLI instance already used");
        }
        instanceExists = true;

        CLIParser parser = new CLIParser("falcondb", FALCON_HELP);
        parser.addCommand(HELP_CMD, "", "Display usage for all commands or specified command", new Options(), false);
        parser.addCommand(VERSION_CMD, "", "Show Falcon DB version information", new Options(), false);
        parser.addCommand(CREATE_CMD, "", "Create Falcon DB schema", getOptions(), false);
        parser.addCommand(UPGRADE_CMD, "", "Upgrade Falcon DB schema", getOptions(), false);

        try {
            CLIParser.Command command = parser.parse(args);
            if (command.getName().equals(HELP_CMD)) {
                parser.showHelp();
            } else if (command.getName().equals(VERSION_CMD)) {
                showVersion();
            } else {
                if (!command.getCommandLine().hasOption(SQL_FILE_OPT)
                        && !command.getCommandLine().hasOption(RUN_OPT)) {
                    throw new Exception("'-sqlfile <FILE>' or '-run' options must be specified");
                }
                CommandLine commandLine = command.getCommandLine();
                String sqlFile = (commandLine.hasOption(SQL_FILE_OPT))
                        ? commandLine.getOptionValue(SQL_FILE_OPT)
                        : File.createTempFile("falcondb-", ".sql").getAbsolutePath();
                boolean run = commandLine.hasOption(RUN_OPT);
                if (command.getName().equals(CREATE_CMD)) {
                    createDB(sqlFile, run);
                } else if (command.getName().equals(UPGRADE_CMD)) {
                    upgradeDB(sqlFile, run);
                }
                System.out.println("The SQL commands have been written to: " + sqlFile);
                if (!run) {
                    System.out.println("WARN: The SQL commands have NOT been executed, you must use the '-run' option");
                }
            }
            return 0;
        } catch (ParseException ex) {
            System.err.println("Invalid sub-command: " + ex.getMessage());
            System.err.println();
            System.err.println(parser.shortHelp());
            return 1;
        } catch (Exception ex) {
            System.err.println();
            System.err.println("Error: " + ex.getMessage());
            System.err.println();
            System.err.println("Stack trace for the error was (for debug purposes):");
            System.err.println("--------------------------------------");
            ex.printStackTrace(System.err);
            System.err.println("--------------------------------------");
            System.err.println();
            return 1;
        }
    }

    private void upgradeDB(String sqlFile, boolean run) throws Exception {
        validateConnection();
        if (!checkDBExists()) {
            throw new Exception("Falcon DB doesn't exist");
        }
        String falconVersion = BuildProperties.get().getProperty("project.version");
        String dbVersion = getFalconDBVersion();
        if (dbVersion.compareTo(falconVersion) >= 0) {
            System.out.println("Falcon DB already upgraded to Falcon version '" + falconVersion + "'");
            return;
        }

        createUpgradeDB(sqlFile, run, false);
        upgradeFalconDBVersion(sqlFile, run, falconVersion);

        // any post upgrade tasks
        if (run) {
            System.out.println("Falcon DB has been upgraded to Falcon version '" + falconVersion + "'");
        }
    }


    private void upgradeFalconDBVersion(String sqlFile, boolean run, String version) throws Exception {
        String updateDBVersion = "update FALCON_DB_PROPS set data='" + version + "' where name='db.version'";
        PrintWriter writer = new PrintWriter(new FileWriter(sqlFile, true));
        writer.println();
        writer.println(updateDBVersion);
        writer.close();
        System.out.println("Upgrade db.version in FALCON_DB_PROPS table to " + version);
        if (run) {
            Connection conn = createConnection();
            Statement st = null;
            try {
                conn.setAutoCommit(true);
                st = conn.createStatement();
                st.executeUpdate(updateDBVersion);
                st.close();
            } catch (Exception ex) {
                throw new Exception("Could not upgrade db.version in FALCON_DB_PROPS table: " + ex.toString(), ex);
            } finally {
                closeStatement(st);
                conn.close();
            }
        }
        System.out.println("DONE");
    }

    private static final String GET_FALCON_DB_VERSION = "select data from FALCON_DB_PROPS where name = 'db.version'";

    private String getFalconDBVersion() throws Exception {
        String version;
        System.out.println("Get Falcon DB version");
        Connection conn = createConnection();
        Statement st = null;
        ResultSet rs = null;
        try {
            st = conn.createStatement();
            rs = st.executeQuery(GET_FALCON_DB_VERSION);
            if (rs.next()) {
                version = rs.getString(1);
            } else {
                throw new Exception("ERROR: Could not find Falcon DB 'db.version' in FALCON_DB_PROPS table");
            }
        } catch (Exception ex) {
            throw new Exception("ERROR: Could not query FALCON_DB_PROPS table: " + ex.toString(), ex);
        } finally {
            closeResultSet(rs);
            closeStatement(st);
            conn.close();
        }
        System.out.println("DONE");
        return version;
    }


    private Map<String, String> getJdbcConf() throws Exception {
        Map<String, String> jdbcConf = new HashMap<String, String>();
        jdbcConf.put("driver", StateStoreProperties.get().getProperty(FalconJPAService.DRIVER));
        String url = StateStoreProperties.get().getProperty(FalconJPAService.URL);
        jdbcConf.put("url", url);
        jdbcConf.put("user", StateStoreProperties.get().getProperty(FalconJPAService.USERNAME));
        jdbcConf.put("password", StateStoreProperties.get().getProperty(FalconJPAService.PASSWORD));
        String dbType = url.substring("jdbc:".length());
        if (dbType.indexOf(":") <= 0) {
            throw new RuntimeException("Invalid JDBC URL, missing vendor 'jdbc:[VENDOR]:...'");
        }
        dbType = dbType.substring(0, dbType.indexOf(":"));
        jdbcConf.put("dbtype", dbType);
        return jdbcConf;
    }

    private String[] createMappingToolArguments(String sqlFile, boolean create) throws Exception {
        Map<String, String> conf = getJdbcConf();
        List<String> args = new ArrayList<String>();
        args.add("-schemaAction");
        if (create) {
            args.add("add");
        } else {
            args.add("refresh");
        }
        args.add("-p");
        args.add("persistence.xml#falcon-" + conf.get("dbtype"));
        args.add("-connectionDriverName");
        args.add(conf.get("driver"));
        args.add("-connectionURL");
        args.add(conf.get("url"));
        args.add("-connectionUserName");
        args.add(conf.get("user"));
        args.add("-connectionPassword");
        args.add(conf.get("password"));
        if (sqlFile != null) {
            args.add("-sqlFile");
            args.add(sqlFile);
        }
        args.add("-indexes");
        args.add("true");
        args.add("org.apache.falcon.persistence.EntityBean");
        args.add("org.apache.falcon.persistence.InstanceBean");
        args.add("org.apache.falcon.persistence.PendingInstanceBean");
        args.add("org.apache.falcon.persistence.MonitoredEntityBean");
        args.add("org.apache.falcon.persistence.EntitySLAAlertBean");
        args.add("org.apache.falcon.persistence.BacklogMetricBean");
        args.add("org.apache.falcon.persistence.ExtensionBean");
        args.add("org.apache.falcon.persistence.ExtensionJobsBean");
        args.add("org.apache.falcon.persistence.ProcessInstanceInfoBean");
        return args.toArray(new String[args.size()]);
    }

    private void createDB(String sqlFile, boolean run) throws Exception {
        validateConnection();
        if (checkDBExists()) {
            return;
        }

        verifyFalconPropsTable(false);
        createUpgradeDB(sqlFile, run, true);
        createFalconPropsTable(sqlFile, run, BuildProperties.get().getProperty("project.version"));
        if (run) {
            System.out.println("Falcon DB has been created for Falcon version '"
                    + BuildProperties.get().getProperty("project.version") + "'");
        }
    }

    private static final String CREATE_FALCON_DB_PROPS =
            "create table FALCON_DB_PROPS (name varchar(100), data varchar(100))";

    private void createFalconPropsTable(String sqlFile, boolean run, String version) throws Exception {
        String insertDbVersion = "insert into FALCON_DB_PROPS (name, data) values ('db.version', '" + version + "')";

        PrintWriter writer = new PrintWriter(new FileWriter(sqlFile, true));
        writer.println();
        writer.println(CREATE_FALCON_DB_PROPS);
        writer.println(insertDbVersion);
        writer.close();
        System.out.println("Create FALCON_DB_PROPS table");
        if (run) {
            Connection conn = createConnection();
            Statement st = null;
            try {
                conn.setAutoCommit(true);
                st = conn.createStatement();
                st.executeUpdate(CREATE_FALCON_DB_PROPS);
                st.executeUpdate(insertDbVersion);
                st.close();
            } catch (Exception ex) {
                closeStatement(st);
                throw new Exception("Could not create FALCON_DB_PROPS table: " + ex.toString(), ex);
            } finally {
                conn.close();
            }
        }
        System.out.println("DONE");
    }

    private static final String FALCON_DB_PROPS_EXISTS = "select count(*) from FALCON_DB_PROPS";

    private boolean verifyFalconPropsTable(boolean exists) throws Exception {
        System.out.println((exists) ? "Check FALCON_DB_PROPS table exists"
                : "Checking FALCON_DB_PROPS table does not exist");
        boolean tableExists;
        Connection conn = createConnection();
        Statement st = null;
        ResultSet rs = null;
        try {
            st = conn.createStatement();
            rs = st.executeQuery(FALCON_DB_PROPS_EXISTS);
            rs.next();
            tableExists = true;
        } catch (Exception ex) {
            tableExists = false;
        } finally {
            closeResultSet(rs);
            closeStatement(st);
            conn.close();
        }
        if (tableExists != exists) {
            throw new Exception("FALCON_DB_PROPS_TABLE table " + ((exists) ? "does not exist" : "exists"));
        }
        System.out.println("DONE");
        return tableExists;
    }

    private void closeResultSet(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (Exception e) {
            System.out.println("Unable to close ResultSet " + rs);
        }
    }

    private void closeStatement(Statement st) throws Exception {
        try {
            if (st != null) {
                st.close();
            }
        } catch (Exception e) {
            System.out.println("Unable to close SQL Statement " + st);
            throw new Exception(e);
        }
    }

    private Connection createConnection() throws Exception {
        Map<String, String> conf = getJdbcConf();
        Class.forName(conf.get("driver")).newInstance();
        return DriverManager.getConnection(conf.get("url"), conf.get("user"), conf.get("password"));
    }

    private void validateConnection() throws Exception {
        System.out.println("Validating DB Connection");
        try {
            createConnection().close();
            System.out.println("DONE");
        } catch (Exception ex) {
            throw new Exception("Could not connect to the database: " + ex.toString(), ex);
        }
    }

    private static final String ENTITY_STATUS_QUERY =
            "select count(*) from ENTITIES where current_state IN ('RUNNING', 'SUSPENDED')";
    private static final String INSTANCE_STATUS_QUERY =
            "select count(*) from INSTANCES where current_state IN ('RUNNING', 'SUSPENDED')";

    private boolean checkDBExists() throws Exception {
        boolean schemaExists;
        Connection conn = createConnection();
        ResultSet rs =  null;
        Statement st = null;
        try {
            st = conn.createStatement();
            rs = st.executeQuery(ENTITY_STATUS_QUERY);
            rs.next();
            schemaExists = true;
        } catch (Exception ex) {
            schemaExists = false;
        } finally {
            closeResultSet(rs);
            closeStatement(st);
            conn.close();
        }
        System.out.println("DB schema " + ((schemaExists) ? "exists" : "does not exist"));
        return schemaExists;
    }

    private void createUpgradeDB(String sqlFile, boolean run, boolean create) throws Exception {
        System.out.println((create) ? "Create SQL schema" : "Upgrade SQL schema");
        String[] args = createMappingToolArguments(sqlFile, create);
        org.apache.openjpa.jdbc.meta.MappingTool.main(args);
        if (run) {
            args = createMappingToolArguments(null, create);
            org.apache.openjpa.jdbc.meta.MappingTool.main(args);
        }
        System.out.println("DONE");
    }

    private void showVersion() throws Exception {
        System.out.println("Falcon Server version: "
                + BuildProperties.get().getProperty("project.version"));
        validateConnection();
        if (!checkDBExists()) {
            throw new Exception("Falcon DB doesn't exist");
        }
        try {
            verifyFalconPropsTable(true);
        } catch (Exception ex) {
            throw new Exception("ERROR: It seems this Falcon DB was never upgraded with the 'falcondb' tool");
        }
        showFalconPropsInfo();
    }

    private static final String GET_FALCON_PROPS_INFO = "select name, data from FALCON_DB_PROPS order by name";

    private void showFalconPropsInfo() throws Exception {
        Connection conn = createConnection();
        Statement st = null;
        ResultSet rs = null;
        try {
            System.out.println("Falcon DB Version Information");
            System.out.println("--------------------------------------");
            st = conn.createStatement();
            rs = st.executeQuery(GET_FALCON_PROPS_INFO);
            while (rs.next()) {
                System.out.println(rs.getString(1) + ": " + rs.getString(2));
            }
            System.out.println("--------------------------------------");
        } catch (Exception ex) {
            throw new Exception("ERROR querying FALCON_DB_PROPS table: " + ex.toString(), ex);
        } finally {
            closeResultSet(rs);
            closeStatement(st);
            conn.close();
        }
    }

}
