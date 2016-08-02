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

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.hsqldb.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create a simple hsqldb server and schema to use for testing.
 */
public final class HsqldbTestUtils {

    public static final Logger LOG = LoggerFactory.getLogger(HsqldbTestUtils.class);

    // singleton server instance.
    private static Server server;

    private static final String IN_MEM = "mem:/";

    private static boolean inMemoryDB = IN_MEM.equals(getServerHost());

    private static String dbLocation;

    private static String dbBaseDir;

    private HsqldbTestUtils() {}

    public static String getServerHost() {
        String host = System.getProperty("hsql.server.host", IN_MEM);
        host = "localhost";
        if (!host.endsWith("/")) { host += "/"; }
        return host;
    }

    // Database name can be altered too
    private static final String DATABASE_NAME = System.getProperty("hsql.database.name",  "db1");
    private static final String CUSTOMER_TABLE_NAME = "CUSTOMER";
    private static final String DB_URL = "jdbc:hsqldb:hsql://" + getServerHost() + DATABASE_NAME;
    private static final String DRIVER_CLASS = "org.hsqldb.jdbcDriver";

    public static String getUrl() {
        return DB_URL;
    }

    public static String getDatabaseName() {
        return DATABASE_NAME;
    }

    /**
     * start the server.
     */
    public static void start() {
        if (null == server) {
            LOG.info("Starting new hsqldb server; database=" + DATABASE_NAME);
            String tmpDir = System.getProperty("test.build.data", "/tmp/");
            dbBaseDir = tmpDir + "/falcon";
            dbLocation =  dbBaseDir + "/testdb.file";
            deleteHSQLDir();
            server = new Server();
            server.setDatabaseName(0, DATABASE_NAME);
            server.putPropertiesFromString("database.0=" + dbLocation + ";no_system_exit=true;");
            server.start();
            LOG.info("Started server with url=" + DB_URL);
        }
    }

    public static void stop() {
        if (null != server) {
            server.stop();
        }
        deleteHSQLDir();
    }

    private static void deleteHSQLDir() {
        try {
            FileUtils.deleteDirectory(new File(dbBaseDir));
            LOG.info("Ok, Deleted HSQL temp dir at {}", dbBaseDir);
        } catch(IOException ioe) {
            LOG.info("Error deleting HSQL temp dir at {}", dbBaseDir);
        }
    }

    public static void tearDown() throws SQLException {
        dropExistingSchema();
        stop();
    }

    public static void createSqoopUser(String user, String password) throws Exception {
        Connection connection = null;
        Statement st = null;

        LOG.info("Creating user {} with password {}", user, password);

        try {
            connection = getConnectionSystem();
            st = connection.createStatement();
            boolean result = st.execute("CREATE USER " + user + " PASSWORD " + password + " ADMIN");
            LOG.info("CREATE USER returned {}", result);
            connection.commit();
        } finally {
            if (null != st) {
                st.close();
            }

            if (null != connection) {
                connection.close();
            }
        }
    }

    public static void changeSAPassword(String passwd) throws Exception {
        Connection connection = null;
        Statement st = null;

        LOG.info("Changing password for SA");
        try {
            connection = getConnectionSystem();

            st = connection.createStatement();
            boolean result = st.execute("SET PASSWORD \"" + passwd + "\"");
            LOG.info("Change PASSWORD for SA returned {}", result);
            connection.commit();
        } finally {
            if (null != st) {
                st.close();
            }

            if (null != connection) {
                connection.close();
            }
        }
    }
    private static Connection getConnectionSystem() throws SQLException {
        return getConnection("SA", "");
    }

    private static Connection getConnection() throws SQLException {
        return getConnection("sqoop_user", "sqoop");
    }
    private static Connection getConnection(String user, String password) throws SQLException {
        try {
            Class.forName(DRIVER_CLASS);
        } catch (ClassNotFoundException cnfe) {
            LOG.error("Could not get connection; driver class not found: "
                    + DRIVER_CLASS);
            return null;
        }
        Connection connection = DriverManager.getConnection(DB_URL, user, password);
        connection.setAutoCommit(false);
        LOG.info("Connection for user {} password {} is open {}", user, password, !connection.isClosed());
        return connection;
    }

    /**
     * Returns database URL for the server instance.
     * @return String representation of DB_URL
     */
    public static String getDbUrl() {
        return DB_URL;
    }

    public static int getNumberOfRows() throws SQLException {
        Connection connection = null;
        Statement st = null;
        try {
            connection = getConnection();

            st = connection.createStatement();
            ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + CUSTOMER_TABLE_NAME);
            int rowCount = 0;
            if (rs.next()) {
                rowCount = rs.getInt(1);
            }
            return rowCount;
        } finally {
            if (null != st) {
                st.close();
            }

            if (null != connection) {
                connection.close();
            }
        }
    }

    public static void createAndPopulateCustomerTable() throws SQLException, ClassNotFoundException {

        LOG.info("createAndPopulateCustomerTable");
        Connection connection = null;
        Statement st = null;
        try {
            connection = getConnection();

            st = connection.createStatement();
            boolean r = st.execute("DROP TABLE " + CUSTOMER_TABLE_NAME + " IF EXISTS");
            r = st.execute("CREATE TABLE " + CUSTOMER_TABLE_NAME + "(id INT NOT NULL PRIMARY KEY, name VARCHAR(64))");
            LOG.info("CREATE TABLE returned {}", r);

            r=st.execute("INSERT INTO " + CUSTOMER_TABLE_NAME + " VALUES(1, 'Apple')");
            LOG.info("INSERT INTO returned {}", r);
            r=st.execute("INSERT INTO " + CUSTOMER_TABLE_NAME + " VALUES(2, 'Blackberry')");
            LOG.info("INSERT INTO returned {}", r);
            r=st.execute("INSERT INTO " + CUSTOMER_TABLE_NAME + " VALUES(3, 'Caterpillar')");
            LOG.info("INSERT INTO returned {}", r);
            r=st.execute("INSERT INTO " + CUSTOMER_TABLE_NAME + " VALUES(4, 'DuPont')");
            LOG.info("INSERT INTO returned {}", r);

            connection.commit();
        } finally {
            if (null != st) {
                st.close();
            }

            if (null != connection) {
                connection.close();
            }
        }
    }

    /**
     * Delete any existing tables.
     */
    public static void dropExistingSchema() throws SQLException {
        String [] tables = listTables();
        if (null != tables) {
            Connection conn = getConnection();
            for (String table : tables) {
                Statement s = conn.createStatement();
                try {
                    s.executeUpdate("DROP TABLE " + table);
                    conn.commit();
                } finally {
                    s.close();
                }
            }
        }
    }

    public static String[] listTables() {
        ResultSet results = null;
        String [] tableTypes = {"TABLE"};
        try {
            try {
                DatabaseMetaData metaData = getConnection().getMetaData();
                results = metaData.getTables(null, null, null, tableTypes);
            } catch (SQLException sqlException) {
                LOG.error("Error reading database metadata: "
                        + sqlException.toString(), sqlException);
                return null;
            }

            if (null == results) {
                return null;
            }

            try {
                ArrayList<String> tables = new ArrayList<String>();
                while (results.next()) {
                    String tableName = results.getString("TABLE_NAME");
                    tables.add(tableName);
                }

                return tables.toArray(new String[0]);
            } catch (SQLException sqlException) {
                LOG.error("Error reading from database: "
                        + sqlException.toString(), sqlException);
                return null;
            }
        } finally {
            if (null != results) {
                try {
                    results.close();
                    getConnection().commit();
                } catch (SQLException sqlE) {
                    LOG.error("Exception closing ResultSet: "
                            + sqlE.toString(), sqlE);
                }
            }
        }
    }
}
