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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hsqldb.Server;

/**
 * Create a simple hsqldb server and schema to use for testing.
 */
public final class HsqldbTestUtils {

    public static final Log LOG = LogFactory.getLog(HsqldbTestUtils.class.getName());

    // singleton server instance.
    private static Server server;

    private static final String IN_MEM = "mem:/";

    private static boolean inMemoryDB = IN_MEM.equals(getServerHost());

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
    private static final String DB_URL = "jdbc:hsqldb:" + getServerHost() + DATABASE_NAME;
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
            String dbLocation = tmpDir + "/falcon/testdb.file";
            if (inMemoryDB) {dbLocation = IN_MEM; }
            server = new Server();
            server.setDatabaseName(0, DATABASE_NAME);
            server.putPropertiesFromString("database.0=" + dbLocation
                    + ";no_system_exit=true;");
            server.start();
            LOG.info("Started server with url=" + DB_URL);
        }
    }

    public static void stop() {
        if (null != server) {
            server.stop();
        }
    }

    public static void tearDown() throws SQLException {
        dropExistingSchema();
        stop();
    }

    public static void changeSAPassword(String passwd) throws Exception {
        Connection connection = null;
        Statement st = null;

        LOG.info("Changing password for SA");
        try {
            connection = getConnectionSystem();

            st = connection.createStatement();
            st.executeUpdate("SET PASSWORD \"" + passwd + "\"");
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
        return getConnection("SA", "sqoop");
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
            st.executeUpdate("DROP TABLE " + CUSTOMER_TABLE_NAME + " IF EXISTS");
            st.executeUpdate("CREATE TABLE " + CUSTOMER_TABLE_NAME + "(id INT NOT NULL PRIMARY KEY, name VARCHAR(64))");

            st.executeUpdate("INSERT INTO " + CUSTOMER_TABLE_NAME + " VALUES(1, 'Apple')");
            st.executeUpdate("INSERT INTO " + CUSTOMER_TABLE_NAME + " VALUES(2, 'Blackberry')");
            st.executeUpdate("INSERT INTO " + CUSTOMER_TABLE_NAME + " VALUES(3, 'Caterpillar')");
            st.executeUpdate("INSERT INTO " + CUSTOMER_TABLE_NAME + " VALUES(4, 'DuPont')");

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
