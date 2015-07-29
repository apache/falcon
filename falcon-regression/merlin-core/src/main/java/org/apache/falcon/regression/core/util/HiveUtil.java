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

package org.apache.falcon.regression.core.util;

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility class for stuff related to hive. All the methods in this class assume that they are
 * dealing with small dataset.
 */
public final class HiveUtil {

    private HiveUtil() {
        throw new AssertionError("Instantiating utility class...");
    }
    private static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

    private static final Logger LOGGER = Logger.getLogger(HiveUtil.class);

    public static Connection getHiveJdbcConnection(final String jdbcUrl, final String user, final String password,
                                                   final String hivePrincipal)
        throws ClassNotFoundException, SQLException, IOException, InterruptedException {
        final String transportMode = new HiveConf().get("hive.server2.transport.mode", "binary");
        String connectionStringSuffix = "";
        if (transportMode.equalsIgnoreCase("http")) {
            connectionStringSuffix += "transportMode=http;httpPath=cliservice;";
        }
        if (MerlinConstants.IS_SECURE) {
            connectionStringSuffix += String.format("principal=%s;kerberosAuthType=fromSubject;", hivePrincipal);
        }
        final String connectionStringSuffix2 = connectionStringSuffix;
        final UserGroupInformation ugi = KerberosHelper.getUGI(user);
        final Connection conn = ugi.doAs(new PrivilegedExceptionAction<Connection>() {
            @Override
            public Connection run() throws Exception {
                Class.forName(DRIVER_NAME);
                return DriverManager.getConnection(jdbcUrl + "/;" + connectionStringSuffix2, ugi.getShortUserName(),
                    password);
            }
        });

        return conn;
    }

    /**
     * Fetch rows from a given ResultSet and convert is a a list of string, each string is comma
     * separated column values. The output also has header with column names and footer with
     * number of rows returned.
     * @param rs result set
     * @return List of string - each string corresponds to the output output that you will get on
     * sql prompt
     * @throws SQLException
     */
    public static List<String> fetchRows(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        List<String> output = new ArrayList<String>();

        int numberOfColumns = metaData.getColumnCount();
        StringBuilder sbCol = new StringBuilder();
        for (int i = 1; i <= numberOfColumns; i++) {
            if (i > 1) {
                sbCol.append(",");
            }
            String columnName = metaData.getColumnName(i);
            // the column name looks like tab1.col1
            // we want to remove table name else table equality will fail
            if (columnName.contains(".")) {
                columnName = columnName.split("\\.")[1];
            }
            sbCol.append("'").append(columnName).append("'");
        }
        LOGGER.info(sbCol.toString());
        output.add(sbCol.toString());

        int numberOfRows = 0;
        while (rs.next()) {
            StringBuilder sbVal = new StringBuilder();
            numberOfRows++;
            for (int i = 1; i <= numberOfColumns; i++) {
                if (i > 1) {
                    sbVal.append(",");
                }
                String columnValue = rs.getString(i);
                sbVal.append("'").append(columnValue != null ? columnValue : "").append("'");
            }
            LOGGER.info(sbVal.toString());
            output.add(sbVal.toString());
        }
        Collections.sort(output); //sorting to ensure stability results across different runs
        String rowStr = (numberOfRows > 0 ? numberOfRows : "No")
            + (numberOfRows == 1 ? " row" : " rows") + " selected";
        LOGGER.info(rowStr);
        output.add(rowStr);
        return output;
    }

    /**
     * Run a sql using given connection.
     * @param connection The connection to be used for running sql
     * @param sql the sql to be run
     * @throws SQLException
     * @return output of the query as a List of strings
     */
    public static List<String> runSql(Connection connection, String sql) throws SQLException {
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            LOGGER.info("Executing: " + sql);
            stmt.execute(sql);
            final ResultSet resultSet = stmt.getResultSet();
            if (resultSet != null) {
                final List<String> output = fetchRows(resultSet);
                LOGGER.info("Results are:\n" + StringUtils.join(output, "\n"));
                return output;
            }
            LOGGER.info("Query executed.");
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
        return new ArrayList<>();
    }
}
