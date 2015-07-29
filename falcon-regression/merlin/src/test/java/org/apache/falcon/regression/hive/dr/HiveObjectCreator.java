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

package org.apache.falcon.regression.hive.dr;

import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import static org.apache.falcon.regression.core.util.HadoopUtil.writeDataForHive;
import static org.apache.falcon.regression.core.util.HiveUtil.runSql;

/**
 * Create Hive tables for testing Hive DR. Note that this is not expected to be used out of
 * HiveDR tests.
 */
final class HiveObjectCreator {
    private static final Logger LOGGER = Logger.getLogger(HiveObjectCreator.class);
    private static final String HDFS_TMP_DIR = "/tmp/hive_objects/";

    private HiveObjectCreator() {
        throw new AssertionError("Instantiating utility class...");
    }

    static void bootstrapCopy(Connection srcConnection, FileSystem srcFs, String srcTable,
                              Connection dstConnection, FileSystem dstFs, String dstTable) throws Exception {
        LOGGER.info("Starting bootstrap...");
        final String dumpPath = HDFS_TMP_DIR + srcTable + "/";
        HadoopUtil.recreateDir(srcFs, dumpPath);
        runSqlQuietly(srcConnection, "dfs -chmod -R 777 " + dumpPath);
        HadoopUtil.deleteDirIfExists(dumpPath, dstFs);
        runSql(srcConnection, "export table " + srcTable + " to '" + dumpPath + "' FOR REPLICATION('ignore')");
        FileUtil.copy(srcFs, new Path(dumpPath), dstFs, new Path(dumpPath), false, true, new Configuration());
        runSqlQuietly(dstConnection, "dfs -chmod -R 777 " + dumpPath);
        runSql(dstConnection, "import table " + dstTable + " from '" + dumpPath + "'");
        HadoopUtil.deleteDirIfExists(dumpPath, srcFs);
        HadoopUtil.deleteDirIfExists(dumpPath, dstFs);
        LOGGER.info("Finished bootstrap");
    }

    /* We need to delete it using hive query as the created directory is owned by hive.*/
    private static void runSqlQuietly(Connection srcConnection, String sql) {
        try {
            runSql(srcConnection, sql);
        } catch (SQLException ex) {
            LOGGER.info("Exception while hive ql execution: " + ex.getMessage());
        }
    }

    /**
     * Create an external table.
     * @param connection jdbc connection object to use for issuing queries to hive
     * @param fs filesystem object to upload the data
     * @param clickDataLocation location to upload the data to
     * @throws IOException
     * @throws SQLException
     */
    static void createExternalTable(Connection connection, FileSystem fs, String
        clickDataLocation, String tableName) throws IOException, SQLException {
        HadoopUtil.deleteDirIfExists(clickDataLocation, fs);
        fs.mkdirs(new Path(clickDataLocation));
        fs.setPermission(new Path(clickDataLocation), FsPermission.getDirDefault());
        writeDataForHive(fs, clickDataLocation,
            new StringBuffer("click1").append((char) 0x01).append("01:01:01").append("\n")
                .append("click2").append((char) 0x01).append("02:02:02"), true);
        //clusterFS.setPermission(new Path(clickDataPart2), FsPermission.getFileDefault());
        runSql(connection, "create external table " + tableName
            + " (data string, time string) "
            + "location '" + clickDataLocation + "'");
        runSql(connection, "select * from " + tableName);
    }


    /**
     * Create an external table.
     * @param connection jdbc connection object to use for issuing queries to hive
     * @param fs filesystem object to upload the data
     * @param clickDataLocation location to upload the data to
     * @throws IOException
     * @throws SQLException
     */
    static void createExternalPartitionedTable(Connection connection, FileSystem fs, String
        clickDataLocation, String tableName) throws IOException, SQLException {
        final String clickDataPart1 = clickDataLocation + "2001-01-01/";
        final String clickDataPart2 = clickDataLocation + "2001-01-02/";
        fs.mkdirs(new Path(clickDataLocation));
        fs.setPermission(new Path(clickDataLocation), FsPermission.getDirDefault());
        writeDataForHive(fs, clickDataPart1,
            new StringBuffer("click1").append((char) 0x01).append("01:01:01"), true);
        writeDataForHive(fs, clickDataPart2,
            new StringBuffer("click2").append((char) 0x01).append("02:02:02"), true);
        //clusterFS.setPermission(new Path(clickDataPart2), FsPermission.getFileDefault());
        runSql(connection, "create external table " + tableName
            + " (data string, time string) partitioned by (date_ string) "
            + "location '" + clickDataLocation + "'");
        runSql(connection, "alter table " + tableName + " add partition "
            + "(date_='2001-01-01') location '" + clickDataPart1 + "'");
        runSql(connection, "alter table " + tableName + " add partition "
            + "(date_='2001-01-02') location '" + clickDataPart2 + "'");
        runSql(connection, "select * from " + tableName);
    }

    /**
     * Create an partitioned table.
     * @param connection jdbc connection object to use for issuing queries to hive
     * @throws SQLException
     */
    static void createPartitionedTable(Connection connection) throws SQLException {
        runSql(connection, "create table global_store_sales "
            + "(customer_id string, item_id string, quantity float, price float, time timestamp) "
            + "partitioned by (country string)");
        runSql(connection,
            "insert into table global_store_sales partition (country = 'us') values"
                + "('c1', 'i1', '1', '1', '2001-01-01 01:01:01')");
        runSql(connection,
            "insert into table global_store_sales partition (country = 'uk') values"
                + "('c2', 'i2', '2', '2', '2001-01-01 01:01:02')");
        runSql(connection, "select * from global_store_sales");
    }

    /**
     * Create an plain old table.
     * @param connection jdbc connection object to use for issuing queries to hive
     * @param tblName
     * @throws SQLException
     */
    static void createVanillaTable(Connection connection, String tblName) throws SQLException {
        //vanilla table
        runSql(connection, "create table " + tblName
            + "(customer_id string, item_id string, quantity float, price float, time timestamp)");
        runSql(connection, "insert into table " + tblName + " values "
            + "('c1', 'i1', '1', '1', '2001-01-01 01:01:01'), "
            + "('c2', 'i2', '2', '2', '2001-01-01 01:01:02')");
        runSql(connection, "select * from " + tblName);
    }

    /**
     * Create a partitioned table with either dynamic or static partitions.
     * @param connection jdbc connection object to use for issuing queries to hive
     * @param dynamic should partitions be added in dynamic or static way
     * @throws SQLException
     */
    static void createPartitionedTable(Connection connection,
                                       boolean dynamic) throws SQLException {
        String [][] partitions = {
            {"us", "Kansas", },
            {"us", "California", },
            {"au", "Queensland", },
            {"au", "Victoria", },
        };
        //create table
        runSql(connection, "drop table global_store_sales");
        runSql(connection, "create table global_store_sales(customer_id string,"
            + " item_id string, quantity float, price float, time timestamp) "
            + "partitioned by (country string, state string)");
        //provide data
        String query;
        if (dynamic) {
            //disable strict mode, thus both partitions can be used as dynamic
            runSql(connection, "set hive.exec.dynamic.partition.mode=nonstrict");
            query = "insert into table global_store_sales partition"
                + "(country, state) values('c%3$s', 'i%3$s', '%3$s', '%3$s', "
                + "'2001-01-01 01:01:0%3$s', '%1$s', '%2$s')";
        } else {
            query = "insert into table global_store_sales partition"
                + "(country = '%1$s', state = '%2$s') values('c%3$s', 'i%3$s', '%3$s', '%3$s', "
                + "'2001-01-01 01:01:0%3$s')";
        }
        for (int i = 0; i < partitions.length; i++) {
            runSql(connection, String.format(query, partitions[i][0], partitions[i][1], i + 1));
        }
        runSql(connection, "select * from global_store_sales");
    }

    static void createSerDeTable(Connection connection) throws SQLException {
        runSql(connection, "create table store_json "
            + "(customer_id string, item_id string, quantity float, price float, time timestamp) "
            + "row format serde 'org.apache.hive.hcatalog.data.JsonSerDe' ");
        runSql(connection, "insert into table store_json values "
            + "('c1', 'i1', '1', '1', '2001-01-01 01:01:01'), "
            + "('c2', 'i2', '2', '2', '2001-01-01 01:01:02')");
        runSql(connection, "select * from store_json");
    }

}
