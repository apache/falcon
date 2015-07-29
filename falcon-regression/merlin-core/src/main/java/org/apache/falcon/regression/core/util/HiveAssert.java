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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatDatabase;
import org.apache.hive.hcatalog.api.HCatPartition;
import org.apache.hive.hcatalog.api.HCatTable;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.log4j.Logger;
import org.testng.asserts.SoftAssert;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Assertions for to Hive objects. */
public final class HiveAssert {
    private HiveAssert() {
        throw new AssertionError("Instantiating utility class...");
    }

    private static final Logger LOGGER = Logger.getLogger(HiveAssert.class);

    /**
     * Assertion for column equality - it also covers stuff that is not covered by
     * HCatFieldSchema.equals().
     * @param columns1 first column for comparison
     * @param columns2 second column for comparison
     * @param softAssert object to use for performing assertion
     * @return object used for performing assertion
     */
    public static SoftAssert assertColumnListEqual(List<HCatFieldSchema> columns1,
                                                   List<HCatFieldSchema> columns2,
                                                   SoftAssert softAssert) {
        softAssert.assertEquals(columns1, columns2, "List of columns for two tables are not same");
        for (int i = 0; i < columns1.size(); ++i) {
            HCatFieldSchema column1 = columns1.get(i);
            HCatFieldSchema column2 = columns2.get(i);
            softAssert.assertEquals(column2.getComment(), column1.getComment(),
                "Comments of the columns: " + column1 + " & " + column2 + " is not same");
        }
        return softAssert;
    }

    /**
     * Assertion for equality of partitions - equality using HCatPartition.equals() is not
     * satisfactory for our purpose.
     * @param table1Partitions first list of partitions for comparison
     * @param table2Partitions second list of partitions for comparison
     * @param softAssert object to use for performing assertion
     * @return object used for performing assertion
     */
    public static SoftAssert assertPartitionListEqual(List<HCatPartition> table1Partitions,
        List<HCatPartition> table2Partitions, SoftAssert softAssert) {
        softAssert.assertEquals(table1Partitions.size(), table2Partitions.size(),
            "Number of partitions are not same");
        try {
            for (int i = 0; i < table1Partitions.size(); i++) {
                final HCatPartition table1Partition = table1Partitions.get(i);
                final HCatPartition table2Partition = table2Partitions.get(i);
                softAssert.assertEquals(table2Partition.getValues(), table1Partition.getValues(),
                    "Partitions don't have same values");
            }
        } catch (Exception e) {
            softAssert.fail("Couldn't do partition equality.", e);
        }
        return softAssert;
    }

    /**
     * Assertion for equality of two tables (including table properties and table type).
     * @param cluster1 the ColoHelper of first cluster
     * @param table1 the first table
     * @param cluster2 the ColoHelper of second cluster
     * @param table2 the second table
     * @param softAssert object used for performing assertion
     * @return object used for performing assertion
     * @throws java.io.IOException
     */
    public static SoftAssert assertTableEqual(ColoHelper cluster1, HCatTable table1,
                                              ColoHelper cluster2, HCatTable table2,
                                              SoftAssert softAssert) throws IOException {
        return assertTableEqual(cluster1, table1, cluster2, table2, softAssert, true);
    }

    /**
     * Assertion for equality of two tables.
     * @param cluster1 the ColoHelper of first cluster
     * @param table1 the first table (expected values)
     * @param cluster2 the ColoHelper of second cluster
     * @param table2 the second table (actual values)
     * @param softAssert object used for performing assertion
     * @return object used for performing assertion
     * @throws java.io.IOException
     */
    public static SoftAssert assertTableEqual(ColoHelper cluster1, HCatTable table1,
                                              ColoHelper cluster2, HCatTable table2,
                                              SoftAssert softAssert,
                                              boolean notIgnoreTblTypeAndProps) throws IOException {
        FileSystem cluster1FS = cluster1.getClusterHelper().getHadoopFS();
        FileSystem cluster2FS = cluster2.getClusterHelper().getHadoopFS();
        final String table1FullName = table1.getDbName() + "." + table1.getTableName();
        final String table2FullName = table2.getDbName() + "." + table2.getTableName();
        LOGGER.info("Checking equality of table : " + table1FullName + " & " + table2FullName);
        //table metadata equality
        softAssert.assertEquals(table2.comment(), table1.comment(),
            "Table " + table1FullName + " has different comment from " + table2FullName);
        softAssert.assertEquals(table2.getBucketCols(), table1.getBucketCols(),
            "Table " + table1FullName + " has different bucket columns from " + table2FullName);
        assertColumnListEqual(table1.getCols(), table2.getCols(), softAssert);
        softAssert.assertEquals(table2.getNumBuckets(), table1.getNumBuckets(),
            "Table " + table1FullName + " has different number of buckets from " + table2FullName);
        assertColumnListEqual(table1.getPartCols(), table2.getPartCols(), softAssert);
        softAssert.assertEquals(table2.getSerdeParams(), table1.getSerdeParams(),
            "Table " + table1FullName + " has different serde params from " + table2FullName);
        softAssert.assertEquals(table2.getSortCols(), table1.getSortCols(),
            "Table " + table1FullName + " has different sort columns from " + table2FullName);
        softAssert.assertEquals(table2.getStorageHandler(), table1.getStorageHandler(),
            "Table " + table1FullName + " has different storage handler from " + table2FullName);
        if (notIgnoreTblTypeAndProps) {
            softAssert.assertEquals(table2.getTabletype(), table1.getTabletype(),
                "Table " + table1FullName + " has different Tabletype from " + table2FullName);
        }
        final Map<String, String> tbl1Props = table1.getTblProps();
        final Map<String, String> tbl2Props = table2.getTblProps();
        final String[] ignoreTblProps = {"transient_lastDdlTime", "repl.last.id",
            "last_modified_by", "last_modified_time", "COLUMN_STATS_ACCURATE", };
        for (String ignoreTblProp : ignoreTblProps) {
            tbl1Props.remove(ignoreTblProp);
            tbl2Props.remove(ignoreTblProp);
        }
        final String[] ignoreDefaultProps = {"numRows", "rawDataSize"};
        for (String ignoreProp : ignoreDefaultProps) {
            if ("-1".equals(tbl1Props.get(ignoreProp))) {
                tbl1Props.remove(ignoreProp);
            }
            if ("-1".equals(tbl2Props.get(ignoreProp))) {
                tbl2Props.remove(ignoreProp);
            }
        }

        if (notIgnoreTblTypeAndProps) {
            softAssert.assertEquals(tbl2Props, tbl1Props,
                "Table " + table1FullName + " has different TblProps from " + table2FullName);
        }
        LOGGER.info("Checking equality of table partitions");
        HCatClient hcatClient1 = cluster1.getClusterHelper().getHCatClient();
        HCatClient hcatClient2 = cluster2.getClusterHelper().getHCatClient();
        final List<HCatPartition> table1Partitions =
            hcatClient1.getPartitions(table1.getDbName(), table1.getTableName());
        final List<HCatPartition> table2Partitions =
            hcatClient2.getPartitions(table2.getDbName(), table2.getTableName());
        assertPartitionListEqual(table1Partitions, table2Partitions, softAssert);
        if (notIgnoreTblTypeAndProps) {
            softAssert.assertEquals(
                cluster2FS.getContentSummary(new Path(table2.getLocation())).getLength(),
                cluster1FS.getContentSummary(new Path(table1.getLocation())).getLength(),
                "Size of content for table1 and table2 are different");
        }

        //table content equality
        LOGGER.info("Checking equality of table contents");
        Statement jdbcStmt1 = null, jdbcStmt2 = null;
        try {
            final boolean execute1;
            final boolean execute2;
            jdbcStmt1 = cluster1.getClusterHelper().getHiveJdbcConnection().createStatement();
            jdbcStmt2 = cluster2.getClusterHelper().getHiveJdbcConnection().createStatement();
            execute1 = jdbcStmt1.execute("select * from " + table1FullName);
            execute2 = jdbcStmt2.execute("select * from " + table2FullName);
            softAssert.assertEquals(execute2, execute1,
                "Table " + table1FullName + " has different result of select * from " + table2FullName);
            if (execute1 && execute2) {
                final ResultSet resultSet1 = jdbcStmt1.getResultSet();
                final ResultSet resultSet2 = jdbcStmt2.getResultSet();
                final List<String> rows1 = HiveUtil.fetchRows(resultSet1);
                final List<String> rows2 = HiveUtil.fetchRows(resultSet2);
                softAssert.assertEquals(rows2, rows1,
                    "Table " + table1FullName + " has different content from " + table2FullName);
            }
        } catch (SQLException e) {
            softAssert.fail("Comparison of content of table " + table1FullName
                + " with content of table " + table2FullName + " failed because of exception\n"
                + ExceptionUtils.getFullStackTrace(e));
        } finally {
            if (jdbcStmt1 != null) {
                try {
                    jdbcStmt1.close();
                } catch (SQLException e) {
                    LOGGER.warn("Closing of jdbcStmt1 failed: " + ExceptionUtils.getFullStackTrace(e));
                }
            }
            if (jdbcStmt2 != null) {
                try {
                    jdbcStmt2.close();
                } catch (SQLException e) {
                    LOGGER.warn("Closing of jdbcStmt2 failed: " + ExceptionUtils.getFullStackTrace(e));
                }
            }
        }
        return softAssert;
    }

    /**
     * Assertion for equality of two dbs.
     * @param cluster1 the ColoHelper of first cluster
     * @param db1 first database for comparison (expected values)
     * @param cluster2 the ColoHelper of second cluster
     * @param db2 second database for comparison (actual values)
     * @param softAssert object used for performing assertion
     * @return object used for performing assertion
     * @throws java.io.IOException
     */
    public static SoftAssert assertDbEqual(ColoHelper cluster1, HCatDatabase db1,
                                           ColoHelper cluster2, HCatDatabase db2,
                                           SoftAssert softAssert) throws IOException {
        HCatClient hcatClient1 = cluster1.getClusterHelper().getHCatClient();
        HCatClient hcatClient2 = cluster2.getClusterHelper().getHCatClient();
        //check database name equality
        final String db1Name = db1.getName();
        final String db2Name = db2.getName();
        softAssert.assertEquals(db2.getComment(), db1.getComment(), "Comment differ for the dbs");
        //check database properties equality
        softAssert.assertEquals(db2.getProperties(), db1.getProperties(),
            "Database " + db1Name + " has different properties from " + db2Name);
        //checking table equality
        final List<String> db1tableNames = hcatClient1.listTableNamesByPattern(db1Name, ".*");
        final List<String> db2tableNames = hcatClient2.listTableNamesByPattern(db2Name, ".*");
        Collections.sort(db1tableNames);
        Collections.sort(db2tableNames);
        softAssert.assertEquals(db2tableNames, db1tableNames,
            "Table names are not same. Actual: " + db1tableNames + " Expected: " + db2tableNames);
        for (String tableName : db1tableNames) {
            try {
                assertTableEqual(cluster1, hcatClient1.getTable(db1Name, tableName),
                    cluster2, hcatClient2.getTable(db2Name, tableName), softAssert);
            } catch (HCatException e) {
                softAssert.fail("Table equality check threw exception.", e);
            }
        }
        return softAssert;
    }
}
