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

package org.apache.falcon.lifecycle;

import org.apache.commons.el.ExpressionEvaluatorImpl;
import org.apache.falcon.Pair;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.resource.TestContext;
import org.apache.falcon.retention.FeedEvictor;
import org.apache.falcon.util.HiveTestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hcatalog.api.HCatAddPartitionDesc;
import org.apache.hcatalog.api.HCatClient;
import org.apache.hcatalog.api.HCatPartition;
import org.apache.hcatalog.common.HCatException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.servlet.jsp.el.ELException;
import javax.servlet.jsp.el.ExpressionEvaluator;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

/**
 * Test for FeedEvictor for table.
 */
public class TableStorageFeedEvictorIT {

    private static final ExpressionEvaluator EVALUATOR = new ExpressionEvaluatorImpl();
    private static final ExpressionHelper RESOLVER = ExpressionHelper.get();

    private static final String METASTORE_URL = "thrift://localhost:49083/";
    private static final String DATABASE_NAME = "falcon_db";
    private static final String TABLE_NAME = "clicks";
    private static final String EXTERNAL_TABLE_NAME = "clicks_external";
    private static final String MULTI_COL_DATED_TABLE_NAME = "downloads";
    private static final String MULTI_COL_DATED_EXTERNAL_TABLE_NAME = "downloads_external";
    private static final String STORAGE_URL = "jail://global:00";
    private static final String EXTERNAL_TABLE_LOCATION = STORAGE_URL + "/falcon/staging/clicks_external/";
    private static final String MULTI_COL_DATED_EXTERNAL_TABLE_LOCATION = STORAGE_URL
        + "/falcon/staging/downloads_external/";

    private final InMemoryWriter stream = new InMemoryWriter(System.out);

    private HCatClient client;

    @BeforeClass
    public void setUp() throws Exception {
        FeedEvictor.OUT.set(stream);

        client = TestContext.getHCatClient(METASTORE_URL);

        HiveTestUtils.createDatabase(METASTORE_URL, DATABASE_NAME);
        final List<String> partitionKeys = Arrays.asList("ds", "region");
        HiveTestUtils.createTable(METASTORE_URL, DATABASE_NAME, TABLE_NAME, partitionKeys);
        HiveTestUtils.createExternalTable(METASTORE_URL, DATABASE_NAME, EXTERNAL_TABLE_NAME,
                partitionKeys, EXTERNAL_TABLE_LOCATION);

        final List<String> multiColDatedPartitionKeys = Arrays.asList("year", "month", "day", "region");
        HiveTestUtils.createTable(METASTORE_URL, DATABASE_NAME, MULTI_COL_DATED_TABLE_NAME, multiColDatedPartitionKeys);
        HiveTestUtils.createExternalTable(METASTORE_URL, DATABASE_NAME, MULTI_COL_DATED_EXTERNAL_TABLE_NAME,
                multiColDatedPartitionKeys, MULTI_COL_DATED_EXTERNAL_TABLE_LOCATION);
    }

    @AfterClass
    public void close() throws Exception {
        HiveTestUtils.dropTable(METASTORE_URL, DATABASE_NAME, EXTERNAL_TABLE_NAME);
        HiveTestUtils.dropTable(METASTORE_URL, DATABASE_NAME, TABLE_NAME);
        HiveTestUtils.dropTable(METASTORE_URL, DATABASE_NAME, MULTI_COL_DATED_EXTERNAL_TABLE_NAME);
        HiveTestUtils.dropTable(METASTORE_URL, DATABASE_NAME, MULTI_COL_DATED_TABLE_NAME);
        HiveTestUtils.dropDatabase(METASTORE_URL, DATABASE_NAME);
    }

    @DataProvider (name = "evictorTestDataProvider")
    private Object[][] createEvictorTestData() {
        return new Object[][] {
            {"days(10)", "", false},
            {"days(10)", "", true},
            {"days(10)", "-", false},
            {"days(10)", "-", true},
            {"days(15)", "", false},
            {"days(15)", "", true},
            {"days(15)", "-", false},
            {"days(15)", "-", true},
            {"days(100)", "", false},
            {"days(100)", "", true},
            {"days(100)", "-", false},
            {"days(100)", "-", true},
        };
    }

    @Test (dataProvider = "evictorTestDataProvider")
    public void testFeedEvictorForTableStorage(String retentionLimit, String dateSeparator,
                                               boolean isExternal) throws Exception {
        final String tableName = isExternal ? EXTERNAL_TABLE_NAME : TABLE_NAME;
        final String timeZone = "UTC";
        final String dateMask = "yyyy" + dateSeparator + "MM" + dateSeparator + "dd";

        List<String> candidatePartitions = getCandidatePartitions("days(10)", dateMask, timeZone, 3);
        addPartitions(tableName, candidatePartitions, isExternal);

        List<HCatPartition> partitions = client.getPartitions(DATABASE_NAME, tableName);
        Assert.assertEquals(partitions.size(), candidatePartitions.size());
        Pair<Date, Date> range = getDateRange(retentionLimit);
        List<HCatPartition> filteredPartitions = getFilteredPartitions(tableName, timeZone, dateMask, range);

        try {
            stream.clear();

            final String tableUri = DATABASE_NAME + "/" + tableName
                    + "/ds=${YEAR}" + dateSeparator + "${MONTH}" + dateSeparator + "${DAY};region=us";
            String feedBasePath = METASTORE_URL + tableUri;
            String logFile = STORAGE_URL + "/falcon/staging/feed/instancePaths-2013-09-13-01-00.csv";

            FeedEvictor.main(new String[]{
                "-feedBasePath", feedBasePath,
                "-retentionType", "instance",
                "-retentionLimit", retentionLimit,
                "-timeZone", timeZone,
                "-frequency", "days(1)",
                "-logFile", logFile,
                "-falconFeedStorageType", Storage.TYPE.TABLE.name(),
            });

            StringBuilder expectedInstancePaths = new StringBuilder();
            List<String> expectedInstancesEvicted = getExpectedEvictedInstances(
                    candidatePartitions, range.first, dateMask, timeZone, expectedInstancePaths);
            int expectedSurvivorSize = candidatePartitions.size() - expectedInstancesEvicted.size();

            List<HCatPartition> survivingPartitions = client.getPartitions(DATABASE_NAME, tableName);
            Assert.assertEquals(survivingPartitions.size(), expectedSurvivorSize,
                    "Unexpected number of surviving partitions");

            Assert.assertEquals(expectedInstancesEvicted.size(), filteredPartitions.size(),
                    "Unexpected number of evicted partitions");

            final String actualInstancesEvicted = readLogFile(new Path(logFile));
            validateInstancePaths(actualInstancesEvicted, expectedInstancePaths.toString());

            if (isExternal) {
                verifyFSPartitionsAreDeleted(candidatePartitions, range.first, dateMask, timeZone);
            }
        } finally {
            dropPartitions(tableName, candidatePartitions);
            Assert.assertEquals(client.getPartitions(DATABASE_NAME, tableName).size(), 0);
        }
    }

    @DataProvider (name = "evictorTestInvalidDataProvider")
    private Object[][] createEvictorTestDataInvalid() {
        return new Object[][] {
            {"days(10)", "/", false},
            {"days(10)", "/", true},
        };
    }

    @Test (dataProvider = "evictorTestInvalidDataProvider", expectedExceptions = URISyntaxException.class)
    public void testFeedEvictorForInvalidTableStorage(String retentionLimit, String dateSeparator,
                                                      boolean isExternal) throws Exception {
        final String tableName = isExternal ? EXTERNAL_TABLE_NAME : TABLE_NAME;
        final String timeZone = "UTC";
        final String dateMask = "yyyy" + dateSeparator + "MM" + dateSeparator + "dd";

        List<String> candidatePartitions = getCandidatePartitions("days(10)", dateMask, timeZone, 3);
        addPartitions(tableName, candidatePartitions, isExternal);

        try {
            stream.clear();

            final String tableUri = DATABASE_NAME + "/" + tableName
                    + "/ds=${YEAR}" + dateSeparator + "${MONTH}" + dateSeparator + "${DAY};region=us";
            String feedBasePath = METASTORE_URL + tableUri;
            String logFile = STORAGE_URL + "/falcon/staging/feed/instancePaths-2013-09-13-01-00.csv";

            FeedEvictor.main(new String[]{
                "-feedBasePath", feedBasePath,
                "-retentionType", "instance",
                "-retentionLimit", retentionLimit,
                "-timeZone", timeZone,
                "-frequency", "daily",
                "-logFile", logFile,
                "-falconFeedStorageType", Storage.TYPE.TABLE.name(),
            });

            Assert.fail("Exception must have been thrown");
        } finally {
            dropPartitions(tableName, candidatePartitions);
        }
    }

    @DataProvider (name = "multiColDatedEvictorTestDataProvider")
    private Object[][] createMultiColDatedEvictorTestData() {
        return new Object[][] {
            {"days(10)", false},
            {"days(10)", true},
            {"days(15)", false},
            {"days(15)", true},
            {"days(100)", false},
            {"days(100)", true},
        };
    }

    @Test (dataProvider = "multiColDatedEvictorTestDataProvider")
    public void testFeedEvictorForMultiColDatedTableStorage(String retentionLimit, boolean isExternal)
        throws Exception {
        final String tableName = isExternal ? MULTI_COL_DATED_EXTERNAL_TABLE_NAME : MULTI_COL_DATED_TABLE_NAME;
        final String timeZone = "UTC";

        List<Map<String, String>> candidatePartitions = getMultiColDatedCandidatePartitions("days(10)", timeZone, 3);
        addMultiColDatedPartitions(tableName, candidatePartitions, isExternal);

        List<HCatPartition> partitions = client.getPartitions(DATABASE_NAME, tableName);
        Assert.assertEquals(partitions.size(), candidatePartitions.size());
        Pair<Date, Date> range = getDateRange(retentionLimit);
        List<HCatPartition> filteredPartitions = getMultiColDatedFilteredPartitions(tableName, timeZone, range);

        try {
            stream.clear();

            final String tableUri = DATABASE_NAME + "/" + tableName
                + "/year=${YEAR};month=${MONTH};day=${DAY};region=us";
            String feedBasePath = METASTORE_URL + tableUri;
            String logFile = STORAGE_URL + "/falcon/staging/feed/instancePaths-2013-09-13-01-00.csv";

            FeedEvictor.main(new String[]{
                "-feedBasePath", feedBasePath,
                "-retentionType", "instance",
                "-retentionLimit", retentionLimit,
                "-timeZone", timeZone,
                "-frequency", "daily",
                "-logFile", logFile,
                "-falconFeedStorageType", Storage.TYPE.TABLE.name(),
            });

            StringBuilder expectedInstancePaths = new StringBuilder();
            List<Map<String, String>> expectedInstancesEvicted = getMultiColDatedExpectedEvictedInstances(
                candidatePartitions, range.first, timeZone, expectedInstancePaths);
            int expectedSurvivorSize = candidatePartitions.size() - expectedInstancesEvicted.size();

            List<HCatPartition> survivingPartitions = client.getPartitions(DATABASE_NAME, tableName);
            Assert.assertEquals(survivingPartitions.size(), expectedSurvivorSize,
                "Unexpected number of surviving partitions");

            Assert.assertEquals(expectedInstancesEvicted.size(), filteredPartitions.size(),
                "Unexpected number of evicted partitions");

            final String actualInstancesEvicted = readLogFile(new Path(logFile));
            validateInstancePaths(actualInstancesEvicted, expectedInstancePaths.toString());

            if (isExternal) {
                verifyMultiColDatedFSPartitionsAreDeleted(candidatePartitions, range.first, timeZone);
            }
        } finally {
            dropMultiColDatedPartitions(tableName, candidatePartitions);
            Assert.assertEquals(client.getPartitions(DATABASE_NAME, tableName).size(), 0);
        }
    }

    public List<String> getCandidatePartitions(String retentionLimit, String dateMask,
                                               String timeZone, int limit) throws Exception {
        List<String> partitions = new ArrayList<String>();

        Pair<Date, Date> range = getDateRange(retentionLimit);

        DateFormat dateFormat = new SimpleDateFormat(dateMask);
        dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));

        String startDate = dateFormat.format(range.first);
        partitions.add(startDate);

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(range.first);
        for (int i = 1; i <= limit; i++) {
            calendar.add(Calendar.DAY_OF_MONTH, -(i + 1));
            partitions.add(dateFormat.format(calendar.getTime()));
        }

        calendar.setTime(range.second);
        for (int i = 1; i <= limit; i++) {
            calendar.add(Calendar.DAY_OF_MONTH, -(i + 1));
            partitions.add(dateFormat.format(calendar.getTime()));
        }

        return partitions;
    }

    public List<Map<String, String>> getMultiColDatedCandidatePartitions(String retentionLimit,
        String timeZone, int limit) throws Exception {
        List<Map<String, String>> partitions = new ArrayList<Map<String, String>>();

        Pair<Date, Date> range = getDateRange(retentionLimit);

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(range.first);
        for (int i = 1; i <= limit; i++) {
            calendar.add(Calendar.DAY_OF_MONTH, -(i + 1));
            String[] dateCols = dateFormat.format(calendar.getTime()).split("-");
            Map<String, String> dateParts = new TreeMap<String, String>();
            dateParts.put("year", dateCols[0]);
            dateParts.put("month", dateCols[1]);
            dateParts.put("day", dateCols[2]);
            partitions.add(dateParts);
        }

        calendar.setTime(range.second);
        for (int i = 1; i <= limit; i++) {
            calendar.add(Calendar.DAY_OF_MONTH, -(i + 1));
            String[] dateCols = dateFormat.format(calendar.getTime()).split("-");
            Map<String, String> dateParts = new TreeMap<String, String>();
            dateParts.put("year", dateCols[0]);
            dateParts.put("month", dateCols[1]);
            dateParts.put("day", dateCols[2]);
            partitions.add(dateParts);
        }

        return partitions;
    }

    private Pair<Date, Date> getDateRange(String period) throws ELException {
        Long duration = (Long) EVALUATOR.evaluate("${" + period + "}",
                Long.class, RESOLVER, RESOLVER);
        Date end = new Date();
        Date start = new Date(end.getTime() - duration);
        return Pair.of(start, end);
    }

    private void addPartitions(String tableName, List<String> candidatePartitions,
                               boolean isTableExternal) throws Exception {
        Path path = new Path(EXTERNAL_TABLE_LOCATION);
        FileSystem fs = path.getFileSystem(new Configuration());

        for (String candidatePartition : candidatePartitions) {
            path = new Path(EXTERNAL_TABLE_LOCATION + candidatePartition);
            if (isTableExternal) {
                touch(fs, path.toString());
            }

            Map<String, String> partition = new HashMap<String, String>();
            partition.put("ds", candidatePartition); //yyyyMMDD
            partition.put("region", "in");
            HCatAddPartitionDesc addPtn = HCatAddPartitionDesc.create(
                    DATABASE_NAME, tableName, isTableExternal ? path.toString() : null, partition).build();
            client.addPartition(addPtn);
        }
    }

    private void addMultiColDatedPartitions(String tableName, List<Map<String, String>> candidatePartitions,
        boolean isTableExternal) throws Exception {
        Path path = new Path(MULTI_COL_DATED_EXTERNAL_TABLE_LOCATION);
        FileSystem fs = path.getFileSystem(new Configuration());

        for (Map<String, String> candidatePartition : candidatePartitions) {
            if (isTableExternal) {
                StringBuilder pathStr = new StringBuilder(MULTI_COL_DATED_EXTERNAL_TABLE_LOCATION);
                for (Map.Entry<String, String> entry : candidatePartition.entrySet()) {
                    pathStr.append(entry.getKey()).append("=").append(entry.getValue()).append("/");
                }
                pathStr.append("region=in");
                touch(fs, pathStr.toString());
            }

            candidatePartition.put("region", "in");
            HCatAddPartitionDesc addPtn = HCatAddPartitionDesc.create(
                DATABASE_NAME, tableName, null, candidatePartition).build();
            client.addPartition(addPtn);
        }
    }

    private void touch(FileSystem fs, String path) throws Exception {
        fs.mkdirs(new Path(path));
    }

    private void dropPartitions(String tableName, List<String> candidatePartitions) throws Exception {

        for (String candidatePartition : candidatePartitions) {
            Map<String, String> partition = new HashMap<String, String>();
            partition.put("ds", candidatePartition); //yyyyMMDD
            partition.put("region", "in");
            client.dropPartitions(DATABASE_NAME, tableName, partition, true);
        }
    }

    private void dropMultiColDatedPartitions(String tableName, List<Map<String, String>> candidatePartitions)
        throws Exception {

        for (Map<String, String> partition : candidatePartitions) {
            client.dropPartitions(DATABASE_NAME, tableName, partition, true);
        }
    }

    private List<HCatPartition> getFilteredPartitions(String tableName, String timeZone, String dateMask,
                                                      Pair<Date, Date> range) throws HCatException {
        DateFormat dateFormat = new SimpleDateFormat(dateMask);
        dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));

        String filter = "ds < '" + dateFormat.format(range.first) + "'";
        return client.listPartitionsByFilter(DATABASE_NAME, tableName, filter);
    }

    private List<HCatPartition> getMultiColDatedFilteredPartitions(String tableName, String timeZone,
        Pair<Date, Date> range) throws HCatException {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(range.first);
        String[] dateCols = dateFormat.format(calendar.getTime()).split("-");
        // filter eg: "(year < '2014') or (year = '2014' and month < '02') or
        // (year = '2014' and month = '02' and day < '24')"
        String filter1 = "(year < '" + dateCols[0] + "')";
        String filter2 = "(year = '" + dateCols[0] + "' and month < '" + dateCols[1] + "')";
        String filter3 = "(year = '" + dateCols[0] + "' and month = '" + dateCols[1]
            + "' and day < '" + dateCols[2] + "')";
        String filter = filter1 + " or " + filter2 + " or " + filter3;
        return client.listPartitionsByFilter(DATABASE_NAME, tableName, filter);
    }

    public List<String> getExpectedEvictedInstances(List<String> candidatePartitions, Date date, String dateMask,
                                                    String timeZone, StringBuilder instancePaths) {
        Collections.sort(candidatePartitions);
        instancePaths.append("instancePaths=");

        DateFormat dateFormat = new SimpleDateFormat(dateMask);
        dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
        String startDate = dateFormat.format(date);

        List<String> expectedInstances = new ArrayList<String>();
        for (String candidatePartition : candidatePartitions) {
            if (candidatePartition.compareTo(startDate) < 0) {
                expectedInstances.add(candidatePartition);
                instancePaths.append("[").append(candidatePartition).append("; in],");
            }
        }

        return expectedInstances;
    }

    public List<Map<String, String>> getMultiColDatedExpectedEvictedInstances(List<Map<String, String>>
            candidatePartitions, Date date, String timeZone, StringBuilder instancePaths) throws Exception {
        instancePaths.append("instancePaths=");

        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
        String startDate = dateFormat.format(date);

        List<Map<String, String>> expectedInstances = new ArrayList<Map<String, String>>();
        for (Map<String, String> partition : candidatePartitions) {
            String partDate = partition.get("year") + partition.get("month") + partition.get("day");
            if (partDate.compareTo(startDate) < 0) {
                expectedInstances.add(partition);
                instancePaths.append("[")
                    .append(partition.values().toString().replace("," , ";"))
                    .append("; in],");
            }
        }

        return expectedInstances;
    }

    private void verifyFSPartitionsAreDeleted(List<String> candidatePartitions, Date date,
                                              String dateMask, String timeZone) throws IOException {

        FileSystem fs = new Path(EXTERNAL_TABLE_LOCATION).getFileSystem(new Configuration());

        DateFormat dateFormat = new SimpleDateFormat(dateMask);
        dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
        String startDate = dateFormat.format(date);

        Collections.sort(candidatePartitions);
        for (String candidatePartition : candidatePartitions) {
            final String path = EXTERNAL_TABLE_LOCATION + "ds=" + candidatePartition + "/region=in";
            if (candidatePartition.compareTo(startDate) < 0
                    && fs.exists(new Path(path))) {
                Assert.fail("Expecting " + path + " to be deleted");
            }
        }
    }

    private void verifyMultiColDatedFSPartitionsAreDeleted(List<Map<String, String>> candidatePartitions,
        Date date, String timeZone) throws Exception {

        FileSystem fs = new Path(MULTI_COL_DATED_EXTERNAL_TABLE_LOCATION).getFileSystem(new Configuration());

        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
        String startDate = dateFormat.format(date);

        for (Map<String, String> partition : candidatePartitions) {
            String partDate = partition.get("year") + partition.get("month") + partition.get("day");
            final String path = MULTI_COL_DATED_EXTERNAL_TABLE_LOCATION
                + partition.get("year") + "/" + partition.get("month") + "/" + partition.get("day") + "/region=in";
            if (partDate.compareTo(startDate) < 0 && fs.exists(new Path(path))) {
                Assert.fail("Expecting " + path + " to be deleted");
            }
        }
    }

    private String readLogFile(Path logFile) throws IOException {
        ByteArrayOutputStream writer = new ByteArrayOutputStream();
        InputStream date = logFile.getFileSystem(new Configuration()).open(logFile);
        IOUtils.copyBytes(date, writer, 4096, true);
        return writer.toString();
    }

    // instance paths could be deleted in any order; compare the list of evicted paths
    private void validateInstancePaths(String actualInstancesEvicted, String expectedInstancePaths) {
        String[] actualEvictedPathStr = actualInstancesEvicted.split("instancePaths=");
        String[] expectedEvictedPathStr = expectedInstancePaths.split("instancePaths=");
        if (actualEvictedPathStr.length == 0) {
            Assert.assertEquals(expectedEvictedPathStr.length, 0);
        } else {
            Assert.assertEquals(actualEvictedPathStr.length, 2);
            Assert.assertEquals(expectedEvictedPathStr.length, 2);

            String[] actualEvictedPaths = actualEvictedPathStr[1].split(",");
            String[] expectedEvictedPaths = actualEvictedPathStr[1].split(",");
            Arrays.sort(actualEvictedPaths);
            Arrays.sort(expectedEvictedPaths);
            Assert.assertEquals(actualEvictedPaths, expectedEvictedPaths,
                "Unexpected number of Logged partitions");
        }
    }

    private static class InMemoryWriter extends PrintStream {

        private final StringBuffer buffer = new StringBuffer();

        public InMemoryWriter(OutputStream out) {
            super(out);
        }

        @Override
        public void println(String x) {
            buffer.append(x);
            super.println(x);
        }

        @SuppressWarnings("UnusedDeclaration")
        public String getBuffer() {
            return buffer.toString();
        }

        public void clear() {
            buffer.delete(0, buffer.length());
        }
    }
}
