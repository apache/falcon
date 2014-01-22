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
import org.apache.falcon.catalog.HiveCatalogService;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.expression.ExpressionHelper;
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
    private static final String STORAGE_URL = "jail://global:00";
    private static final String EXTERNAL_TABLE_LOCATION = STORAGE_URL + "/falcon/staging/clicks_external/";

    private final InMemoryWriter stream = new InMemoryWriter(System.out);

    private HCatClient client;

    @BeforeClass
    public void setUp() throws Exception {
        FeedEvictor.OUT.set(stream);

        client = HiveCatalogService.get(METASTORE_URL);

        HiveTestUtils.createDatabase(METASTORE_URL, DATABASE_NAME);
        final List<String> partitionKeys = Arrays.asList("ds", "region");
        HiveTestUtils.createTable(METASTORE_URL, DATABASE_NAME, TABLE_NAME, partitionKeys);
        HiveTestUtils.createExternalTable(METASTORE_URL, DATABASE_NAME, EXTERNAL_TABLE_NAME,
                partitionKeys, EXTERNAL_TABLE_LOCATION);
    }

    @AfterClass
    public void close() throws Exception {
        HiveTestUtils.dropTable(METASTORE_URL, DATABASE_NAME, EXTERNAL_TABLE_NAME);
        HiveTestUtils.dropTable(METASTORE_URL, DATABASE_NAME, TABLE_NAME);
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
                "-frequency", "daily",
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
            Assert.assertEquals(actualInstancesEvicted, expectedInstancePaths.toString(),
                    "Unexpected number of Logged partitions");

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
            if (isTableExternal) {
                touch(fs, EXTERNAL_TABLE_LOCATION + candidatePartition);
            }

            Map<String, String> partition = new HashMap<String, String>();
            partition.put("ds", candidatePartition); //yyyyMMDD
            partition.put("region", "in");
            HCatAddPartitionDesc addPtn = HCatAddPartitionDesc.create(
                    DATABASE_NAME, tableName, null, partition).build();
            client.addPartition(addPtn);
        }
    }

    private void touch(FileSystem fs, String path) throws Exception {
        fs.create(new Path(path)).close();
    }

    private void dropPartitions(String tableName, List<String> candidatePartitions) throws Exception {

        for (String candidatePartition : candidatePartitions) {
            Map<String, String> partition = new HashMap<String, String>();
            partition.put("ds", candidatePartition); //yyyyMMDD
            partition.put("region", "in");
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
                instancePaths.append("[")
                        .append(candidatePartition)
                        .append(", in],");
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

    private String readLogFile(Path logFile) throws IOException {
        ByteArrayOutputStream writer = new ByteArrayOutputStream();
        InputStream date = logFile.getFileSystem(new Configuration()).open(logFile);
        IOUtils.copyBytes(date, writer, 4096, true);
        return writer.toString();
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

        public String getBuffer() {
            return buffer.toString();
        }

        public void clear() {
            buffer.delete(0, buffer.length());
        }
    }
}
