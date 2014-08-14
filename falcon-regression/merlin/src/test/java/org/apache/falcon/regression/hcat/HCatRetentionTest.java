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

package org.apache.falcon.regression.hcat;

import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.regression.core.enumsAndConstants.FeedType;
import org.apache.falcon.regression.core.enumsAndConstants.RetentionUnit;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HCatUtil;
import org.apache.falcon.regression.core.util.MathUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util.URLS;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hive.hcatalog.api.HCatAddPartitionDesc;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatCreateTableDesc;
import org.apache.hive.hcatalog.api.HCatPartition;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.oozie.client.OozieClient;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.annotations.DataProvider;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HCatRetentionTest extends BaseTestClass {

    private static final Logger logger = Logger.getLogger(HCatRetentionTest.class);

    private Bundle bundle;
    public static HCatClient cli;
    final String testDir = "/HCatRetentionTest/";
    final String baseTestHDFSDir = baseHDFSDir + testDir;
    final String dBName = "default";
    final ColoHelper cluster = servers.get(0);
    final FileSystem clusterFS = serverFS.get(0);
    final OozieClient clusterOC = serverOC.get(0);
    String tableName;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        HadoopUtil.recreateDir(clusterFS, baseTestHDFSDir);
        cli = cluster.getClusterHelper().getHCatClient();
        bundle = new Bundle(BundleUtil.readHCat2Bundle(), cluster);
        bundle.generateUniqueBundle();
        bundle.submitClusters(prism);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws HCatException {
        bundle.deleteBundle(prism);
        cli.dropTable(dBName, tableName, true);
    }

    @Test(enabled = true, dataProvider = "loopBelow", timeOut = 900000, groups = "embedded")
    public void testHCatRetention(int retentionPeriod, RetentionUnit retentionUnit,
                                  FeedType feedType) throws Exception {

        /*the hcatalog table that is created changes tablename characters to lowercase. So the
          name in the feed should be the same.*/
        tableName = String.format("testhcatretention_%s_%d", retentionUnit.getValue(),
            retentionPeriod);
        createPartitionedTable(cli, dBName, tableName, baseTestHDFSDir, feedType);
        FeedMerlin feedElement = new FeedMerlin(bundle.getInputFeedFromBundle());
        feedElement.setTableValue(dBName, tableName, feedType.getHcatPathValue());
        feedElement
            .setRetentionValue(retentionUnit.getValue() + "(" + retentionPeriod + ")");
        if (retentionPeriod <= 0) {
            AssertUtil.assertFailed(prism.getFeedHelper()
                .submitEntity(URLS.SUBMIT_URL, bundle.getInputFeedFromBundle()));
        } else {
            final DateTime dataStartTime = new DateTime(
                feedElement.getClusters().getClusters().get(0).getValidity().getStart(),
                DateTimeZone.UTC).withSecondOfMinute(0);
            final DateTime dataEndTime = new DateTime(
                feedElement.getClusters().getClusters().get(0).getValidity().getEnd(),
                DateTimeZone.UTC).withSecondOfMinute(0);
            final List<DateTime> dataDates =
                TimeUtil.getDatesOnEitherSide(dataStartTime, dataEndTime, feedType);
            final List<String> dataDateStrings = TimeUtil.convertDatesToString(dataDates,
                    feedType.getFormatter());
            AssertUtil.checkForListSizes(dataDates, dataDateStrings);
            final List<String> dataFolders = HadoopUtil.flattenAndPutDataInFolder(clusterFS,
                OSUtil.OOZIE_EXAMPLE_INPUT_LATE_INPUT, baseTestHDFSDir, dataDateStrings);
            addPartitionsToExternalTable(cli, dBName, tableName, feedType, dataDates, dataFolders);
            List<String> initialData =
                getHadoopDataFromDir(clusterFS, baseTestHDFSDir, testDir, feedType);
            List<HCatPartition> initialPtnList = cli.getPartitions(dBName, tableName);
            AssertUtil.checkForListSizes(initialData, initialPtnList);

            AssertUtil.assertSucceeded(prism.getFeedHelper()
                .submitAndSchedule(URLS.SUBMIT_AND_SCHEDULE_URL, feedElement.toString()));
            final String bundleId = OozieUtil.getBundles(clusterOC, feedElement.getName(),
                EntityType.FEED).get(0);
            OozieUtil.waitForRetentionWorkflowToSucceed(bundleId, clusterOC);
            AssertUtil.assertSucceeded(prism.getFeedHelper().suspend(URLS.SUSPEND_URL,
                feedElement.toString()));

            List<String> expectedOutput = getExpectedOutput(retentionPeriod, retentionUnit,
                feedType, new DateTime(DateTimeZone.UTC), initialData);
            List<String> finalData = getHadoopDataFromDir(clusterFS, baseTestHDFSDir, testDir,
                feedType);
            List<HCatPartition> finalPtnList = cli.getPartitions(dBName, tableName);

            logger.info("checking expectedOutput and finalPtnList");
            AssertUtil.checkForListSizes(expectedOutput, finalPtnList);
            logger.info("checking expectedOutput and finalData");
            AssertUtil.checkForListSizes(expectedOutput, finalData);
            logger.info("finalData = " + finalData);
            logger.info("expectedOutput = " + expectedOutput);
            Assert.assertTrue(Arrays.deepEquals(finalData.toArray(new String[finalData.size()]),
                    expectedOutput.toArray(new String[expectedOutput.size()])),
                "expectedOutput and finalData don't match");
        }
    }

    private static List<String> getHadoopDataFromDir(FileSystem fs, String hadoopPath,
                                                     String dir, FeedType feedType)
        throws IOException {
        List<String> finalResult = new ArrayList<String>();
        final int dirDepth = feedType.getDirDepth();

        List<Path> results = HadoopUtil.getAllDirsRecursivelyHDFS(fs,
            new Path(hadoopPath), dirDepth);

        for (Path result : results) {
            int pathDepth = result.toString().split(dir)[1].split("/").length - 1;
            if (pathDepth == dirDepth) {
                finalResult.add(result.toString().split(dir)[1]);
            }
        }

        return finalResult;
    }

    /**
     * Get the expected output after retention is applied
     *
     * @param retentionPeriod retention period
     * @param retentionUnit   retention unit
     * @param feedType        feed type
     * @param endDateUTC      end date of retention
     * @param inputData       input data on which retention was applied
     * @return expected output of the retention
     */
    private static List<String> getExpectedOutput(int retentionPeriod,
                                                  RetentionUnit retentionUnit,
                                                  FeedType feedType,
                                                  DateTime endDateUTC,
                                                  List<String> inputData) {
        List<String> finalData = new ArrayList<String>();

        //convert the end date to the same format
        final String endLimit =
            feedType.getFormatter().print(retentionUnit.minusTime(endDateUTC, retentionPeriod));
        //now to actually check!
        for (String testDate : inputData) {
            if (testDate.compareTo(endLimit) >= 0) {
                finalData.add(testDate);
            }
        }
        return finalData;
    }

    private static void createPartitionedTable(HCatClient client, String dbName, String tableName,
                                               String tableLoc, FeedType dataType)
        throws HCatException {
        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        ArrayList<HCatFieldSchema> ptnCols = new ArrayList<HCatFieldSchema>();

        //client.dropDatabase("sample_db", true, HCatClient.DropDBMode.CASCADE);

        cols.add(HCatUtil.getStringSchema("id", "id comment"));
        cols.add(HCatUtil.getStringSchema("value", "value comment"));

        switch (dataType) {
            case MINUTELY:
                ptnCols.add(
                    HCatUtil.getStringSchema("minute", "min prt"));
            case HOURLY:
                ptnCols.add(
                    HCatUtil.getStringSchema("hour", "hour prt"));
            case DAILY:
                ptnCols.add(HCatUtil.getStringSchema("day", "day prt"));
            case MONTHLY:
                ptnCols.add(
                    HCatUtil.getStringSchema("month", "month prt"));
            case YEARLY:
                ptnCols.add(
                    HCatUtil.getStringSchema("year", "year prt"));
            default:
                break;
        }
        HCatCreateTableDesc tableDesc = HCatCreateTableDesc
            .create(dbName, tableName, cols)
            .fileFormat("rcfile")
            .ifNotExists(true)
            .partCols(ptnCols)
            .isTableExternal(true)
            .location(tableLoc)
            .build();
        client.dropTable(dbName, tableName, true);
        client.createTable(tableDesc);
    }

    private static void addPartitionsToExternalTable(HCatClient client, String dbName,
                                                     String tableName, FeedType feedType,
                                                     List<DateTime> dataDates,
                                                     List<String> dataFolders)
        throws HCatException {
        //Adding specific partitions that map to an external location
        Map<String, String> ptn = new HashMap<String, String>();
        for (int i = 0; i < dataDates.size(); ++i) {
            final String dataFolder = dataFolders.get(i);
            final DateTime dataDate = dataDates.get(i);
            switch (feedType) {
                case MINUTELY:
                    ptn.put("minute", "" + dataDate.getMinuteOfHour());
                case HOURLY:
                    ptn.put("hour", "" + dataDate.getHourOfDay());
                case DAILY:
                    ptn.put("day", "" + dataDate.getDayOfMonth());
                case MONTHLY:
                    ptn.put("month", "" + dataDate.getMonthOfYear());
                case YEARLY:
                    ptn.put("year", "" + dataDate.getYear());
                    break;
                default:
                    Assert.fail("Unexpected feedType = " + feedType);
            }
            //Each HCat partition maps to a directory, not to a file
            HCatAddPartitionDesc addPtn = HCatAddPartitionDesc.create(dbName,
                tableName, dataFolder, ptn).build();
            client.addPartition(addPtn);
            ptn.clear();
        }
    }

    @DataProvider(name = "loopBelow")
    public Object[][] getTestData(Method m) {
        RetentionUnit[] retentionUnits = new RetentionUnit[]{RetentionUnit.HOURS, RetentionUnit.DAYS,
            RetentionUnit.MONTHS};// "minutes","years",
        Integer[] periods = new Integer[]{7, 824, 43}; // a negative value like -4 should be covered
        // in validation scenarios.
        FeedType[] dataTypes =
            new FeedType[]{
                //disabling since falcon has support is for only for single hcat partition
                //FeedType.DAILY, FeedType.MINUTELY, FeedType.HOURLY, FeedType.MONTHLY,
                FeedType.YEARLY};
        return MathUtil.crossProduct(periods, retentionUnits, dataTypes);
    }

}
