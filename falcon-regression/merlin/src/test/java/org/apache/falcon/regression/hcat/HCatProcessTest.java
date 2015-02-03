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

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.process.EngineType;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HCatUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.api.HCatAddPartitionDesc;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatCreateTableDesc;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * Tests falcon processes that run hive scripts.
 */
@Test(groups = "embedded")
public class HCatProcessTest extends BaseTestClass {
    private static final Logger LOGGER = Logger.getLogger(HCatProcessTest.class);
    private final ColoHelper cluster = servers.get(0);
    private final FileSystem clusterFS = serverFS.get(0);
    private final OozieClient clusterOC = serverOC.get(0);
    private HCatClient clusterHC;

    private final String baseTestHDFSDir = cleanAndGetTestDir();
    private final String hiveScriptDir = baseTestHDFSDir + "/hive";
    private final String hiveScriptFile = hiveScriptDir + "/script.hql";
    private final String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private final String hiveScriptFileNonHCatInput = hiveScriptDir + "/script_non_hcat_input.hql";
    private final String hiveScriptFileNonHCatOutput =
        hiveScriptDir + "/script_non_hcat_output.hql";
    private final String hiveScriptTwoHCatInputOneHCatOutput =
        hiveScriptDir + "/script_two_hcat_input_one_hcat_output.hql";
    private final String hiveScriptOneHCatInputTwoHCatOutput =
        hiveScriptDir + "/script_one_hcat_input_two_hcat_output.hql";
    private final String hiveScriptTwoHCatInputTwoHCatOutput =
        hiveScriptDir + "/script_two_hcat_input_two_hcat_output.hql";
    private final String inputHDFSDir = baseTestHDFSDir + "/input";
    private final String inputHDFSDir2 = baseTestHDFSDir + "/input2";
    private final String outputHDFSDir = baseTestHDFSDir + "/output";
    private final String outputHDFSDir2 = baseTestHDFSDir + "/output2";

    private final String dbName = "default";
    private final String inputTableName = "hcatprocesstest_input_table";
    private final String inputTableName2 = "hcatprocesstest_input_table2";
    private final String outputTableName = "hcatprocesstest_output_table";
    private final String outputTableName2 = "hcatprocesstest_output_table2";
    private final String col1Name = "id";
    private final String col2Name = "value";
    private final String partitionColumn = "dt";

    private final String hcatDir = OSUtil.getPath("src", "test", "resources", "hcat");
    private final String localHCatData = OSUtil.getPath(hcatDir, "data");
    private final String hiveScript = OSUtil.getPath(hcatDir, "hivescript");
    private final String startDate = "2010-01-01T20:00Z";
    private final String endDate = "2010-01-01T21:10Z";

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        clusterHC = cluster.getClusterHelper().getHCatClient();
        bundles[0] = BundleUtil.readHCatBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].setProcessWorkflow(hiveScriptFile, EngineType.HIVE);
        bundles[0].setClusterInterface(Interfacetype.REGISTRY,
            cluster.getClusterHelper().getHCatEndpoint());

        HadoopUtil.deleteDirIfExists(baseTestHDFSDir, clusterFS);
        HadoopUtil.uploadDir(clusterFS, hiveScriptDir, hiveScript);
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        HadoopUtil.recreateDir(clusterFS, outputHDFSDir);
        HadoopUtil.recreateDir(clusterFS, outputHDFSDir2);
        clusterHC.dropTable(dbName, inputTableName, true);
        clusterHC.dropTable(dbName, inputTableName2, true);
        clusterHC.dropTable(dbName, outputTableName, true);
        clusterHC.dropTable(dbName, outputTableName2, true);
    }

    @DataProvider
    public String[][] generateSeparators() {
        //disabling till FALCON-372 is fixed
        //return new String[][] {{"-"}, {"/"}};
        return new String[][]{{"-", }, };
    }

    @Test(dataProvider = "generateSeparators")
    public void oneHCatInputOneHCatOutput(String separator) throws Exception {
        /* upload data and create partition */
        final String datePattern =
            StringUtils.join(new String[]{"yyyy", "MM", "dd", "HH"}, separator);
        List<String> dataDates = getDatesList(startDate, endDate, datePattern, 60);

        final List<String> dataset = HadoopUtil
            .flattenAndPutDataInFolder(clusterFS, localHCatData, inputHDFSDir, dataDates);

        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        cols.add(HCatUtil.getStringSchema(col1Name, col1Name + " comment"));
        cols.add(HCatUtil.getStringSchema(col2Name, col2Name + " comment"));
        ArrayList<HCatFieldSchema> partitionCols = new ArrayList<HCatFieldSchema>();

        partitionCols.add(HCatUtil.getStringSchema(partitionColumn, partitionColumn + " partition"));
        clusterHC.createTable(HCatCreateTableDesc
                .create(dbName, inputTableName, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(inputHDFSDir)
                .build());

        clusterHC.createTable(HCatCreateTableDesc
                .create(dbName, outputTableName, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(outputHDFSDir)
                .build());

        addPartitionsToTable(dataDates, dataset, "dt", dbName, inputTableName);

        final String tableUriPartitionFragment = StringUtils.join(
            new String[]{"#dt=${YEAR}", "${MONTH}", "${DAY}", "${HOUR}"}, separator);
        String inputTableUri =
            "catalog:" + dbName + ":" + inputTableName + tableUriPartitionFragment;
        bundles[0].setInputFeedTableUri(inputTableUri);
        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setInputFeedValidity(startDate, endDate);
        String outputTableUri =
            "catalog:" + dbName + ":" + outputTableName + tableUriPartitionFragment;
        bundles[0].setOutputFeedTableUri(outputTableUri);
        bundles[0].setOutputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setOutputFeedValidity(startDate, endDate);

        bundles[0].setProcessValidity(startDate, endDate);
        bundles[0].setProcessPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setProcessInputStartEnd("now(0,0)", "now(0,0)");
        bundles[0].submitFeedsScheduleProcess();

        InstanceUtil.waitTillInstanceReachState(
            clusterOC, bundles[0].getProcessName(), 1, CoordinatorAction.Status.SUCCEEDED,
            EntityType.PROCESS);

        AssertUtil.checkContentSize(inputHDFSDir + "/" + dataDates.get(0),
            outputHDFSDir + "/dt=" + dataDates.get(0), clusterFS);
    }

    @Test(dataProvider = "generateSeparators")
    public void twoHCatInputOneHCatOutput(String separator) throws Exception {
        /* upload data and create partition */
        final String datePattern =
            StringUtils.join(new String[]{"yyyy", "MM", "dd", "HH"}, separator);
        List<String> dataDates = getDatesList(startDate, endDate, datePattern, 60);

        final List<String> dataset = HadoopUtil
            .flattenAndPutDataInFolder(clusterFS, localHCatData, inputHDFSDir, dataDates);
        final List<String> dataset2 = HadoopUtil
            .flattenAndPutDataInFolder(clusterFS, localHCatData, inputHDFSDir2, dataDates);

        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        cols.add(HCatUtil.getStringSchema(col1Name, col1Name + " comment"));
        cols.add(HCatUtil.getStringSchema(col2Name, col2Name + " comment"));
        ArrayList<HCatFieldSchema> partitionCols = new ArrayList<HCatFieldSchema>();

        partitionCols.add(HCatUtil.getStringSchema(partitionColumn, partitionColumn + " partition"));
        clusterHC.createTable(HCatCreateTableDesc
                .create(dbName, inputTableName, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(inputHDFSDir)
                .build());

        clusterHC.createTable(HCatCreateTableDesc
                .create(dbName, inputTableName2, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(inputHDFSDir2)
                .build());

        clusterHC.createTable(HCatCreateTableDesc
                .create(dbName, outputTableName, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(outputHDFSDir)
                .build());

        addPartitionsToTable(dataDates, dataset, "dt", dbName, inputTableName);
        addPartitionsToTable(dataDates, dataset2, "dt", dbName, inputTableName2);

        final String tableUriPartitionFragment = StringUtils.join(
            new String[]{"#dt=${YEAR}", "${MONTH}", "${DAY}", "${HOUR}"}, separator);
        String inputTableUri =
            "catalog:" + dbName + ":" + inputTableName + tableUriPartitionFragment;
        String inputTableUri2 =
            "catalog:" + dbName + ":" + inputTableName2 + tableUriPartitionFragment;
        bundles[0].setInputFeedTableUri(inputTableUri);
        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setInputFeedValidity(startDate, endDate);
        final String inputFeed1 = bundles[0].getInputFeedFromBundle();
        final String inputFeed2Name = Util.readEntityName(inputFeed1) + "-second";

        FeedMerlin feedObj = new FeedMerlin(inputFeed1);
        feedObj.setName(inputFeed2Name);
        feedObj.getTable().setUri(inputTableUri2);

        bundles[0].addInputFeedToBundle("inputData2", feedObj);

        String outputTableUri =
            "catalog:" + dbName + ":" + outputTableName + tableUriPartitionFragment;
        bundles[0].setOutputFeedTableUri(outputTableUri);
        bundles[0].setOutputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setOutputFeedValidity(startDate, endDate);

        bundles[0].setProcessValidity(startDate, endDate);
        bundles[0].setProcessPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setProcessInputStartEnd("now(0,0)", "now(0,0)");
        bundles[0].setProcessWorkflow(hiveScriptTwoHCatInputOneHCatOutput, EngineType.HIVE);
        bundles[0].submitFeedsScheduleProcess();

        InstanceUtil.waitTillInstanceReachState(
            clusterOC, bundles[0].getProcessName(), 1, CoordinatorAction.Status.SUCCEEDED,
            EntityType.PROCESS);

        final ContentSummary inputContentSummary =
            clusterFS.getContentSummary(new Path(inputHDFSDir + "/" + dataDates.get(0)));
        final ContentSummary inputContentSummary2 =
            clusterFS.getContentSummary(new Path(inputHDFSDir2 + "/" + dataDates.get(0)));
        final ContentSummary outputContentSummary =
            clusterFS.getContentSummary(new Path(outputHDFSDir + "/dt=" + dataDates.get(0)));
        LOGGER.info("inputContentSummary = " + inputContentSummary.toString(false));
        LOGGER.info("inputContentSummary2 = " + inputContentSummary2.toString(false));
        LOGGER.info("outputContentSummary = " + outputContentSummary.toString(false));
        Assert.assertEquals(inputContentSummary.getLength() + inputContentSummary2.getLength(),
            outputContentSummary.getLength(),
            "Unexpected size of the output.");
    }

    @Test(dataProvider = "generateSeparators")
    public void oneHCatInputTwoHCatOutput(String separator) throws Exception {
        /* upload data and create partition */
        final String datePattern =
            StringUtils.join(new String[]{"yyyy", "MM", "dd", "HH"}, separator);
        List<String> dataDates = getDatesList(startDate, endDate, datePattern, 60);

        final List<String> dataset = HadoopUtil
            .flattenAndPutDataInFolder(clusterFS, localHCatData, inputHDFSDir, dataDates);

        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        cols.add(HCatUtil.getStringSchema(col1Name, col1Name + " comment"));
        cols.add(HCatUtil.getStringSchema(col2Name, col2Name + " comment"));
        ArrayList<HCatFieldSchema> partitionCols = new ArrayList<HCatFieldSchema>();

        partitionCols.add(HCatUtil.getStringSchema(partitionColumn, partitionColumn + " partition"));
        clusterHC.createTable(HCatCreateTableDesc
                .create(dbName, inputTableName, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(inputHDFSDir)
                .build());

        clusterHC.createTable(HCatCreateTableDesc
                .create(dbName, outputTableName, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(outputHDFSDir)
                .build());

        clusterHC.createTable(HCatCreateTableDesc
                .create(dbName, outputTableName2, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(outputHDFSDir2)
                .build());

        addPartitionsToTable(dataDates, dataset, "dt", dbName, inputTableName);

        final String tableUriPartitionFragment = StringUtils.join(
            new String[]{"#dt=${YEAR}", "${MONTH}", "${DAY}", "${HOUR}"}, separator);
        String inputTableUri =
            "catalog:" + dbName + ":" + inputTableName + tableUriPartitionFragment;
        bundles[0].setInputFeedTableUri(inputTableUri);
        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setInputFeedValidity(startDate, endDate);
        String outputTableUri =
            "catalog:" + dbName + ":" + outputTableName + tableUriPartitionFragment;
        String outputTableUri2 =
            "catalog:" + dbName + ":" + outputTableName2 + tableUriPartitionFragment;
        bundles[0].setOutputFeedTableUri(outputTableUri);
        bundles[0].setOutputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setOutputFeedValidity(startDate, endDate);
        final String outputFeed1 = bundles[0].getInputFeedFromBundle();
        final String outputFeed2Name = Util.readEntityName(outputFeed1) + "-second";
        FeedMerlin feedObj = new FeedMerlin(outputFeed1);
        feedObj.setName(outputFeed2Name);
        feedObj.getTable().setUri(outputTableUri2);
        bundles[0].addOutputFeedToBundle("outputData2", feedObj);

        bundles[0].setProcessValidity(startDate, endDate);
        bundles[0].setProcessPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setProcessInputStartEnd("now(0,0)", "now(0,0)");
        bundles[0].setProcessWorkflow(hiveScriptOneHCatInputTwoHCatOutput, EngineType.HIVE);
        bundles[0].submitFeedsScheduleProcess();

        InstanceUtil.waitTillInstanceReachState(
            clusterOC, bundles[0].getProcessName(), 1, CoordinatorAction.Status.SUCCEEDED,
            EntityType.PROCESS);

        AssertUtil.checkContentSize(inputHDFSDir + "/" + dataDates.get(0),
            outputHDFSDir + "/dt=" + dataDates.get(0), clusterFS);
        AssertUtil.checkContentSize(inputHDFSDir + "/" + dataDates.get(0),
            outputHDFSDir2 + "/dt=" + dataDates.get(0), clusterFS);
    }


    @Test(dataProvider = "generateSeparators")
    public void twoHCatInputTwoHCatOutput(String separator) throws Exception {
        /* upload data and create partition */
        final String datePattern =
            StringUtils.join(new String[]{"yyyy", "MM", "dd", "HH"}, separator);
        List<String> dataDates = getDatesList(startDate, endDate, datePattern, 60);

        final List<String> dataset = HadoopUtil
            .flattenAndPutDataInFolder(clusterFS, localHCatData, inputHDFSDir, dataDates);
        final List<String> dataset2 = HadoopUtil
            .flattenAndPutDataInFolder(clusterFS, localHCatData, inputHDFSDir2, dataDates);

        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        cols.add(HCatUtil.getStringSchema(col1Name, col1Name + " comment"));
        cols.add(HCatUtil.getStringSchema(col2Name, col2Name + " comment"));
        ArrayList<HCatFieldSchema> partitionCols = new ArrayList<HCatFieldSchema>();

        partitionCols.add(HCatUtil.getStringSchema(partitionColumn, partitionColumn + " partition"));
        clusterHC.createTable(HCatCreateTableDesc
                .create(dbName, inputTableName, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(inputHDFSDir)
                .build());

        clusterHC.createTable(HCatCreateTableDesc
                .create(dbName, inputTableName2, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(inputHDFSDir2)
                .build());

        clusterHC.createTable(HCatCreateTableDesc
                .create(dbName, outputTableName, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(outputHDFSDir)
                .build());

        clusterHC.createTable(HCatCreateTableDesc
                .create(dbName, outputTableName2, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(outputHDFSDir2)
                .build());

        addPartitionsToTable(dataDates, dataset, "dt", dbName, inputTableName);
        addPartitionsToTable(dataDates, dataset2, "dt", dbName, inputTableName2);

        final String tableUriPartitionFragment = StringUtils.join(
            new String[]{"#dt=${YEAR}", "${MONTH}", "${DAY}", "${HOUR}"}, separator);
        String inputTableUri =
            "catalog:" + dbName + ":" + inputTableName + tableUriPartitionFragment;
        String inputTableUri2 =
            "catalog:" + dbName + ":" + inputTableName2 + tableUriPartitionFragment;
        bundles[0].setInputFeedTableUri(inputTableUri);
        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setInputFeedValidity(startDate, endDate);
        final String inputFeed1 = bundles[0].getInputFeedFromBundle();
        final String inputFeed2Name = Util.readEntityName(inputFeed1) + "-second";
        FeedMerlin feedObj = new FeedMerlin(inputFeed1);
        feedObj.setName(inputFeed2Name);
        feedObj.getTable().setUri(inputTableUri2);
        bundles[0].addInputFeedToBundle("inputData2", feedObj);

        String outputTableUri =
            "catalog:" + dbName + ":" + outputTableName + tableUriPartitionFragment;
        String outputTableUri2 =
            "catalog:" + dbName + ":" + outputTableName2 + tableUriPartitionFragment;
        bundles[0].setOutputFeedTableUri(outputTableUri);
        bundles[0].setOutputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setOutputFeedValidity(startDate, endDate);
        final String outputFeed1 = bundles[0].getOutputFeedFromBundle();
        final String outputFeed2Name = Util.readEntityName(outputFeed1) + "-second";
        FeedMerlin feedObj2 = new FeedMerlin(outputFeed1);
        feedObj2.setName(outputFeed2Name);
        feedObj2.getTable().setUri(outputTableUri2);
        bundles[0].addOutputFeedToBundle("outputData2", feedObj2);
        bundles[0].setProcessValidity(startDate, endDate);
        bundles[0].setProcessPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setProcessInputStartEnd("now(0,0)", "now(0,0)");
        bundles[0].setProcessWorkflow(hiveScriptTwoHCatInputTwoHCatOutput, EngineType.HIVE);
        bundles[0].submitFeedsScheduleProcess();

        InstanceUtil.waitTillInstanceReachState(
            clusterOC, bundles[0].getProcessName(), 1, CoordinatorAction.Status.SUCCEEDED,
            EntityType.PROCESS);

        final ContentSummary inputContentSummary =
            clusterFS.getContentSummary(new Path(inputHDFSDir + "/" + dataDates.get(0)));
        final ContentSummary inputContentSummary2 =
            clusterFS.getContentSummary(new Path(inputHDFSDir2 + "/" + dataDates.get(0)));
        final ContentSummary outputContentSummary =
            clusterFS.getContentSummary(new Path(outputHDFSDir + "/dt=" + dataDates.get(0)));
        final ContentSummary outputContentSummary2 =
            clusterFS.getContentSummary(new Path(outputHDFSDir2 + "/dt=" + dataDates.get(0)));
        LOGGER.info("inputContentSummary = " + inputContentSummary.toString(false));
        LOGGER.info("inputContentSummary2 = " + inputContentSummary2.toString(false));
        LOGGER.info("outputContentSummary = " + outputContentSummary.toString(false));
        LOGGER.info("outputContentSummary2 = " + outputContentSummary2.toString(false));
        Assert.assertEquals(inputContentSummary.getLength() + inputContentSummary2.getLength(),
            outputContentSummary.getLength(),
            "Unexpected size of the output.");
        Assert.assertEquals(inputContentSummary.getLength() + inputContentSummary2.getLength(),
            outputContentSummary2.getLength(),
            "Unexpected size of the output.");
    }


    @Test(dataProvider = "generateSeparators")
    public void oneHCatInputOneNonHCatOutput(String separator) throws Exception {
        /* upload data and create partition */
        final String datePattern =
            StringUtils.join(new String[]{"yyyy", "MM", "dd", "HH"}, separator);
        List<String> dataDates = getDatesList(startDate, endDate, datePattern, 60);

        final List<String> dataset = HadoopUtil
            .flattenAndPutDataInFolder(clusterFS, localHCatData, inputHDFSDir, dataDates);

        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        cols.add(HCatUtil.getStringSchema(col1Name, col1Name + " comment"));
        cols.add(HCatUtil.getStringSchema(col2Name, col2Name + " comment"));
        ArrayList<HCatFieldSchema> partitionCols = new ArrayList<HCatFieldSchema>();

        partitionCols.add(HCatUtil.getStringSchema(partitionColumn, partitionColumn + " partition"));
        clusterHC.createTable(HCatCreateTableDesc
                .create(dbName, inputTableName, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(inputHDFSDir)
                .build());

        addPartitionsToTable(dataDates, dataset, "dt", dbName, inputTableName);

        final String tableUriPartitionFragment = StringUtils.join(
            new String[]{"#dt=${YEAR}", "${MONTH}", "${DAY}", "${HOUR}"}, separator);
        String inputTableUri =
            "catalog:" + dbName + ":" + inputTableName + tableUriPartitionFragment;
        bundles[0].setInputFeedTableUri(inputTableUri);
        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setInputFeedValidity(startDate, endDate);

        //
        FeedMerlin nonHCatFeed = new FeedMerlin(BundleUtil.readELBundle().getOutputFeedFromBundle());
        final String outputFeedName = bundles[0].getOutputFeedNameFromBundle();
        nonHCatFeed.setName(outputFeedName);
        final List<String> clusterNames = bundles[0].getClusterNames();
        Assert.assertEquals(clusterNames.size(), 1, "Expected only one cluster in the bundle.");
        nonHCatFeed.setClusterNameInFeed(clusterNames.get(0), 0);
        bundles[0].writeFeedElement(nonHCatFeed, outputFeedName);
        bundles[0].setOutputFeedLocationData(outputHDFSDir + "/"
            + StringUtils.join(new String[]{"${YEAR}", "${MONTH}", "${DAY}", "${HOUR}"}, separator));
        bundles[0].setOutputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setOutputFeedValidity(startDate, endDate);

        bundles[0].setProcessValidity(startDate, endDate);
        bundles[0].setProcessPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setProcessInputStartEnd("now(0,0)", "now(0,0)");

        bundles[0].setProcessWorkflow(hiveScriptFileNonHCatOutput, EngineType.HIVE);
        bundles[0].submitFeedsScheduleProcess();

        InstanceUtil.waitTillInstanceReachState(
            clusterOC, bundles[0].getProcessName(), 1, CoordinatorAction.Status.SUCCEEDED,
            EntityType.PROCESS);

        AssertUtil.checkContentSize(inputHDFSDir + "/" + dataDates.get(0),
            outputHDFSDir + "/" + dataDates.get(0), clusterFS);
    }

    @Test(dataProvider = "generateSeparators")
    public void oneNonCatInputOneHCatOutput(String separator) throws Exception {
        /* upload data and create partition */
        final String datePattern =
            StringUtils.join(new String[]{"yyyy", "MM", "dd", "HH"}, separator);
        List<String> dataDates = getDatesList(startDate, endDate, datePattern, 60);

        final List<String> dataset = HadoopUtil.
            flattenAndPutDataInFolder(clusterFS, localHCatData, inputHDFSDir, dataDates);

        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        cols.add(HCatUtil.getStringSchema(col1Name, col1Name + " comment"));
        cols.add(HCatUtil.getStringSchema(col2Name, col2Name + " comment"));
        ArrayList<HCatFieldSchema> partitionCols = new ArrayList<HCatFieldSchema>();

        partitionCols.add(HCatUtil.getStringSchema(partitionColumn, partitionColumn + " partition"));
        clusterHC.createTable(HCatCreateTableDesc
                .create(dbName, outputTableName, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(outputHDFSDir)
                .build());

        FeedMerlin nonHCatFeed = new FeedMerlin(BundleUtil.readELBundle().getInputFeedFromBundle());
        final String inputFeedName = bundles[0].getInputFeedNameFromBundle();
        nonHCatFeed.setName(inputFeedName);
        final List<String> clusterNames = bundles[0].getClusterNames();
        Assert.assertEquals(clusterNames.size(), 1, "Expected only one cluster in the bundle.");
        nonHCatFeed.setClusterNameInFeed(clusterNames.get(0), 0);
        bundles[0].writeFeedElement(nonHCatFeed, inputFeedName);
        bundles[0].setInputFeedDataPath(inputHDFSDir + "/"
            + StringUtils.join(new String[]{"${YEAR}", "${MONTH}", "${DAY}", "${HOUR}"}, separator));
        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setInputFeedValidity(startDate, endDate);

        final String tableUriPartitionFragment = StringUtils.join(
            new String[]{"#dt=${YEAR}", "${MONTH}", "${DAY}", "${HOUR}"}, separator);
        String outputTableUri =
            "catalog:" + dbName + ":" + outputTableName + tableUriPartitionFragment;
        bundles[0].setOutputFeedTableUri(outputTableUri);
        bundles[0].setOutputFeedPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setOutputFeedValidity(startDate, endDate);

        bundles[0].setProcessWorkflow(hiveScriptFileNonHCatInput, EngineType.HIVE);
        bundles[0].setProcessValidity(startDate, endDate);
        bundles[0].setProcessPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setProcessInputStartEnd("now(0,0)", "now(0,0)");
        bundles[0].submitFeedsScheduleProcess();

        InstanceUtil.waitTillInstanceReachState(
            clusterOC, bundles[0].getProcessName(), 1, CoordinatorAction.Status.SUCCEEDED,
            EntityType.PROCESS);

        AssertUtil.checkContentSize(inputHDFSDir + "/" + dataDates.get(0),
            outputHDFSDir + "/dt=" + dataDates.get(0), clusterFS);
    }

    private void addPartitionsToTable(List<String> partitions, List<String> partitionLocations,
                                      String partitionCol,
                                      String databaseName, String tableName) throws HCatException {
        Assert.assertEquals(partitions.size(), partitionLocations.size(),
            "Number of locations is not same as number of partitions.");
        final List<HCatAddPartitionDesc> partitionDesc = new ArrayList<HCatAddPartitionDesc>();
        for (int i = 0; i < partitions.size(); ++i) {
            final String partition = partitions.get(i);
            final Map<String, String> onePartition = new HashMap<String, String>();
            onePartition.put(partitionCol, partition);
            final String partitionLoc = partitionLocations.get(i);
            partitionDesc.add(
                HCatAddPartitionDesc.create(databaseName, tableName, partitionLoc, onePartition).build());
        }
        clusterHC.addPartitions(partitionDesc);
    }

    public static List<String> getDatesList(String startDate, String endDate, String datePattern,
                                            int skipMinutes) {
        DateTime startDateJoda = new DateTime(TimeUtil.oozieDateToDate(startDate));
        DateTime endDateJoda = new DateTime(TimeUtil.oozieDateToDate(endDate));
        DateTimeFormatter formatter = DateTimeFormat.forPattern(datePattern);
        LOGGER.info("generating data between " + formatter.print(startDateJoda) + " and "
            + formatter.print(endDateJoda));
        List<String> dates = new ArrayList<String>();
        dates.add(formatter.print(startDateJoda));
        while (!startDateJoda.isAfter(endDateJoda)) {
            startDateJoda = startDateJoda.plusMinutes(skipMinutes);
            dates.add(formatter.print(startDateJoda));
        }
        return dates;
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }
}
