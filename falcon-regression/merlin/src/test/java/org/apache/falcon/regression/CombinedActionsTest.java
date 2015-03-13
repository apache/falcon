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

package org.apache.falcon.regression;

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.*;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatCreateTableDesc;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.joda.time.format.DateTimeFormat;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Test where a single workflow contains multiple actions.
 */

public class CombinedActionsTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private HCatClient clusterHC;

    private final String hiveTestDir = "/HiveData";
    private final String baseTestHDFSDir = cleanAndGetTestDir() + hiveTestDir;
    private final String inputHDFSDir = baseTestHDFSDir + "/input";
    private final String outputHDFSDir = baseTestHDFSDir + "/output";
    private String aggregateWorkflowDir = cleanAndGetTestDir() + "/aggregator";
    private static final Logger LOGGER = Logger.getLogger(CombinedActionsTest.class);
    private static final String HCATDIR = OSUtil.getPath("src", "test", "resources", "hcat");
    private static final String LOCALHCATDATA = OSUtil.getPath(HCATDIR, "data");
    public static final String DBNAME = "default";
    public static final String COL1NAME = "id";
    public static final String COL2NAME = "value";
    public static final String PARTITIONCOLUMN = "dt";
    private final String inputTableName = "combinedactionstest_input_table";
    private final String outputTableName = "combinedactionstest_output_table";

    private String pigMrTestDir = cleanAndGetTestDir() + "/pigMrData";
    private String inputPath = pigMrTestDir + "/input/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    private String outputPathPig = pigMrTestDir + "/output/pig/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    private String outputPathMr = pigMrTestDir + "/output/mr/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {

        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.OOZIE_COMBINED_ACTIONS);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp(Method method) throws Exception {
        LOGGER.info("test name: " + method.getName());
        clusterHC = cluster.getClusterHelper().getHCatClient();
        bundles[0] = BundleUtil.readCombinedActionsBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        AssertUtil.assertSucceeded(prism.getClusterHelper().submitEntity(bundles[0].getClusters().get(0)));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()throws Exception {
        removeTestClassEntities();
        clusterHC.dropTable(DBNAME, inputTableName, true);
        clusterHC.dropTable(DBNAME, outputTableName, true);
        HadoopUtil.deleteDirIfExists(pigMrTestDir, clusterFS);
    }

    /**
     *Schedule a process, for which the oozie workflow contains multiple actions like hive, mr, pig
     *The process should succeed. Fails right now due to: https://issues.apache.org/jira/browse/FALCON-670
     *
     * @throws Exception
    */

    @Test
    public void combinedMrPigHiveAction()throws Exception{

        //create data for pig, mr and hcat jobs
        final String startDate = "2010-01-01T20:00Z";
        final String endDate = "2010-01-02T04:00Z";

        String inputFeedMrPig = bundles[0].getFeed("sampleFeed1");
        FeedMerlin feedObj = new FeedMerlin(inputFeedMrPig);

        HadoopUtil.deleteDirIfExists(pigMrTestDir + "/input", clusterFS);
        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(startDate, endDate, 20);

        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, pigMrTestDir + "/input", dataDates);

        final String datePattern = StringUtils.join(new String[] { "yyyy", "MM", "dd", "HH", "mm"}, "-");
        dataDates = TimeUtil.getMinuteDatesOnEitherSide(startDate, endDate, 60, DateTimeFormat.forPattern(datePattern));

        final List<String> dataset = HadoopUtil.flattenAndPutDataInFolder(clusterFS, LOCALHCATDATA,
                inputHDFSDir, dataDates);

        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        cols.add(HCatUtil.getStringSchema(COL1NAME, COL1NAME + " comment"));
        cols.add(HCatUtil.getStringSchema(COL2NAME, COL2NAME + " comment"));
        ArrayList<HCatFieldSchema> partitionCols = new ArrayList<HCatFieldSchema>();

        partitionCols.add(HCatUtil.getStringSchema(PARTITIONCOLUMN, PARTITIONCOLUMN + " partition"));
        clusterHC.createTable(HCatCreateTableDesc
                .create(DBNAME, inputTableName, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(inputHDFSDir)
                .build());

        clusterHC.createTable(HCatCreateTableDesc
                .create(DBNAME, outputTableName, cols)
                .partCols(partitionCols)
                .ifNotExists(true)
                .isTableExternal(true)
                .location(outputHDFSDir)
                .build());

        HCatUtil.addPartitionsToTable(clusterHC, dataDates, dataset, "dt", DBNAME, inputTableName);

        final String tableUriPartitionFragment = StringUtils.join(
            new String[]{"#dt=${YEAR}", "${MONTH}", "${DAY}", "${HOUR}", "${MINUTE}"}, "-");
        String inputTableUri =
            "catalog:" + DBNAME + ":" + inputTableName + tableUriPartitionFragment;
        String outputTableUri =
            "catalog:" + DBNAME + ":" + outputTableName + tableUriPartitionFragment;

        //Set input and output feeds for bundle
        //input feed for both mr and pig jobs
        feedObj.setLocation(LocationType.DATA, inputPath);
        LOGGER.info(feedObj.toString());
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(feedObj.toString()));

        //output feed for pig jobs
        String outputFeedPig = bundles[0].getFeed("sampleFeed2");
        feedObj = new FeedMerlin(outputFeedPig);
        feedObj.setLocation(LocationType.DATA, outputPathPig);
        feedObj.setFrequency(new Frequency("5", Frequency.TimeUnit.minutes));
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(feedObj.toString()));

        //output feed for mr jobs
        String outputFeedMr = bundles[0].getFeed("sampleFeed3");
        feedObj = new FeedMerlin(outputFeedMr);
        feedObj.setLocation(LocationType.DATA, outputPathMr);
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(feedObj.toString()));

        //input feed for hcat jobs
        String inputHive = bundles[0].getFeed("sampleFeedHCat1");
        feedObj = new FeedMerlin(inputHive);
        feedObj.getTable().setUri(inputTableUri);
        feedObj.setFrequency(new Frequency("1", Frequency.TimeUnit.hours));
        feedObj.getClusters().getClusters().get(0).getValidity()
            .setStart(TimeUtil.oozieDateToDate(startDate).toDate());
        feedObj.getClusters().getClusters().get(0).getValidity()
            .setEnd(TimeUtil.oozieDateToDate(endDate).toDate());
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(feedObj.toString()));

        //output feed for hcat jobs
        String outputHive = bundles[0].getFeed("sampleFeedHCat2");
        feedObj = new FeedMerlin(outputHive);
        feedObj.getTable().setUri(outputTableUri);
        feedObj.setFrequency(new Frequency("1", Frequency.TimeUnit.hours));
        feedObj.getClusters().getClusters().get(0).getValidity()
            .setStart(TimeUtil.oozieDateToDate(startDate).toDate());
        feedObj.getClusters().getClusters().get(0).getValidity()
            .setEnd(TimeUtil.oozieDateToDate(endDate).toDate());
        AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(feedObj.toString()));

        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setProcessValidity(startDate, endDate);
        bundles[0].setProcessPeriodicity(1, Frequency.TimeUnit.hours);
        bundles[0].setProcessInputStartEnd("now(0,0)", "now(0,0)");
        AssertUtil.assertSucceeded(prism.getProcessHelper().submitAndSchedule(bundles[0].getProcessData()));
        InstanceUtil.waitTillInstanceReachState(clusterOC, bundles[0].getProcessName(),
            1, CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);
    }
}
