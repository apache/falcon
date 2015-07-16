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

package org.apache.falcon.regression.prism;

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests with partitions as expression language variables.
 */
@Test(groups = "embedded")
public class ProcessPartitionExpVariableTest extends BaseTestClass {
    private static final Logger LOGGER = Logger.getLogger(ProcessPartitionExpVariableTest.class);

    private ColoHelper cluster = servers.get(0);
    private FileSystem clusterFS = serverFS.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private String baseTestDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        removeTestClassEntities();
        HadoopUtil.deleteDirIfExists(baseTestDir, clusterFS);
    }

    /**
     * Test case: set 1 optional and 1 compulsory input for process. Set partitions for each
     * input as expression language variable linked with process properties. Check that process
     * runs fine with partition provided for compulsory input as exp variable and succeeds in
     * spite of nonexistent partition provided for optional input.
     *
     * @throws Exception
     */
    @Test(enabled = true)
    public void optionalCompulsoryPartition() throws Exception {
        String startTime = TimeUtil.getTimeWrtSystemTime(-4);
        String endTime = TimeUtil.getTimeWrtSystemTime(30);

        bundles[0].generateRequiredBundle(1, 2, 1, baseTestDir, 1, startTime, endTime);
        bundles[0].setProcessInputNames("inputData0", "inputData");
        bundles[0].addProcessProperty("var1", "hardCoded");
        bundles[0].setProcessInputPartition("${var1}", "${fileTime}");

        for (int i = 0; i < bundles[0].getDataSets().size(); i++) {
            LOGGER.info(Util.prettyPrintXml(bundles[0].getDataSets().get(i)));
        }
        LOGGER.info(Util.prettyPrintXml(bundles[0].getProcessData()));
        bundles[0].submitAndScheduleBundle(prism, false);

        List<String> dataDates = generateDateAndOneDayAfter(
                TimeUtil.oozieDateToDate(TimeUtil.addMinsToTime(startTime, -25)),
                TimeUtil.oozieDateToDate(TimeUtil.addMinsToTime(endTime, 25)), 5);

        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, baseTestDir
            + "/input1/", dataDates);

        InstanceUtil.waitTillInstanceReachState(clusterOC, bundles[0].getProcessName(), 2,
            CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);
    }


    /**
     * Generates patterns of the form .../2014/03/06/21/57/2014-Mar-07 between two supplied dates.
     * There are two dates and the second date is one day after the first one
     *
     * @param startDate  start date
     * @param endDate    end date
     * @param minuteSkip interval with which directories are created
     * @return list of such dates
     */
    private static List<String> generateDateAndOneDayAfter(DateTime startDate, DateTime endDate,
                                                           int minuteSkip) {
        final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy/MM/dd/HH/mm/");
        final DateTimeFormatter formatter2 = DateTimeFormat.forPattern("yyyy-MMM-dd");
        LOGGER.info("generating data between " + formatter.print(startDate) + " and "
            + formatter.print(endDate));

        List<String> dates = new ArrayList<>();
        while (!startDate.isAfter(endDate)) {
            final DateTime nextDate = startDate.plusMinutes(minuteSkip);
            dates.add(formatter.print(nextDate) + formatter2.print(nextDate.plusDays(1)));
            if (minuteSkip == 0) {
                minuteSkip = 1;
            }
            startDate = nextDate;
        }
        return dates;
    }
}
