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

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.TestNGException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;


/**
 * EL Validations tests.
 */
@Test(groups = "embedded")
public class ELValidationsTest extends BaseTestClass {

    private ColoHelper cluster = servers.get(0);
    private static final Logger LOGGER = Logger.getLogger(ELValidationsTest.class);
    private String aggregateWorkflowDir = cleanAndGetTestDir() + "/aggregator";


    @Test(groups = {"0.1", "0.2"})
    public void startInstBeforeFeedStartToday02() throws Exception {
        String response =
            testWith("2009-02-02T20:00Z", "2011-12-31T00:00Z", "2009-02-02T20:00Z",
                "2011-12-31T00:00Z", "now(-40,0)", "currentYear(20,30,24,20)", false);
        validate(response);
    }

    @Test(groups = {"singleCluster"})
    public void startInstAfterFeedEnd() throws Exception {
        String response = testWith(null, null, null, null,
            "currentYear(10,0,22,0)", "now(4,20)", false);
        validate(response);
    }

    @Test(groups = {"singleCluster"})
    public void bothInstReverse() throws Exception {
        String response = testWith(null, null, null, null,
            "now(0,0)", "now(-100,0)", false);
        validate(response);
    }

    @Test(groups = {"singleCluster"}, dataProvider = "EL-DP")
    public void expressionLanguageTest(String startInstance, String endInstance) throws Exception {
        testWith(null, null, null, null, startInstance, endInstance, true);
    }

    @DataProvider(name = "EL-DP")
    public Object[][] getELData() {
        return new Object[][]{
            {"now(-3,0)", "now(4,20)"},
            {"yesterday(22,0)", "now(4,20)"},
            {"currentMonth(0,22,0)", "now(4,20)"},
            {"lastMonth(30,22,0)", "now(4,20)"},
            {"currentYear(0,0,22,0)", "currentYear(1,1,22,0)"},
            {"currentMonth(0,22,0)", "currentMonth(1,22,20)"},
            {"lastMonth(30,22,0)", "lastMonth(60,2,40)"},
            {"lastYear(12,0,22,0)", "lastYear(13,1,22,0)"},
        };
    }

    private void validate(String response) {
        if ((response.contains("End instance ") || response.contains("Start instance"))
            && (response.contains("for feed") || response.contains("of feed"))
            && (response.contains("is before the start of feed")
            || response.contains("is after the end of feed"))) {
            return;
        }
        if (response.contains("End instance")
            && response.contains("is before the start instance")) {
            return;
        }
        Assert.fail("Response is not valid");
    }

    private String testWith(String feedStart,
                            String feedEnd, String processStart,
                            String processEnd,
                            String startInstance, String endInstance, boolean isMatch)
        throws IOException, JAXBException, ParseException, URISyntaxException {
        HadoopUtil.uploadDir(cluster.getClusterHelper().getHadoopFS(),
            aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        Bundle bundle = BundleUtil.readELBundle();
        bundle = new Bundle(bundle, cluster.getPrefix());
        bundle.generateUniqueBundle(this);
        bundle.setProcessWorkflow(aggregateWorkflowDir);
        if (feedStart != null && feedEnd != null) {
            bundle.setFeedValidity(feedStart, feedEnd, bundle.getInputFeedNameFromBundle());
        }
        if (processStart != null && processEnd != null) {
            bundle.setProcessValidity(processStart, processEnd);
        }
        try {
            bundle.setInvalidData();
            bundle.setDatasetInstances(startInstance, endInstance);
            String submitResponse = bundle.submitFeedsScheduleProcess(prism).getMessage();
            LOGGER.info("processData in try is: " + Util.prettyPrintXml(bundle.getProcessData()));
            TimeUtil.sleepSeconds(45);
            if (isMatch) {
                getAndMatchDependencies(serverOC.get(0), bundle);
            }
            return submitResponse;
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e);
        } finally {
            LOGGER.info("deleting entity:");
            bundle.deleteBundle(prism);
        }
    }

    private void getAndMatchDependencies(OozieClient oozieClient, Bundle bundle) {
        try {
            List<String> bundles = null;
            for (int i = 0; i < 10; ++i) {
                bundles = OozieUtil.getBundles(oozieClient, bundle.getProcessName(), EntityType.PROCESS);
                if (bundles.size() > 0) {
                    break;
                }
                TimeUtil.sleepSeconds(30);
            }
            Assert.assertTrue(bundles != null && bundles.size() > 0, "Bundle job not created.");
            String coordID = bundles.get(0);
            LOGGER.info("coord id: " + coordID);
            List<String> missingDependencies = OozieUtil.getMissingDependencies(oozieClient, coordID);
            for (int i = 0; i < 10 && missingDependencies == null; ++i) {
                TimeUtil.sleepSeconds(30);
                missingDependencies = OozieUtil.getMissingDependencies(oozieClient, coordID);
            }
            Assert.assertNotNull(missingDependencies, "Missing dependencies not found.");
            for (String dependency : missingDependencies) {
                LOGGER.info("dependency from job: " + dependency);
            }
            Date jobNominalTime = OozieUtil.getNominalTime(oozieClient, coordID);
            Calendar time = Calendar.getInstance();
            time.setTime(jobNominalTime);
            LOGGER.info("nominalTime:" + jobNominalTime);
            SimpleDateFormat df = new SimpleDateFormat("dd MMM yyyy HH:mm:ss");
            LOGGER.info(
                "nominalTime in GMT string: " + df.format(jobNominalTime.getTime()) + " GMT");
            TimeZone z = time.getTimeZone();
            int offset = z.getRawOffset();
            int offsetHrs = offset / 1000 / 60 / 60;
            int offsetMins = offset / 1000 / 60 % 60;

            LOGGER.info("offset: " + offsetHrs);
            LOGGER.info("offset: " + offsetMins);

            time.add(Calendar.HOUR_OF_DAY, (-offsetHrs));
            time.add(Calendar.MINUTE, (-offsetMins));

            LOGGER.info("GMT Time: " + time.getTime());

            int frequency = bundle.getInitialDatasetFrequency();
            List<String> qaDependencyList =
                getQADepedencyList(time, bundle.getStartInstanceProcess(time),
                    bundle.getEndInstanceProcess(time),
                    frequency, bundle);
            for (String qaDependency : qaDependencyList) {
                LOGGER.info("qa qaDependencyList: " + qaDependency);
            }

            Assert.assertTrue(matchDependencies(missingDependencies, qaDependencyList));
        } catch (Exception e) {
            e.printStackTrace();
            throw new TestNGException(e);
        }
    }

    private boolean matchDependencies(List<String> fromJob, List<String> qaList) {
        if (fromJob.size() != qaList.size()) {
            return false;
        }
        Collections.sort(fromJob);
        Collections.sort(qaList);
        for (int index = 0; index < fromJob.size(); index++) {
            if (!fromJob.get(index).contains(qaList.get(index))) {
                return false;
            }
        }
        return true;
    }

    private List<String> getQADepedencyList(Calendar nominalTime, Date startRef,
                                            Date endRef, int frequency, Bundle bundle) {
        LOGGER.info("start ref:" + startRef);
        LOGGER.info("end ref:" + endRef);
        Calendar initialTime = Calendar.getInstance();
        initialTime.setTime(startRef);
        Calendar finalTime = Calendar.getInstance();

        finalTime.setTime(endRef);
        String path = bundle.getDatasetPath();

        TimeZone tz = TimeZone.getTimeZone("GMT");
        nominalTime.setTimeZone(tz);
        LOGGER.info("nominalTime: " + initialTime.getTime());
        LOGGER.info("finalTime: " + finalTime.getTime());
        List<String> returnList = new ArrayList<>();
        while (initialTime.getTime().before(finalTime.getTime())) {
            LOGGER.info("initialTime: " + initialTime.getTime());
            returnList.add(getPath(path, initialTime));
            initialTime.add(Calendar.MINUTE, frequency);
        }
        returnList.add(getPath(path, initialTime));
        Collections.reverse(returnList);
        return returnList;
    }

    private String getPath(String path, Calendar time) {
        if (path.contains("${YEAR}")) {
            path = path.replaceAll("\\$\\{YEAR\\}", Integer.toString(time.get(Calendar.YEAR)));
        }
        if (path.contains("${MONTH}")) {
            path = path.replaceAll("\\$\\{MONTH\\}", intToString(time.get(Calendar.MONTH) + 1, 2));
        }
        if (path.contains("${DAY}")) {
            path = path.replaceAll("\\$\\{DAY\\}", intToString(time.get(Calendar.DAY_OF_MONTH), 2));
        }
        if (path.contains("${HOUR}")) {
            path = path.replaceAll("\\$\\{HOUR\\}", intToString(time.get(Calendar.HOUR_OF_DAY), 2));
        }
        if (path.contains("${MINUTE}")) {
            path = path.replaceAll("\\$\\{MINUTE\\}", intToString(time.get(Calendar.MINUTE), 2));
        }
        return path;
    }

    private String intToString(int num, int digits) {
        assert digits > 0 : "Invalid number of digits";

        // create variable length array of zeros
        char[] zeros = new char[digits];
        Arrays.fill(zeros, '0');

        // format number as String
        DecimalFormat df = new DecimalFormat(String.valueOf(zeros));
        return df.format(num);
    }
}
