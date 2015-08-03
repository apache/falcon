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

package org.apache.falcon.regression.lineage;

import org.apache.falcon.Pair;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.Generator;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.EntityLineageUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.LineageGraphResult;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Test Suite for Entity lineage.
 */
@Test(groups = "embedded")
public class EntityLineageTest extends BaseTestClass {

    private String baseTestDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private ColoHelper cluster = servers.get(0);
    private static final Logger LOGGER = Logger.getLogger(EntityLineageTest.class);

    private int numInputFeeds;
    private String startTime = "2015-06-06T09:37Z";
    private String endTime = "2015-06-06T09:45Z";

    @BeforeMethod(alwaysRun = true)
    public void setup() throws Exception {
        LOGGER.info("Time range between : " + startTime + " and " + endTime);
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle(this);
        bundles[0].submitClusters(prism);

        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessConcurrency(6);
        bundles[0].setProcessPeriodicity(10, TimeUnit.minutes);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        removeTestClassEntities();
        cleanTestsDirs();
    }

    @Test(groups = {"singleCluster"})
    public void lineageTestFail() throws Exception {
        String failedPipelineName = "pipeline2";
        ServiceResponse response = prism.getProcessHelper().getEntityLineage("pipeline=" + failedPipelineName);
        AssertUtil.assertFailed(response, "No processes belonging to pipeline " + failedPipelineName);
    }

    @Test(groups = {"singleCluster"})
    public void lineageTestPass() throws Exception {

        String pipelineName = "pipeline1";

        FeedMerlin inputMerlin = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        inputMerlin.setValidity("2014-01-01T01:00Z", "2016-12-12T22:00Z");
        inputMerlin.setFrequency(new Frequency("5", TimeUnit.minutes));
        numInputFeeds = 10;

        FeedMerlin[] feedMerlins = generateFeeds(numInputFeeds, inputMerlin,
                                   Generator.getNameGenerator("Feed", ""),
                                   Generator.getHadoopPathGenerator(baseTestDir, MINUTE_DATE_PATTERN));

        Pair<String[], String[]>[] input = new Pair[]{
            new Pair((new String[]{"Feed001"}), new String[]{"Feed002", "Feed005"}),
            new Pair(new String[]{"Feed002"}, new String[]{"Feed003"}),
            new Pair(new String[]{"Feed002"}, new String[]{"Feed004"}),
            new Pair(new String[]{"Feed003", "Feed004", "Feed005"}, new String[]{"Feed001", "Feed006"}),
            new Pair(new String[]{"Feed006"}, new String[]{"Feed007"}),
            new Pair(new String[]{"Feed008", "Feed010"}, new String[]{"Feed009"}),
            new Pair(new String[]{"Feed009"}, new String[]{"Feed010"}),
        };

        List<Pair<String[], String[]>> processFeed = Arrays.asList(input);
        List<ProcessMerlin> processMerlins = new ArrayList<>();
        Generator nameGenerator=Generator.getNameGenerator("Process", "");

        for(Integer i=0; i<processFeed.size(); i++){
            ProcessMerlin process = createProcess(nameGenerator.generate().replace("-", ""), pipelineName,
                            processFeed.get(i).first, processFeed.get(i).second);
            processMerlins.add(i, process);
        }

        deleteProcess(processMerlins);
        deleteFeed(Arrays.asList(feedMerlins));

        for (FeedMerlin feed : feedMerlins) {
            AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(feed.toString()));
        }

        for(ProcessMerlin process : processMerlins) {
            AssertUtil.assertSucceeded(prism.getProcessHelper().submitEntity(process.toString()));
        }

        LineageGraphResult lineageGraphResult = prism.getProcessHelper().getEntityLineage("pipeline=" + pipelineName)
                .getLineageGraphResult();

        LOGGER.info("LineageGraphResult : " + lineageGraphResult.toString());

        validateLineage(lineageGraphResult);

        deleteProcess(processMerlins);
        deleteFeed(Arrays.asList(feedMerlins));
    }

    public ProcessMerlin createProcess(String processName, String pipelineName, String[] inputFeed,
                                       String[] outputFeed) throws URISyntaxException, AuthenticationException,
                                       InterruptedException, IOException, JAXBException {
        ProcessMerlin processMerlin = new ProcessMerlin(bundles[0].getProcessObject().toString());
        processMerlin.setName(processName);
        processMerlin.setPipelineTag(pipelineName);
        setProcessData(processMerlin, inputFeed, outputFeed);
        return processMerlin;
    }

    private void setProcessData(ProcessMerlin processMerlin, String[] ipName, String[] opName) {
        processMerlin.resetInputFeed(ipName[0], ipName[0]);
        processMerlin.resetOutputFeed(opName[0], opName[0]);

        if (ipName.length > 1) {
            String[] ipFeed = new String[ipName.length -1];
            System.arraycopy(ipName, 1, ipFeed, 0, ipName.length - 1);
            processMerlin.addInputFeeds(ipFeed);
        }
        if (opName.length > 1) {
            String[] opFeed = new String[opName.length - 1];
            System.arraycopy(opName, 1, opFeed, 0, opName.length - 1);
            processMerlin.addOutputFeeds(opFeed);
        }
    }

    public static FeedMerlin[] generateFeeds(final int numInputFeeds,
                                             final FeedMerlin originalFeedMerlin,
                                             final Generator nameGenerator,
                                             final Generator pathGenerator) {
        FeedMerlin[] inputFeeds = new FeedMerlin[numInputFeeds];
        //submit all input feeds
        for(int count = 0; count < numInputFeeds; ++count) {
            final FeedMerlin feed = new FeedMerlin(originalFeedMerlin.toString());
            feed.setName(nameGenerator.generate().replace("-", ""));
            feed.setLocation(LocationType.DATA, pathGenerator.generate());
            inputFeeds[count] = feed;
        }
        return inputFeeds;
    }

    private void deleteProcess(List<ProcessMerlin> processMerlins) throws InterruptedException, IOException,
            URISyntaxException, JAXBException, AuthenticationException {
        for(ProcessMerlin process : processMerlins) {
            AssertUtil.assertSucceeded(prism.getProcessHelper().delete(process.toString()));
        }
    }

    private void deleteFeed(List<FeedMerlin> feedMerlins) throws InterruptedException, IOException,
            URISyntaxException, JAXBException, AuthenticationException {
        for(FeedMerlin feed : feedMerlins) {
            AssertUtil.assertSucceeded(prism.getFeedHelper().delete(feed.toString()));
        }
    }

    private void validateLineage(LineageGraphResult lineageGraphResult) {
        String[] expectedVertices = {"Process001", "Process002", "Process003", "Process004", "Process005",
                                     "Process006", "Process007", };
        LineageGraphResult.Edge[] expectedEdgeArray = new LineageGraphResult.Edge[lineageGraphResult.getEdges().length];
        expectedEdgeArray[0] = new LineageGraphResult.Edge("Process004", "Process005", "Feed006");
        expectedEdgeArray[1] = new LineageGraphResult.Edge("Process006", "Process007", "Feed009");
        expectedEdgeArray[2] = new LineageGraphResult.Edge("Process004", "Process001", "Feed001");
        expectedEdgeArray[3] = new LineageGraphResult.Edge("Process002", "Process004", "Feed003");
        expectedEdgeArray[4] = new LineageGraphResult.Edge("Process007", "Process006", "Feed010");
        expectedEdgeArray[5] = new LineageGraphResult.Edge("Process001", "Process002", "Feed002");
        expectedEdgeArray[6] = new LineageGraphResult.Edge("Process001", "Process003", "Feed002");
        expectedEdgeArray[7] = new LineageGraphResult.Edge("Process001", "Process004", "Feed005");
        expectedEdgeArray[8] = new LineageGraphResult.Edge("Process003", "Process004", "Feed004");

        EntityLineageUtil.validateLineageGraphResult(lineageGraphResult, expectedVertices, expectedEdgeArray);
    }
}

