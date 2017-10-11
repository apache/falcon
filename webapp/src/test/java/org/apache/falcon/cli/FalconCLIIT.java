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

package org.apache.falcon.cli;

import org.apache.falcon.client.FalconCLIConstants;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.metadata.RelationshipType;
import org.apache.falcon.resource.TestContext;
import org.apache.falcon.util.FalconTestUtil;
import org.apache.falcon.util.OozieTestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Date;
import java.util.Map;

/**
 * Test for Falcon CLI.
 *
 * todo: Refactor both the classes to move this methods to helper;
 */
@Test(groups = {"exhaustive"})
public class FalconCLIIT {
    private InMemoryWriter stream = new InMemoryWriter(System.out);

    @BeforeClass
    public void prepare() throws Exception {
        TestContext.prepare();
    }

    @AfterClass
    public void tearDown() throws Exception {
        TestContext.deleteEntitiesFromStore();
    }

    public void testSubmitEntityValidCommands() throws Exception {

        FalconCLI.OUT.set(stream);

        String filePath;
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();

        filePath = TestContext.overlayParametersOverTemplate(TestContext.CLUSTER_TEMPLATE, overlay);
        Assert.assertEquals(executeWithURL("entity -submit -type cluster -file " + filePath), 0);
        context.setCluster(overlay.get("cluster"));
        Assert.assertEquals(stream.buffer.toString().trim(),
                "falcon/default/Submit successful (cluster) " + context.getClusterName());

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE1, overlay);
        Assert.assertEquals(executeWithURL("entity -submit -type feed -file " + filePath), 0);
        Assert.assertEquals(
                stream.buffer.toString().trim(),
                "falcon/default/Submit successful (feed) "
                        + overlay.get("inputFeedName"));

        // Test the lookup command
        Assert.assertEquals(executeWithURL("entity -lookup -type feed -path "
                + "/falcon/test/input/2014/11/23/23"), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE2, overlay);
        Assert.assertEquals(executeWithURL("entity -submit -type feed -doAs " + FalconTestUtil.TEST_USER_2
                + " -file " + filePath), 0);
        Assert.assertEquals(
                stream.buffer.toString().trim(),
                "falcon/default/Submit successful (feed) "
                        + overlay.get("outputFeedName"));

        filePath = TestContext.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
        Assert.assertEquals(executeWithURL("entity -submit -type process -doAs " + FalconTestUtil.TEST_USER_2
                + " -file " + filePath), 0);
        Assert.assertEquals(
                stream.buffer.toString().trim(),
                "falcon/default/Submit successful (process) "
                        + overlay.get("processName"));
    }

    public void testListWithEmptyConfigStore() throws Exception {
        Assert.assertEquals(executeWithURL("entity -list -type process "), 0);
    }

    public void testSubmitAndScheduleEntityValidCommands() throws Exception {

        String filePath;
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();

        filePath = TestContext.overlayParametersOverTemplate(context.getClusterFileTemplate(), overlay);
        Assert.assertEquals(executeWithURL("entity -submitAndSchedule -type cluster -file " + filePath), -1);
        context.setCluster(overlay.get("cluster"));

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE1, overlay);
        Assert.assertEquals(executeWithURL("entity -submitAndSchedule -type feed -doAs " + FalconTestUtil.TEST_USER_2
                + " -file " + filePath), 0);
        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE2, overlay);
        Assert.assertEquals(executeWithURL("entity -submitAndSchedule -type feed -file " + filePath), 0);
        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE1, overlay);
        Assert.assertEquals(executeWithURL("entity -submit -type feed -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE2, overlay);
        Assert.assertEquals(executeWithURL("entity -submit -type feed -doAs " + FalconTestUtil.TEST_USER_2
                + " -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
        Assert.assertEquals(executeWithURL("entity -submitAndSchedule -type process -file " + filePath), 0);
        OozieTestUtils.waitForProcessWFtoStart(context);

        Assert.assertEquals(executeWithURL("entity -update -name " + overlay.get("processName")
                + " -type process -file " + filePath), 0);

        Assert.assertEquals(0,
                executeWithURL("entity -touch -name " + overlay.get("processName") + " -type process"));
    }

    public void testValidateValidCommands() throws Exception {

        String filePath;
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();

        filePath = TestContext.overlayParametersOverTemplate(context.getClusterFileTemplate(), overlay);
        Assert.assertEquals(executeWithURL("entity -validate -type cluster -file " + filePath), 0);
        context.setCluster(overlay.get("cluster"));
        Assert.assertEquals(executeWithURL("entity -submit -type cluster -file " + filePath), 0);
        context.setCluster(overlay.get("cluster"));

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE1, overlay);
        Assert.assertEquals(executeWithURL("entity -validate -type feed -file " + filePath), 0);
        Assert.assertEquals(executeWithURL("entity -submit -type feed -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE2, overlay);
        Assert.assertEquals(executeWithURL("entity -validate -type feed -file " + filePath), 0);
        Assert.assertEquals(executeWithURL("entity -submit -type feed -doAs " + FalconTestUtil.TEST_USER_2
                + " -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
        Assert.assertEquals(executeWithURL("entity -validate -type process -file " + filePath), 0);

        Assert.assertEquals(executeWithURL("entity -submit -type process -file " + filePath), 0);
    }

    public void testDefinitionEntityValidCommands() throws Exception {
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        submitTestFiles(context, overlay);

        Assert.assertEquals(executeWithURL("entity -definition -type cluster -name " + overlay.get("cluster")), 0);

        Assert.assertEquals(executeWithURL("entity -definition -type feed -name " + overlay.get("inputFeedName")), 0);

        Assert.assertEquals(executeWithURL("entity -definition -type feed  -doAs " + FalconTestUtil.TEST_USER_2
                + " -name " + overlay.get("outputFeedName")), 0);

        Assert.assertEquals(executeWithURL("entity -definition -type process -name " + overlay.get("processName")), 0);

    }

    public void testScheduleEntityValidCommands() throws Exception {

        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        submitTestFiles(context, overlay);

        Assert.assertEquals(executeWithURL("entity -schedule -type cluster -name " + overlay.get("cluster")), -1);

        Assert.assertEquals(executeWithURL("entity -schedule -type feed -doAs " + FalconTestUtil.TEST_USER_2
                + " -name " + overlay.get("inputFeedName")), 0);

        Assert.assertEquals(executeWithURL("entity -schedule -type feed -name " + overlay.get("outputFeedName")), 0);

        Assert.assertEquals(executeWithURL("entity -schedule -type process -name " + overlay.get("processName")
                                            + " -properties key:value"), 0);

    }

    public void testSkipDryRunValidCommands() throws Exception {
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        submitTestFiles(context, overlay);

        Assert.assertEquals(
                executeWithURL("entity -schedule -skipDryRun -type cluster -name " + overlay.get("cluster")), -1);

        Assert.assertEquals(
                executeWithURL("entity -schedule -type feed -name " + overlay.get("outputFeedName")), 0);

        Assert.assertEquals(
                executeWithURL("entity -schedule -type process -skipDryRun -name " + overlay.get("processName")), 0);

        Assert.assertEquals(0,
                executeWithURL("entity -touch -skipDryRun -name " + overlay.get("processName") + " -type process"));

        String filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE1, overlay);
        Assert.assertEquals(
                executeWithURL("entity -submitAndSchedule -skipDryRun -type feed -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
        Assert.assertEquals(
                executeWithURL("entity -validate -skipDryRun -type process -file " + filePath), 0);

    }

    public void testSuspendResumeStatusEntityValidCommands() throws Exception {

        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        submitTestFiles(context, overlay);

        Assert.assertEquals(executeWithURL("entity -status -type feed -doAs " + FalconTestUtil.TEST_USER_2
                + " -name " + overlay.get("inputFeedName")), 0);

        Assert.assertEquals(executeWithURL("entity -status -type feed -name " + overlay.get("outputFeedName")), 0);

        Assert.assertEquals(executeWithURL("entity -status -type process -name " + overlay.get("processName")), 0);

        Assert.assertEquals(executeWithURL("entity -schedule -type feed -doAs " + FalconTestUtil.TEST_USER_2
                + " -name " + overlay.get("inputFeedName")), 0);

        Assert.assertEquals(executeWithURL("entity -schedule -type feed -name " + overlay.get("outputFeedName")), 0);

        Assert.assertEquals(executeWithURL("entity -schedule -type process -name " + overlay.get("processName")), 0);

        OozieTestUtils.waitForProcessWFtoStart(context);

        Assert.assertEquals(executeWithURL("entity -suspend -type feed -doAs " + FalconTestUtil.TEST_USER_2
                + " -name " + overlay.get("inputFeedName")), 0);

        Assert.assertEquals(executeWithURL("entity -suspend -type feed -name " + overlay.get("outputFeedName")), 0);

        Assert.assertEquals(executeWithURL("entity -suspend -type process -name " + overlay.get("processName")), 0);

        Assert.assertEquals(executeWithURL("entity -status -type feed -doAs " + FalconTestUtil.TEST_USER_2
                + " -name " + overlay.get("inputFeedName")), 0);

        Assert.assertEquals(executeWithURL("entity -status -type feed -name " + overlay.get("outputFeedName")), 0);

        Assert.assertEquals(executeWithURL("entity -status -type process -name " + overlay.get("processName")), 0);

        Assert.assertEquals(executeWithURL("entity -resume -type feed -doAs " + FalconTestUtil.TEST_USER_2
                + " -name " + overlay.get("inputFeedName")), 0);

        Assert.assertEquals(executeWithURL("entity -resume -type feed -name " + overlay.get("outputFeedName")), 0);

        Assert.assertEquals(executeWithURL("entity -resume -type process -name " + overlay.get("processName")), 0);

        Assert.assertEquals(executeWithURL("entity -status -type feed -doAs " + FalconTestUtil.TEST_USER_2
                + " -name " + overlay.get("inputFeedName")), 0);

        Assert.assertEquals(executeWithURL("entity -status -type feed -name " + overlay.get("outputFeedName")), 0);

        Assert.assertEquals(executeWithURL("entity -status -type process -name " + overlay.get("processName")), 0);

        Assert.assertEquals(executeWithURL("entity -summary -type feed -cluster "+ overlay.get("cluster")
                + " -fields status,tags -start " + START_INSTANCE
                + " -filterBy TYPE:FEED -orderBy name -sortOrder asc "
                + " -offset 0 -numResults 1 -numInstances 5"), 0);

        Assert.assertEquals(executeWithURL("entity -summary -type process -fields status,pipelines"
                + " -cluster " + overlay.get("cluster")
                + " -start " + SchemaHelper.getDateFormat().format(new Date(0))
                + " -end " + SchemaHelper.getDateFormat().format(new Date())
                + " -filterBy TYPE:PROCESS -orderBy name -sortOrder desc "
                + " -offset 0 -numResults 1 -numInstances 7"), 0);

        Assert.assertEquals(executeWithURL("entity -summary -type process -fields status,pipelines"
                + " -cluster " + overlay.get("cluster")
                + " -start " + SchemaHelper.getDateFormat().format(new Date(0))
                + " -end " + SchemaHelper.getDateFormat().format(new Date())
                + " -filterBy TYPE:PROCESS -orderBy name -sortOrder invalid "
                + " -offset 0 -numResults 1 -numInstances 7"), -1);

        // No start or end date and with doAs option
        Assert.assertEquals(executeWithURL("entity -summary -type process -doAs " + FalconTestUtil.TEST_USER_2
                + " -fields status,pipelines"
                + " -cluster " + overlay.get("cluster")
                + " -filterBy TYPE:PROCESS -orderBy name "
                + " -offset 0 -numResults 1 -numInstances 7"), 0);

    }

    public void testSubCommandPresence() throws Exception {
        Assert.assertEquals(-1, executeWithURL("entity -type cluster "));
    }

    public void testDeleteEntityValidCommands() throws Exception {

        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        submitTestFiles(context, overlay);

        Assert.assertEquals(executeWithURL("entity -delete -type cluster -name " + overlay.get("cluster")), -1);

        Assert.assertEquals(executeWithURL("entity -delete -type feed -name " + overlay.get("inputFeedName")), -1);

        Assert.assertEquals(executeWithURL("entity -delete -type feed -doAs " + FalconTestUtil.TEST_USER_2 + " -name "
                + overlay.get("outputFeedName")), -1);

        Assert.assertEquals(executeWithURL("entity -delete -type process -doAs " + FalconTestUtil.TEST_USER_2
                + " -name " + overlay.get("processName")), 0);

        Assert.assertEquals(executeWithURL("entity -delete -type feed -name " + overlay.get("inputFeedName")), 0);

        Assert.assertEquals(executeWithURL("entity -delete -type feed -name " + overlay.get("outputFeedName")), 0);
    }

    public void testInvalidCLIEntitycommands() throws Exception {

        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE1, overlay);
        Assert.assertEquals(executeWithURL("entity -submit -type feed -name " + "name"), -1);

        Assert.assertEquals(executeWithURL("entity -schedule -type feed -file " + "name"), -1);
    }

    public void testInstanceRunningAndStatusCommands() throws Exception {
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        submitTestFiles(context, overlay);

        Assert.assertEquals(executeWithURL("entity -schedule -type process -name " + overlay.get("processName")), 0);

        Assert.assertEquals(executeWithURL("entity -schedule -type feed -name " + overlay.get("outputFeedName")), 0);
        OozieTestUtils.waitForProcessWFtoStart(context);

        //Test the dependency command
        Assert.assertEquals(executeWithURL("instance -dependency -type feed -name " + overlay.get("inputFeedName")
                + " -instanceTime 2010-01-01T00:00Z"), 0);

        //Test the dependency command with doAs
        Assert.assertEquals(executeWithURL("instance -dependency -type feed -doAs " + FalconTestUtil.TEST_USER_2
                + " -name " + overlay.get("inputFeedName") + " -instanceTime 2010-01-01T00:00Z"), 0);

        Assert.assertEquals(executeWithURL("instance -status -type feed -name "
                + overlay.get("outputFeedName")
                + " -start " + START_INSTANCE), 0);

        Assert.assertEquals(executeWithURL("instance -running -type process -name " + overlay.get("processName")), 0);
        // with doAs
        Assert.assertEquals(executeWithURL("instance -running -type process -doAs " + FalconTestUtil.TEST_USER_2
                + " -name " + overlay.get("processName")), 0);

        Assert.assertEquals(executeWithURL("instance -running -type feed -lifecycle eviction -name "
                + overlay.get("outputFeedName") + " -start " + SchemaHelper.getDateFormat().format(new Date())), 0);

        Assert.assertEquals(executeWithURL("instance -listing -type feed -name "
                + overlay.get("outputFeedName") + " -start " + SchemaHelper.getDateFormat().format(new Date())), 0);

        Assert.assertEquals(executeWithURL("instance -status -type process -name "
                + overlay.get("processName") + " -start " + START_INSTANCE), 0);

        //TEst instance status with doAs
        Assert.assertEquals(executeWithURL("instance -status -type process -doAs " + FalconTestUtil.TEST_USER_2
                + " -name " + overlay.get("processName") + " -start " + START_INSTANCE), 0);


        Assert.assertEquals(executeWithURL("instance -status -type feed -lifecycle eviction,replication -name "
                + overlay.get("outputFeedName")
                + " -start " + SchemaHelper.getDateFormat().format(new Date())), 0);

        Assert.assertEquals(executeWithURL("instance -status -type feed -lifecycle eviction -name "
                + overlay.get("outputFeedName") + " -start " + SchemaHelper.getDateFormat().format(new Date())), 0);

        Assert.assertEquals(executeWithURL("instance -params -type process -name "
                + overlay.get("processName") + " -start " + START_INSTANCE), 0);

        // doAs option
        Assert.assertEquals(executeWithURL("instance -params -type process -doAs " + FalconTestUtil.TEST_USER_2
                + " -name " + overlay.get("processName") + " -start " + START_INSTANCE), 0);

        // test filterBy, orderBy, offset, numResults
        String startTimeString = SchemaHelper.getDateFormat().format(new Date());
        Assert.assertEquals(executeWithURL("instance -running -type feed -lifecycle eviction -name "
                + overlay.get("outputFeedName") + " -start " + startTimeString
                + " -orderBy startTime -sortOrder asc -offset 0 -numResults 1"), 0);

        Assert.assertEquals(executeWithURL("instance -running -type feed -lifecycle eviction -name "
                + overlay.get("outputFeedName")
                + " -start " + SchemaHelper.getDateFormat().format(new Date())
                + " -orderBy INVALID -offset 0 -numResults 1"), -1);

        Assert.assertEquals(executeWithURL("instance -running -type feed -lifecycle eviction -name "
                + overlay.get("outputFeedName") + " -start " + startTimeString
                + " -orderBy startTime -sortOrder desc -offset 0 -numResults 1"), 0);

        Assert.assertEquals(executeWithURL("instance -running -type feed -lifecycle eviction -name "
                + overlay.get("outputFeedName") + " -start " + startTimeString
                + " -orderBy startTime -sortOrder invalid -offset 0 -numResults 1"), -1);

        Assert.assertEquals(executeWithURL("instance -running -type feed -lifecycle eviction -name "
                + overlay.get("outputFeedName") + " -start " + SchemaHelper.getDateFormat().format(new Date())
                + " -filterBy INVALID:FILTER -offset 0 -numResults 1"), -1);

        // testcase : start str is older than entity schedule time.
        Assert.assertEquals(executeWithURL("instance -running -type feed -lifecycle eviction -name "
                + overlay.get("outputFeedName")
                + " -start " + SchemaHelper.getDateFormat().format(new Date(10000))
                + " -orderBy startTime -sortOrder asc -offset 0 -numResults 1"), 0);
        // testcase : end str is in future
        long futureTimeinMilliSecs = (new Date()).getTime()+ 86400000;
        Assert.assertEquals(executeWithURL("instance -running -type feed -lifecycle eviction -name "
                + overlay.get("outputFeedName")
                + " -start " + SchemaHelper.getDateFormat().format(new Date(10000))
                + " -end " + SchemaHelper.getDateFormat().format(new Date(futureTimeinMilliSecs))
                + " -orderBy startTime -offset 0 -numResults 1"), 0);
        // Both start and end dates are optional
        Assert.assertEquals(executeWithURL("instance -running -type feed -lifecycle eviction -name "
                + overlay.get("outputFeedName")
                + " -orderBy startTime -offset 0 -numResults 1"), 0);

        Assert.assertEquals(executeWithURL("instance -status -type process -name "
                + overlay.get("processName")
                + " -start " + START_INSTANCE
                + " -filterBy STATUS:SUCCEEDED,STARTEDAFTER:" + START_INSTANCE
                + " -orderBy startTime -sortOrder desc -offset 0 -numResults 1"), 0);

        Assert.assertEquals(executeWithURL("instance -status -type process -name "
                + overlay.get("processName")
                + " -start " + START_INSTANCE
                + " -filterBy SOURCECLUSTER:" + overlay.get("cluster")
                + " -orderBy startTime -sortOrder desc -offset 0 -numResults 1"), 0);

        //Test list with doAs
        Assert.assertEquals(executeWithURL("instance -list -type feed -doAs " + FalconTestUtil.TEST_USER_2
                + " -name " + overlay.get("outputFeedName") + " -start "
                + SchemaHelper.getDateFormat().format(new Date())), 0);

        Assert.assertEquals(executeWithURL("instance -list -type feed -lifecycle eviction -name "
                + overlay.get("outputFeedName")
                + " -start " + SchemaHelper.getDateFormat().format(new Date())
                + " -filterBy STATUS:SUCCEEDED -orderBy startTime -offset 0 -numResults 1"), 0);

        Assert.assertEquals(executeWithURL("instance -list -type feed -lifecycle eviction -name "
                + overlay.get("outputFeedName")
                + " -start " + SchemaHelper.getDateFormat().format(new Date())
                + " -filterBy SOURCECLUSTER:" + overlay.get("src.cluster.name")
                + " -orderBy startTime -offset 0 -numResults 1"), 0);

        Assert.assertEquals(executeWithURL("instance -status -type feed -lifecycle eviction -name "
                + overlay.get("outputFeedName")
                + " -start "+ SchemaHelper.getDateFormat().format(new Date())
                +" -filterBy INVALID:FILTER -orderBy startTime -offset 0 -numResults 1"), -1);

        Assert.assertEquals(executeWithURL("instance -list -type feed -lifecycle eviction -name "
                + overlay.get("outputFeedName")
                + " -start "+ SchemaHelper.getDateFormat().format(new Date())
                +" -filterBy STATUS:SUCCEEDED -orderBy INVALID -offset 0 -numResults 1"), -1);
        Assert.assertEquals(executeWithURL("instance -status -type feed -lifecycle eviction -name "
                + overlay.get("outputFeedName")
                + " -start " + SchemaHelper.getDateFormat().format(new Date())
                + " -filterBy STATUS:SUCCEEDED -orderBy startTime -offset 1 -numResults 1"), 0);

        // When you get a cluster for which there are no feed entities,
        Assert.assertEquals(executeWithURL("entity -summary -type feed -cluster " + overlay.get("cluster")
                + " -fields status,tags"
                + " -start " + SchemaHelper.getDateFormat().format(new Date())
                + " -offset 0 -numResults 1 -numInstances 3"), 0);

    }

    public void testInstanceRunningAndSearchSummaryCommands() throws Exception {
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        submitTestFiles(context, overlay);

        Assert.assertEquals(executeWithURL("entity -schedule -type process -name " + overlay.get("processName")), 0);

        Assert.assertEquals(executeWithURL("entity -schedule -type feed -name " + overlay.get("outputFeedName")), 0);

        OozieTestUtils.waitForProcessWFtoStart(context);

        Assert.assertEquals(executeWithURL("instance -status -type feed -name " + overlay.get("outputFeedName")
                + " -start " + START_INSTANCE), 0);

        Assert.assertEquals(executeWithURL("instance -search"), 0);

        Assert.assertEquals(executeWithURL("instance -running -type process -name " + overlay.get("processName")), 0);

        //with doAs
        Assert.assertEquals(executeWithURL(
                "instance -search -type process -instanceStatus RUNNING -doAs " + FalconTestUtil.TEST_USER_2), 0);

        Assert.assertEquals(executeWithURL("instance -running -type process -doAs " + FalconTestUtil.TEST_USER_2
                + " -name " + overlay.get("processName")), 0);

        Assert.assertEquals(executeWithURL("instance -summary -type process -name "
                + overlay.get("processName") + " -start " + START_INSTANCE), 0);

        //with doAs
        Assert.assertEquals(executeWithURL("instance -summary -type process -doAs " + FalconTestUtil.TEST_USER_2
                + " -name " + overlay.get("processName") + " -start " + START_INSTANCE), 0);

        Assert.assertEquals(executeWithURL("instance -summary -type feed -lifecycle eviction -name "
                + overlay.get("outputFeedName")
                + " -start " + SchemaHelper.getDateFormat().format(new Date())), 0);

        Assert.assertEquals(executeWithURL("instance -params -type process -name "
                + overlay.get("processName")
                + " -start " + START_INSTANCE), 0);

        //with doAs
        Assert.assertEquals(executeWithURL("instance -params -type process -doAs " + FalconTestUtil.TEST_USER_2
                + " -name " + overlay.get("processName") + " -start " + START_INSTANCE), 0);
    }

    public void testInstanceSuspendAndResume() throws Exception {
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        submitTestFiles(context, overlay);

        Assert.assertEquals(executeWithURL("entity -schedule -type process -name " + overlay.get("processName")), 0);

        Assert.assertEquals(executeWithURL("entity -schedule -type feed -name " + overlay.get("inputFeedName")), 0);

        Assert.assertEquals(executeWithURL("entity -schedule -type feed -name " + overlay.get("outputFeedName")), 0);

        Assert.assertEquals(executeWithURL("instance -suspend -type feed -name "
                + overlay.get("inputFeedName")
                + " -start " + START_INSTANCE + " -end " + START_INSTANCE), 0);

        Assert.assertEquals(executeWithURL("instance -suspend -type process -doAs " + FalconTestUtil.TEST_USER_2
                + " -name " + overlay.get("processName") + " -start " + START_INSTANCE + " -end " + START_INSTANCE), 0);

        // No end date, should fail.
        Assert.assertEquals(executeWithURL("instance -suspend -type feed -lifecycle eviction -name "
                + overlay.get("outputFeedName")
                + " -start "+ SchemaHelper.getDateFormat().format(new Date())), -1);

        Assert.assertEquals(executeWithURL("instance -resume -type process -name "
                + overlay.get("processName")
                + " -start " + START_INSTANCE + " -end " + START_INSTANCE), 0);

        Assert.assertEquals(executeWithURL("instance -resume -type feed -doAs " + FalconTestUtil.TEST_USER_2 + " -name "
                + overlay.get("inputFeedName")
                + " -start " + START_INSTANCE + " -end " + START_INSTANCE), 0);

        Assert.assertEquals(executeWithURL("instance -resume -type feed -lifecycle eviction -name "
                + overlay.get("outputFeedName")
                + " -start " + SchemaHelper.getDateFormat().format(new Date())
                + " -end " + SchemaHelper.getDateFormat().format(new Date())), 0);
    }

    private static final String START_INSTANCE = "2012-04-20T00:00Z";

    public void testInstanceKillAndRerun() throws Exception {
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        submitTestFiles(context, overlay);

        Assert.assertEquals(executeWithURL("entity -schedule -type process -name " + overlay.get("processName")), 0);

        Assert.assertEquals(executeWithURL("entity -schedule -type feed -doAs " + FalconTestUtil.TEST_USER_2 + " -name "
                + overlay.get("inputFeedName")), 0);

        Assert.assertEquals(executeWithURL("entity -schedule -type feed -name " + overlay.get("outputFeedName")), 0);

        OozieTestUtils.waitForProcessWFtoStart(context);
        Assert.assertEquals(executeWithURL("instance -kill -type process -name "
                + overlay.get("processName")
                + " -start " + START_INSTANCE + " -end " + START_INSTANCE), 0);

        Assert.assertEquals(executeWithURL("instance -kill -type feed -doAs " + FalconTestUtil.TEST_USER_2 + " -name "
                + overlay.get("inputFeedName")
                + " -start " + START_INSTANCE + " -end " + START_INSTANCE), 0);

        // Fail due to no end date
        Assert.assertEquals(executeWithURL("instance -kill -type feed -lifecycle eviction -name "
                + overlay.get("outputFeedName")
                + " -start "+ SchemaHelper.getDateFormat().format(new Date())), -1);

        Assert.assertEquals(executeWithURL("instance -rerun -type process -name "
                + overlay.get("processName")
                + " -start " + START_INSTANCE + " -end " + START_INSTANCE
                + " -file " + createTempJobPropertiesFile()), 0);

        Assert.assertEquals(executeWithURL("instance -rerun -type feed -doAs " + FalconTestUtil.TEST_USER_2 + " -name "
                + overlay.get("inputFeedName")
                + " -start " + START_INSTANCE + " -end " + START_INSTANCE
                + " -file " + createTempJobPropertiesFile()), 0);

        Assert.assertEquals(executeWithURL("instance -rerun -type feed -lifecycle eviction -name "
                + overlay.get("outputFeedName")
                + " -start " + SchemaHelper.getDateFormat().format(new Date())
                + " -end " + SchemaHelper.getDateFormat().format(new Date())
                + " -file " + createTempJobPropertiesFile()), 0);
    }


    @Test
    public void testEntityLineage() throws Exception {
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();

        String filePath;
        filePath = TestContext.overlayParametersOverTemplate(context.getClusterFileTemplate(), overlay);
        context.setCluster(overlay.get("cluster"));
        Assert.assertEquals(executeWithURL("entity -submit -type cluster -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE1, overlay);
        Assert.assertEquals(executeWithURL("entity -submit -type feed -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE2, overlay);
        Assert.assertEquals(executeWithURL("entity -submit -type feed -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
        Assert.assertEquals(executeWithURL("entity -submit -type process -file " + filePath), 0);

        Assert.assertEquals(executeWithURL("metadata -lineage -pipeline testPipeline"), 0);
        Assert.assertEquals(executeWithURL("metadata -lineage -doAs " + FalconTestUtil.TEST_USER_2
                + " -pipeline testPipeline"), 0);
    }

    @Test
    public void testEntityPaginationFilterByCommands() throws Exception {

        String filePath;
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();

        filePath = TestContext.overlayParametersOverTemplate(context.getClusterFileTemplate(), overlay);
        Assert.assertEquals(executeWithURL("entity -submitAndSchedule -type cluster -file " + filePath), -1);
        context.setCluster(overlay.get("cluster"));

        // this is necessary for lineage
        Assert.assertEquals(executeWithURL("entity -submit -type cluster -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE1, overlay);
        Assert.assertEquals(executeWithURL("entity -submitAndSchedule -type feed -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE2, overlay);
        Assert.assertEquals(executeWithURL("entity -submitAndSchedule -type feed -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE1, overlay);
        Assert.assertEquals(executeWithURL("entity -submit -type feed -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE2, overlay);
        Assert.assertEquals(executeWithURL("entity -submit -type feed -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
        Assert.assertEquals(executeWithURL("entity -validate -type process -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
        Assert.assertEquals(executeWithURL("entity -submitAndSchedule -type process -file " + filePath), 0);

        OozieTestUtils.waitForProcessWFtoStart(context);

        // test entity List cli
        Assert.assertEquals(executeWithURL("entity -list -offset 0 -numResults 1"), 0);
        Assert.assertEquals(executeWithURL("entity -list -type feed,process -offset 0 -numResults 1"), 0);
        Assert.assertEquals(executeWithURL("entity -list -type feed,process -offset 0 -numResults 1 "
                + "-nameseq abc -tagkeys abc"), 0);
        Assert.assertEquals(executeWithURL("entity -list -type cluster" + " -offset 0 -numResults 1"), 0);
        Assert.assertEquals(executeWithURL("entity -list -type process -fields status "
                + " -filterBy STATUS:SUBMITTED,TYPE:process -orderBy name "
                + " -sortOrder asc -offset 1 -numResults 1"), 0);
        Assert.assertEquals(executeWithURL("entity -list -type process -fields status,pipelines "
                + " -filterBy STATUS:SUBMITTED,type:process -orderBy name -offset 1 -numResults 1"), 0);
        Assert.assertEquals(executeWithURL("entity -list -type process -fields status,pipelines "
                + " -filterBy STATUS:SUBMITTED,pipelines:testPipeline "
                + " -orderBy name -sortOrder desc -offset 1 -numResults 1"), 0);
        Assert.assertEquals(executeWithURL("entity -list -type process -fields status,tags "
                + " -tags owner=producer@xyz.com,department=forecasting "
                + " -filterBy STATUS:SUBMITTED,type:process -orderBy name -offset 1 -numResults 1"), 0);
        Assert.assertEquals(executeWithURL("entity -list -type process -fields status "
                + " -filterBy STATUS:SUCCEEDED,TYPE:process -orderBy INVALID -offset 0 -numResults 1"), -1);

        Assert.assertEquals(executeWithURL("entity -list -type process -fields INVALID "
                + " -filterBy STATUS:SUCCEEDED,TYPE:process -orderBy name -offset 1 -numResults 1"), -1);

        Assert.assertEquals(executeWithURL("entity -list -type process -fields status "
                + " -filterBy INVALID:FILTER,TYPE:process -orderBy name -offset 1 -numResults 1"), -1);

        Assert.assertEquals(executeWithURL("entity -definition -type cluster -name " + overlay.get("cluster")), 0);

        Assert.assertEquals(executeWithURL("entity -list -type process -fields status,tags "
                + " -tags owner=producer@xyz.com,department=forecasting "
                + " -filterBy STATUS:SUBMITTED,type:process "
                + " -orderBy name -sortOrder invalid -offset 1 -numResults 1"), -1);
        Assert.assertEquals(executeWithURL("instance -status -type feed -name "
                + overlay.get("outputFeedName") + " -start " + START_INSTANCE), 0);
        Assert.assertEquals(executeWithURL("instance -running -type process -name " + overlay.get("processName")), 0);

        // with doAs
        Assert.assertEquals(executeWithURL("entity -list -type process -doAs " + FalconTestUtil.TEST_USER_2
                + " -fields status -filterBy STATUS:SUBMITTED,TYPE:process -orderBy name "
                + " -sortOrder asc -offset 1 -numResults 1"), 0);
    }

    @Test
    public void testMetadataListCommands() throws Exception {
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        submitTestFiles(context, overlay);

        String processName = overlay.get("processName");
        String feedName = overlay.get("outputFeedName");
        String clusterName = overlay.get("cluster");

        Assert.assertEquals(executeWithURL(FalconCLIConstants.ENTITY_CMD + " -" + FalconCLIConstants.SCHEDULE_OPT + " -"
                        + FalconCLIConstants.TYPE_OPT + " process  -"
                + FalconCLIConstants.ENTITY_NAME_OPT + " " + processName), 0);

        Assert.assertEquals(executeWithURL(FalconCLIConstants.ENTITY_CMD + " -" + FalconCLIConstants.SCHEDULE_OPT + " -"
                + FalconCLIConstants.TYPE_OPT + " feed -" + FalconCLIConstants.ENTITY_NAME_OPT + " " + feedName), 0);

        OozieTestUtils.waitForProcessWFtoStart(context);

        String metadataListCommand = FalconCLIConstants.METADATA_CMD + " -" + FalconCLIConstants.LIST_OPT + " -"
                + FalconCLIConstants.TYPE_OPT + " ";
        String metadataListCommandWithDoAs = FalconCLIConstants.METADATA_CMD + " -doAs " + FalconTestUtil.TEST_USER_2
                + " -" + FalconCLIConstants.LIST_OPT + " -" + FalconCLIConstants.TYPE_OPT + " ";

        String clusterString = " -" + FalconCLIConstants.CLUSTER_OPT + " " + clusterName;

        Assert.assertEquals(executeWithURL(metadataListCommand + RelationshipType.CLUSTER_ENTITY.name()), 0);
        Assert.assertEquals(executeWithURL(metadataListCommand + RelationshipType.PROCESS_ENTITY.name()), 0);
        Assert.assertEquals(executeWithURL(metadataListCommand + RelationshipType.FEED_ENTITY.name()), 0);
        Assert.assertEquals(executeWithURL(metadataListCommand + RelationshipType.PROCESS_ENTITY.name()
                + clusterString), 0);
        Assert.assertEquals(executeWithURL(metadataListCommand + RelationshipType.FEED_ENTITY.name()
                + clusterString), 0);
        Assert.assertEquals(executeWithURL(metadataListCommand + RelationshipType.CLUSTER_ENTITY.name()
                + clusterString), 0);

        //with doAs
        Assert.assertEquals(executeWithURL(metadataListCommandWithDoAs + RelationshipType.FEED_ENTITY.name()), 0);

        Assert.assertEquals(executeWithURL(metadataListCommand + "feed"), -1);
        Assert.assertEquals(executeWithURL(metadataListCommand + "invalid"), -1);
    }

    @Test
    public void testMetadataRelationsCommands() throws Exception {
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        submitTestFiles(context, overlay);

        String processName = overlay.get("processName");
        String feedName = overlay.get("outputFeedName");
        String clusterName = overlay.get("cluster");

        Assert.assertEquals(executeWithURL(FalconCLIConstants.ENTITY_CMD + " -" + FalconCLIConstants.SCHEDULE_OPT + " -"
                        + FalconCLIConstants.TYPE_OPT + " process  -"
                + FalconCLIConstants.ENTITY_NAME_OPT + " " + processName), 0);

        Assert.assertEquals(executeWithURL(FalconCLIConstants.ENTITY_CMD + " -" + FalconCLIConstants.SCHEDULE_OPT + " -"
                + FalconCLIConstants.TYPE_OPT + " feed -" + FalconCLIConstants.ENTITY_NAME_OPT + " " + feedName), 0);

        OozieTestUtils.waitForProcessWFtoStart(context);

        String metadataRelationsCommand = FalconCLIConstants.METADATA_CMD + " -"
                + FalconCLIConstants.RELATIONS_OPT + " -" + FalconCLIConstants.TYPE_OPT + " ";

        String metadataRelationsCommandWithDoAs = FalconCLIConstants.METADATA_CMD
                + " -doAs " + FalconTestUtil.TEST_USER_2 + " -" + FalconCLIConstants.RELATIONS_OPT + " -"
                + FalconCLIConstants.TYPE_OPT + " ";

        Assert.assertEquals(executeWithURL(metadataRelationsCommand + RelationshipType.CLUSTER_ENTITY.name()
                + " -" + FalconCLIConstants.NAME_OPT + " " + clusterName), 0);
        Assert.assertEquals(executeWithURL(metadataRelationsCommand + RelationshipType.PROCESS_ENTITY.name()
                + " -" + FalconCLIConstants.NAME_OPT + " " + processName), 0);

        // with doAs
        Assert.assertEquals(executeWithURL(metadataRelationsCommandWithDoAs + RelationshipType.PROCESS_ENTITY.name()
                + " -" + FalconCLIConstants.NAME_OPT + " " + processName), 0);

        Assert.assertEquals(executeWithURL(metadataRelationsCommand + "feed -"
                + FalconCLIConstants.NAME_OPT + " " + clusterName), -1);

        Assert.assertEquals(executeWithURL(metadataRelationsCommand + "invalid -"
                + FalconCLIConstants.NAME_OPT + " " + clusterName), -1);
        Assert.assertEquals(executeWithURL(metadataRelationsCommand + RelationshipType.CLUSTER_ENTITY.name()), -1);
    }

    public void testContinue() throws Exception {
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        submitTestFiles(context, overlay);

        Assert.assertEquals(executeWithURL("entity -schedule -type process -name " + overlay.get("processName")), 0);

        Assert.assertEquals(executeWithURL("entity -schedule -type feed -name " + overlay.get("outputFeedName")), 0);

        OozieTestUtils.waitForProcessWFtoStart(context);

        Assert.assertEquals(executeWithURL("instance -kill -type process -name "
                + overlay.get("processName")
                + " -start " + START_INSTANCE + " -end " + START_INSTANCE), 0);

        Assert.assertEquals(executeWithURL("instance -kill -type feed -lifecycle eviction -name "
                + overlay.get("outputFeedName")
                + " -start "+ SchemaHelper.getDateFormat().format(new Date())
                + " -end " +  SchemaHelper.getDateFormat().format(new Date())), 0);

        Assert.assertEquals(executeWithURL("instance -rerun -type process -name "
                + overlay.get("processName")
                + " -start " + START_INSTANCE), -1);

        Assert.assertEquals(executeWithURL("instance -rerun -type process -name "
                + overlay.get("processName")
                + " -start " + START_INSTANCE + " -end " + START_INSTANCE), 0);

        Assert.assertEquals(executeWithURL("instance -rerun -type feed -lifecycle eviction -name "
                + overlay.get("outputFeedName")
                + " -start "+ SchemaHelper.getDateFormat().format(new Date())
                + " -end " + SchemaHelper.getDateFormat().format(new Date())), 0);
    }

    public void testInvalidCLIInstanceCommands() throws Exception {
        // no command
        Assert.assertEquals(executeWithURL(" -kill -type process -name "
                + "name" + " -start 2010-01-01T01:00Z  -end 2010-01-01T03:00Z"), -1);

        Assert.assertEquals(executeWithURL("instance -kill  " + "name"
                + " -start 2010-01-01T01:00Z  -end 2010-01-01T01:00Z"), -1);

        Assert.assertEquals(executeWithURL("instance -kill -type process -name " + "name"
                + " -end 2010-01-01T03:00Z"), -1);

        Assert.assertEquals(executeWithURL("instance -kill -type process -name "
                + " -start 2010-01-01T01:00Z  -end 2010-01-01T03:00Z"), -1);
    }

    public void testFalconURL() throws Exception {
        Assert.assertEquals(new FalconCLI()
                .run(("instance -status -type process -name " + "processName"
                        + " -start 2010-01-01T01:00Z  -end 2010-01-01T03:00Z")
                        .split("\\s")), -1);

        Assert.assertEquals(new FalconCLI()
                .run(("instance -status -type process -name "
                        + "processName -url http://unknownhost:1234/"
                        + " -start 2010-01-01T01:00Z  -end 2010-01-01T03:00Z")
                        .split("\\s")), -1);
    }

    public void testClientProperties() throws Exception {
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        submitTestFiles(context, overlay);

        Assert.assertEquals(new FalconCLI().run(("entity -schedule -type feed -name "
                + overlay.get("outputFeedName") + " -url "
                + TestContext.BASE_URL).split("\\s+")), 0);

        Assert.assertEquals(new FalconCLI().run(("entity -schedule -type process -name "
                + overlay.get("processName") + " -url "
                + TestContext.BASE_URL).split("\\s+")), 0);
    }

    public void testGetVersion() throws Exception {
        Assert.assertEquals(new FalconCLI().run(("admin -version -url " + TestContext.BASE_URL).split("\\s")), 0);
        Assert.assertEquals(new FalconCLI().run(("admin -doAs " + FalconTestUtil.TEST_USER_2 + " -version -url "
                + TestContext.BASE_URL).split("\\s")), 0);
    }

    public void testGetStatus() throws Exception {
        Assert.assertEquals(new FalconCLI().run(("admin -status -url " + TestContext.BASE_URL).split("\\s")), 0);
        Assert.assertEquals(new FalconCLI().run(("admin -doAs " + FalconTestUtil.TEST_USER_2 + " -status -url "
                + TestContext.BASE_URL).split("\\s")), 0);
    }

    public void testGetThreadStackDump() throws Exception {
        Assert.assertEquals(new FalconCLI().run(("admin -stack -url " + TestContext.BASE_URL).split("\\s")), 0);
        Assert.assertEquals(new FalconCLI().run(("admin -doAs " + FalconTestUtil.TEST_USER_2 + " -stack -url "
                + TestContext.BASE_URL).split("\\s")), 0);
    }

    public void testInstanceGetLogs() throws Exception {
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        submitTestFiles(context, overlay);

        Assert.assertEquals(executeWithURL("entity -schedule -type process -name " + overlay.get("processName")), 0);

        Assert.assertEquals(executeWithURL("entity -schedule -type feed -name " + overlay.get("outputFeedName")), 0);

        Thread.sleep(500);

        Assert.assertEquals(executeWithURL("instance -logs -type process -name "
                + overlay.get("processName")
                + " -start " + START_INSTANCE + " -end " + START_INSTANCE), 0);

        Assert.assertEquals(executeWithURL("instance -logs -type feed -lifecycle eviction -name "
                + overlay.get("outputFeedName")
                + " -start "+ SchemaHelper.getDateFormat().format(new Date())), 0);

        // with doAs
        Assert.assertEquals(executeWithURL("instance -logs -doAs " + FalconTestUtil.TEST_USER_2 + " -type feed "
                + "-lifecycle eviction -name " + overlay.get("outputFeedName") + " -start "
                + SchemaHelper.getDateFormat().format(new Date())), 0);

        // test filterBy, orderBy, offset, numResults
        Assert.assertEquals(executeWithURL("instance -logs -type process -name "
                + overlay.get("processName")
                + " -start " + START_INSTANCE + " -end " + START_INSTANCE
                + " -filterBy STATUS:SUCCEEDED -orderBy endtime "
                + " -sortOrder asc -offset 0 -numResults 1"), 0);
        Assert.assertEquals(executeWithURL("instance -logs -type process -name "
                + overlay.get("processName")
                + " -start " + START_INSTANCE + " -end " + START_INSTANCE
                + " -filterBy STATUS:SUCCEEDED -orderBy starttime "
                + " -sortOrder asc -offset 0 -numResults 1"), 0);
        Assert.assertEquals(executeWithURL("instance -logs -type process -name "
                + overlay.get("processName")
                + " -start " + START_INSTANCE + " -end " + START_INSTANCE
                + " -filterBy STATUS:SUCCEEDED -orderBy cluster "
                + " -sortOrder asc -offset 0 -numResults 1"), 0);

        Assert.assertEquals(executeWithURL("instance -logs -type process -name "
                + overlay.get("processName")
                + " -start " + START_INSTANCE + " -end " + START_INSTANCE
                + " -filterBy STATUS:WAITING -orderBy startTime -offset 0 -numResults 1"), 0);

        Assert.assertEquals(executeWithURL("instance -logs -type process -name "
                + overlay.get("processName")
                + " -start " + START_INSTANCE + " -end " + START_INSTANCE
                + " -filterBy STATUS:SUCCEEDED -orderBy endtime "
                + " -sortOrder invalid -offset 0 -numResults 1"), -1);
        Assert.assertEquals(executeWithURL("instance -logs -type process -name "
                + overlay.get("processName")
                + " -start " + START_INSTANCE + " -end " + START_INSTANCE
                + " -filterBy STATUS:SUCCEEDED,STARTEDAFTER:"+START_INSTANCE+" -offset 1 -numResults 1"), 0);
        Assert.assertEquals(executeWithURL("instance -logs -type process -name "
                + overlay.get("processName")
                + " -start " + START_INSTANCE + " -end " + START_INSTANCE
                + " -filterBy INVALID:FILTER -orderBy startTime -offset 0 -numResults 1"), -1);
        Assert.assertEquals(executeWithURL("instance -logs -type process -name "
                + overlay.get("processName")
                + " -start " + START_INSTANCE + " -end " + START_INSTANCE
                + " -filterBy STATUS:SUCCEEDED -orderBy wrongOrder -offset 0 -numResults 1"), -1);
    }

    private int executeWithURL(String command) throws Exception {
        //System.out.println("COMMAND IS "+command + " -url " + TestContext.BASE_URL);
        return new FalconCLI()
                .run((command + " -url " + TestContext.BASE_URL).split("\\s+"));
    }

    private String createTempJobPropertiesFile() throws IOException {
        File target = new File("webapp/target");
        if (!target.exists()) {
            target = new File("target");
        }
        File tmpFile = File.createTempFile("job", ".properties", target);
        OutputStream out = new FileOutputStream(tmpFile);
        out.write("oozie.wf.rerun.failnodes=true\n".getBytes());
        out.close();
        return tmpFile.getAbsolutePath();
    }

    private void submitTestFiles(TestContext context, Map<String, String> overlay) throws Exception {

        String filePath = TestContext.overlayParametersOverTemplate(context.getClusterFileTemplate(),
                overlay);
        Assert.assertEquals(executeWithURL("entity -submit -type cluster -file " + filePath), 0);
        context.setCluster(overlay.get("cluster"));

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE1, overlay);
        Assert.assertEquals(executeWithURL("entity -submit -type feed -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE2, overlay);
        Assert.assertEquals(executeWithURL("entity -submit -type feed -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
        Assert.assertEquals(executeWithURL("entity -submit -type process -file " + filePath), 0);
    }

    private static class InMemoryWriter extends PrintStream {

        private StringBuffer buffer = new StringBuffer();

        public InMemoryWriter(OutputStream out) {
            super(out);
        }

        @Override
        public void println(String x) {
            clear();
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
