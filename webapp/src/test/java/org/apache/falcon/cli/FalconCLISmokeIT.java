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

import org.apache.falcon.resource.TestContext;
import org.apache.falcon.util.OozieTestUtils;
import org.apache.falcon.util.StartupProperties;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;

/**
 * Smoke Test for Falcon CLI.
 */
public class FalconCLISmokeIT {

    private static final String START_INSTANCE = "2012-04-20T00:00Z";

    @BeforeClass
    public void prepare() throws Exception {
        TestContext.prepare();

        String services = StartupProperties.get().getProperty("application.services");
        StartupProperties.get().setProperty("application.services",
                services + ",org.apache.falcon.metadata.MetadataMappingService");
    }

    @AfterClass
    public void tearDown() throws Exception {
        TestContext.deleteEntitiesFromStore();
    }

    @Test
    public void testSubmitAndScheduleEntityValidCommands() throws Exception {

        String filePath;
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();

        filePath = TestContext.overlayParametersOverTemplate(context.getClusterFileTemplate(), overlay);
        Assert.assertEquals(executeWithURL("entity -submitAndSchedule -type cluster -file " + filePath), -1);
        context.setCluster(overlay.get("cluster"));

        // this is necessary for lineage
        Assert.assertEquals(executeWithURL("entity -submit -type cluster -file " + filePath), 0);
        // verify
        Assert.assertEquals(executeWithURL("metadata -vertices -key name -value " + context.getClusterName()), 0);

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
        Assert.assertEquals(executeWithURL("entity -list -type cluster -offset 0 -numResults 1"), 0);
        Assert.assertEquals(executeWithURL("entity -list -type process -fields status "
                        + " -filterBy STATUS:SUBMITTED,TYPE:process -orderBy name -offset 1 -numResults 1"), 0);
        Assert.assertEquals(executeWithURL("entity -list -type process -fields status "
                        + " -filterBy STATUS:SUCCEEDED,TYPE:process -orderBy INVALID -offset 0 -numResults 1"), -1);
        Assert.assertEquals(executeWithURL("entity -definition -type cluster -name " + overlay.get("cluster")), 0);

        Assert.assertEquals(executeWithURL("instance -status -type feed -name "
                        + overlay.get("outputFeedName") + " -start " + START_INSTANCE), 0);
        Assert.assertEquals(executeWithURL("instance -running -type process -name " + overlay.get("processName")), 0);
        Assert.assertEquals(executeWithURL("instance -search"), 0);
        Assert.assertEquals(executeWithURL("instance -search -type process -instanceStatus RUNNING"), 0);
    }

    private int executeWithURL(String command) throws Exception {
        return new FalconCLI()
                .run((command + " -url " + TestContext.BASE_URL).split("\\s+"));
    }
}
