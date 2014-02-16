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
import org.testng.Assert;
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
    }

    @Test
    public void testSubmitAndScheduleEntityValidCommands() throws Exception {

        String filePath;
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();

        filePath = TestContext.overlayParametersOverTemplate(context.getClusterFileTemplate(), overlay);
        Assert.assertEquals(-1,
                executeWithURL("entity -submitAndSchedule -type cluster -file "
                        + filePath));
        context.setCluster(overlay.get("cluster"));

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE1, overlay);
        Assert.assertEquals(0,
                executeWithURL("entity -submitAndSchedule -type feed -file "
                        + filePath));
        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE2, overlay);
        Assert.assertEquals(0,
                executeWithURL("entity -submitAndSchedule -type feed -file "
                        + filePath));
        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE1, overlay);
        Assert.assertEquals(0,
                executeWithURL("entity -submit -type feed -file " + filePath));

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE2, overlay);
        Assert.assertEquals(0,
                executeWithURL("entity -submit -type feed -file " + filePath));

        filePath = TestContext.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
        Assert.assertEquals(0,
                executeWithURL("entity -validate -type process -file "
                        + filePath));

        filePath = TestContext.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
        Assert.assertEquals(0,
                executeWithURL("entity -submitAndSchedule -type process -file "
                        + filePath));

        OozieTestUtils.waitForProcessWFtoStart(context);

        Assert.assertEquals(0,
                executeWithURL("entity -definition -type cluster -name "
                        + overlay.get("cluster")));

        Assert.assertEquals(0,
                executeWithURL("instance -status -type feed -name "
                        + overlay.get("outputFeedName")
                        + " -start " + START_INSTANCE));

        Assert.assertEquals(0,
                executeWithURL("instance -running -type process -name "
                        + overlay.get("processName")));
    }

    private int executeWithURL(String command) throws Exception {
        return new FalconCLI()
                .run((command + " -url " + TestContext.BASE_URL).split("\\s+"));
    }
}
