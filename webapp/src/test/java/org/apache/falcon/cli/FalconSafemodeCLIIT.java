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

import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.resource.TestContext;
import org.apache.falcon.util.FalconTestUtil;
import org.apache.falcon.util.StartupProperties;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Date;
import java.util.Map;

/**
 * Test for Falcon CLI.
 */
@Test(groups = {"exhaustive"})
public class FalconSafemodeCLIIT {
    private InMemoryWriter stream = new InMemoryWriter(System.out);
    private TestContext context = new TestContext();
    private Map<String, String> overlay;
    private static final String START_INSTANCE = "2012-04-20T00:00Z";

    @BeforeClass
    public void prepare() throws Exception {
        TestContext.prepare();
        FalconCLI.OUT.set(stream);
        initSafemode();
    }

    @AfterClass
    public void tearDown() throws Exception {
        clearSafemode();
        TestContext.deleteEntitiesFromStore();

    }

    private void initSafemode() throws Exception {
        overlay = context.getUniqueOverlay();
        // Submit one cluster
        String filePath = TestContext.overlayParametersOverTemplate(TestContext.CLUSTER_TEMPLATE, overlay);
        Assert.assertEquals(executeWithURL("entity -submit -type cluster -file " + filePath), 0);
        context.setCluster(overlay.get("cluster"));
        Assert.assertEquals(stream.buffer.toString().trim(),
                "falcon/default/Submit successful (cluster) " + context.getClusterName());

        // Submit and schedule one feed
        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE1, overlay);
        Assert.assertEquals(executeWithURL("entity -submitAndSchedule -type feed -file " + filePath), 0);

        // Schedule the feed
        Assert.assertEquals(executeWithURL("entity -status -type feed -name " + overlay.get("inputFeedName")), 0);

        // Test the lookup command
        Assert.assertEquals(executeWithURL("entity -lookup -type feed -path "
                + "/falcon/test/input/2014/11/23/23"), 0);

        // Set safemode
        Assert.assertEquals(new FalconCLI().run(("admin -setsafemode true -url "
                + TestContext.BASE_URL).split("\\s")), 0);
    }

    private void clearSafemode() throws Exception {
        Assert.assertEquals(new FalconCLI().run(("admin -setsafemode false -url "
                + TestContext.BASE_URL).split("\\s")), 0);
        Assert.assertEquals(StartupProperties.get().getProperty(StartupProperties.SAFEMODE_PROPERTY, "false"),
                "false");
    }

    public void testEntityCommandsNotAllowedInSafeMode() throws Exception {
        String filePath;

        filePath = TestContext.overlayParametersOverTemplate(TestContext.CLUSTER_TEMPLATE, overlay);
        Assert.assertEquals(executeWithURL("entity -submit -type cluster -file " + filePath), -1);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE2, overlay);
        Assert.assertEquals(executeWithURL("entity -submit -type feed -file " + filePath), -1);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
        Assert.assertEquals(executeWithURL("entity -submit -type process -doAs " + FalconTestUtil.TEST_USER_2
                + " -file " + filePath), -1);

        filePath = TestContext.overlayParametersOverTemplate(context.getClusterFileTemplate(), overlay);
        Assert.assertEquals(executeWithURL("entity -submitAndSchedule -type cluster -file " + filePath), -1);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE2, overlay);
        Assert.assertEquals(executeWithURL("entity -submitAndSchedule -type feed -doAs " + FalconTestUtil.TEST_USER_2
                + " -file " + filePath), -1);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
        Assert.assertEquals(executeWithURL("entity -submitAndSchedule -type process -file " + filePath), -1);

        Assert.assertEquals(executeWithURL("entity -update -name " + overlay.get("processName")
                + " -type process -file " + filePath), -1);

        Assert.assertEquals(executeWithURL("entity -touch -name " + overlay.get("processName")
                + " -type process"), -1);

        Assert.assertEquals(executeWithURL("entity -schedule -type feed "
                + " -name " + overlay.get("inputFeedName")), -1);

        Assert.assertEquals(executeWithURL("entity -resume -type feed "
                + " -name " + overlay.get("inputFeedName")), -1);

        Assert.assertEquals(executeWithURL("entity -delete -type feed -name " + overlay.get("inputFeedName")), -1);


    }

    public void testEntityCommandsAllowedInSafeMode() throws Exception {
        // Allow definition, summary, list, suspend operations
        Assert.assertEquals(executeWithURL("entity -definition -type cluster -name " + overlay.get("cluster")), 0);

        Assert.assertEquals(executeWithURL("entity -suspend -type feed "
                + " -name " + overlay.get("inputFeedName")), 0);

        Assert.assertEquals(executeWithURL("entity -summary -type feed -cluster "+ overlay.get("cluster")
                + " -fields status,tags -start " + START_INSTANCE
                + " -filterBy TYPE:FEED -orderBy name -sortOrder asc "
                + " -offset 0 -numResults 1 -numInstances 5"), 0);

        Assert.assertEquals(executeWithURL("instance -list -type feed "
                + " -name " + overlay.get("inputFeedName") + " -start "
                + SchemaHelper.getDateFormat().format(new Date())), 0);

        Assert.assertEquals(executeWithURL("instance -kill -type feed -name "
                + overlay.get("inputFeedName")
                + " -start " + START_INSTANCE + " -end " + START_INSTANCE), 0);

    }

    private int executeWithURL(String command) throws Exception {
        FalconCLI.OUT.get().print("COMMAND IS "+command + " -url " + TestContext.BASE_URL + "\n");
        return new FalconCLI()
                .run((command + " -url " + TestContext.BASE_URL).split("\\s+"));
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
