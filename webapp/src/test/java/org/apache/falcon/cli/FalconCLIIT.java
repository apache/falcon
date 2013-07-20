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
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Map;

/**
 * Test for Falcon CLI.
 *
 * todo: Refactor both the classes to move this methods to helper;
 */
public class FalconCLIIT {

    private InMemoryWriter stream = new InMemoryWriter(System.out);
    // private static final String BROKER_URL =
    // "tcp://localhost:61616?daemon=true";
    private static final boolean TEST_ENABLED = true;

    @BeforeClass
    public void prepare() throws Exception {
        TestContext.prepare();
    }

    @Test(enabled = TEST_ENABLED)
    public void testSubmitEntityValidCommands() throws Exception {

        FalconCLI.OUT.set(stream);

        String filePath;
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();

        filePath = context.overlayParametersOverTemplate(TestContext.CLUSTER_TEMPLATE, overlay);
        Assert.assertEquals(
                0,
                executeWithURL("entity -submit -type cluster -file " + filePath));
        context.setCluster(filePath);
        Assert.assertEquals(stream.buffer.toString().trim(),
                "default/Submit successful (cluster) " + context.getClusterName());

        filePath = context.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE1, overlay);
        Assert.assertEquals(0,
                executeWithURL("entity -submit -type feed -file " + filePath));
        Assert.assertEquals(
                stream.buffer.toString().trim(),
                "default/Submit successful (feed) "
                        + overlay.get("inputFeedName"));

        filePath = context.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE2, overlay);
        Assert.assertEquals(0,
                executeWithURL("entity -submit -type feed -file " + filePath));
        Assert.assertEquals(
                stream.buffer.toString().trim(),
                "default/Submit successful (feed) "
                        + overlay.get("outputFeedName"));

        filePath = context.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
        Assert.assertEquals(
                0,
                executeWithURL("entity -submit -type process -file " + filePath));
        Assert.assertEquals(
                stream.buffer.toString().trim(),
                "default/Submit successful (process) "
                        + overlay.get("processName"));
    }

    @Test(enabled = TEST_ENABLED)
    public void testListWithEmptyConfigStore() throws Exception {
        Assert.assertEquals(
                0,
                executeWithURL("entity -list -type process "));
    }

    @Test(enabled = TEST_ENABLED)
    public void testSubmitAndScheduleEntityValidCommands() throws Exception {

        Thread.sleep(5000);
        String filePath;
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();

        filePath = context.overlayParametersOverTemplate(context.getClusterFileTemplate(), overlay);
        Assert.assertEquals(-1,
                executeWithURL("entity -submitAndSchedule -type cluster -file "
                        + filePath));
        context.setCluster(filePath);

        filePath = context.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE1, overlay);
        Assert.assertEquals(0,
                executeWithURL("entity -submitAndSchedule -type feed -file "
                        + filePath));
        filePath = context.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE2, overlay);
        Assert.assertEquals(0,
                executeWithURL("entity -submitAndSchedule -type feed -file "
                        + filePath));
        filePath = context.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE1, overlay);
        Assert.assertEquals(0,
                executeWithURL("entity -submit -type feed -file " + filePath));

        filePath = context.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE2, overlay);
        Assert.assertEquals(0,
                executeWithURL("entity -submit -type feed -file " + filePath));

        filePath = context.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
        Assert.assertEquals(0,
                executeWithURL("entity -submitAndSchedule -type process -file "
                        + filePath));

        Thread.sleep(5000);
    }

    @Test(enabled = TEST_ENABLED)
    public void testValidateValidCommands() throws Exception {

        String filePath;
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();

        filePath = context.overlayParametersOverTemplate(context.getClusterFileTemplate(), overlay);
        Assert.assertEquals(0,
                executeWithURL("entity -validate -type cluster -file "
                        + filePath));
        context.setCluster(filePath);
        Assert.assertEquals(
                0,
                executeWithURL("entity -submit -type cluster -file " + filePath));
        context.setCluster(filePath);

        filePath = context.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE1, overlay);
        Assert.assertEquals(0,
                executeWithURL("entity -validate -type feed -file " + filePath));
        Assert.assertEquals(0,
                executeWithURL("entity -submit -type feed -file " + filePath));

        filePath = context.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE2, overlay);
        Assert.assertEquals(0,
                executeWithURL("entity -validate -type feed -file " + filePath));
        Assert.assertEquals(0,
                executeWithURL("entity -submit -type feed -file " + filePath));

        filePath = context.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
        Assert.assertEquals(0,
                executeWithURL("entity -validate -type process -file "
                        + filePath));
        Assert.assertEquals(
                0,
                executeWithURL("entity -submit -type process -file " + filePath));

    }

    @Test(enabled = TEST_ENABLED)
    public void testDefinitionEntityValidCommands() throws Exception {
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        submitTestFiles(context, overlay);

        Assert.assertEquals(0,
                executeWithURL("entity -definition -type cluster -name "
                        + overlay.get("cluster")));

        Assert.assertEquals(
                0,
                executeWithURL("entity -definition -type feed -name "
                        + overlay.get("inputFeedName")));

        Assert.assertEquals(
                0,
                executeWithURL("entity -definition -type feed -name "
                        + overlay.get("outputFeedName")));

        Assert.assertEquals(0,
                executeWithURL("entity -definition -type process -name "
                        + overlay.get("processName")));

    }

    @Test(enabled = TEST_ENABLED)
    public void testScheduleEntityValidCommands() throws Exception {

        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        submitTestFiles(context, overlay);

        Assert.assertEquals(-1,
                executeWithURL("entity -schedule -type cluster -name "
                        + overlay.get("cluster")));

        Assert.assertEquals(
                0,
                executeWithURL("entity -schedule -type feed -name "
                        + overlay.get("outputFeedName")));

        Assert.assertEquals(0,
                executeWithURL("entity -schedule -type process -name "
                        + overlay.get("processName")));

    }

    @Test(enabled = TEST_ENABLED)
    public void testSuspendResumeStatusEntityValidCommands() throws Exception {

        Thread.sleep(5000);
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        submitTestFiles(context, overlay);

        Assert.assertEquals(
                0,
                executeWithURL("entity -status -type feed -name "
                        + overlay.get("outputFeedName")));

        Assert.assertEquals(
                0,
                executeWithURL("entity -status -type process -name "
                        + overlay.get("processName")));

        Assert.assertEquals(
                0,
                executeWithURL("entity -schedule -type feed -name "
                        + overlay.get("outputFeedName")));

        Assert.assertEquals(0,
                executeWithURL("entity -schedule -type process -name "
                        + overlay.get("processName")));

        context.waitForProcessWFtoStart();

        Assert.assertEquals(
                0,
                executeWithURL("entity -suspend -type feed -name "
                        + overlay.get("outputFeedName")));

        Assert.assertEquals(
                0,
                executeWithURL("entity -suspend -type process -name "
                        + overlay.get("processName")));

        Assert.assertEquals(
                0,
                executeWithURL("entity -status -type feed -name "
                        + overlay.get("outputFeedName")));

        Assert.assertEquals(
                0,
                executeWithURL("entity -status -type process -name "
                        + overlay.get("processName")));

        Assert.assertEquals(
                0,
                executeWithURL("entity -resume -type feed -name "
                        + overlay.get("outputFeedName")));

        Assert.assertEquals(
                0,
                executeWithURL("entity -resume -type process -name "
                        + overlay.get("processName")));

        Assert.assertEquals(
                0,
                executeWithURL("entity -status -type feed -name "
                        + overlay.get("outputFeedName")));

        Assert.assertEquals(
                0,
                executeWithURL("entity -status -type process -name "
                        + overlay.get("processName")));

        Thread.sleep(5000);
    }

    @Test(enabled = TEST_ENABLED)
    public void testSubCommandPresence() throws Exception {
        Assert.assertEquals(-1, executeWithURL("entity -type cluster "));
    }

    @Test(enabled = TEST_ENABLED)
    public void testDeleteEntityValidCommands() throws Exception {

        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        submitTestFiles(context, overlay);

        Assert.assertEquals(
                -1,
                executeWithURL("entity -delete -type cluster -name "
                        + overlay.get("cluster")));

        Assert.assertEquals(
                -1,
                executeWithURL("entity -delete -type feed -name "
                        + overlay.get("inputFeedName")));

        Assert.assertEquals(
                -1,
                executeWithURL("entity -delete -type feed -name "
                        + overlay.get("outputFeedName")));

        Assert.assertEquals(
                0,
                executeWithURL("entity -delete -type process -name "
                        + overlay.get("processName")));

        Assert.assertEquals(
                0,
                executeWithURL("entity -delete -type feed -name "
                        + overlay.get("inputFeedName")));

        Assert.assertEquals(
                0,
                executeWithURL("entity -delete -type feed -name "
                        + overlay.get("outputFeedName")));

    }

    @Test(enabled = TEST_ENABLED)
    public void testInvalidCLIEntitycommands() throws Exception {

        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        context.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE1, overlay);
        Assert.assertEquals(-1,
                executeWithURL("entity -submit -type feed -name " + "name"));

        Assert.assertEquals(-1,
                executeWithURL("entity -schedule -type feed -file " + "name"));
    }

    @Test(enabled = TEST_ENABLED)
    public void testInstanceRunningAndStatusCommands() throws Exception {
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        submitTestFiles(context, overlay);

        Assert.assertEquals(0,
                executeWithURL("entity -schedule -type process -name "
                        + overlay.get("processName")));

        Assert.assertEquals(0,
                executeWithURL("entity -schedule -type feed -name "
                        + overlay.get("outputFeedName")));
        context.waitForProcessWFtoStart();

        Assert.assertEquals(0,
                executeWithURL("instance -status -type feed -name "
                        + overlay.get("outputFeedName")
                        + " -start " + START_INSTANCE));

        Assert.assertEquals(0,
                executeWithURL("instance -running -type process -name "
                        + overlay.get("processName")));

        Assert.assertEquals(0,
                executeWithURL("instance -status -type process -name "
                        + overlay.get("processName")
                        + " -start " + START_INSTANCE));
    }

    @Test(enabled = TEST_ENABLED)
    public void testInstanceSuspendAndResume() throws Exception {
        Thread.sleep(5000);
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        submitTestFiles(context, overlay);

        Assert.assertEquals(0,
                executeWithURL("entity -schedule -type process -name "
                        + overlay.get("processName")));


        Assert.assertEquals(0,
                executeWithURL("instance -suspend -type process -name "
                        + overlay.get("processName")
                        + " -start " + START_INSTANCE + " -end " + START_INSTANCE));

        Assert.assertEquals(0,
                executeWithURL("instance -resume -type process -name "
                        + overlay.get("processName")
                        + " -start " + START_INSTANCE + " -end " + START_INSTANCE));
        Thread.sleep(5000);
    }

    private static final String START_INSTANCE = "2012-04-20T00:00Z";

    @Test(enabled = TEST_ENABLED)
    public void testInstanceKillAndRerun() throws Exception {
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        submitTestFiles(context, overlay);

        Assert.assertEquals(0,
                executeWithURL("entity -schedule -type process -name "
                        + overlay.get("processName")));

        context.waitForProcessWFtoStart();
        Assert.assertEquals(
                0,
                executeWithURL("instance -kill -type process -name "
                        + overlay.get("processName")
                        + " -start " + START_INSTANCE + " -end " + START_INSTANCE));

        Assert.assertEquals(
                0,
                executeWithURL("instance -rerun -type process -name "
                        + overlay.get("processName")
                        + " -start " + START_INSTANCE + " -file "
                        + createTempJobPropertiesFile()));
    }

    @Test(enabled = TEST_ENABLED)
    public void testContinue() throws Exception {
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        submitTestFiles(context, overlay);

        Assert.assertEquals(0,
                executeWithURL("entity -schedule -type process -name "
                        + overlay.get("processName")));

        context.waitForProcessWFtoStart();
        Assert.assertEquals(
                0,
                executeWithURL("instance -kill -type process -name "
                        + overlay.get("processName")
                        + " -start " + START_INSTANCE + " -end " + START_INSTANCE));

        Assert.assertEquals(
                0,
                executeWithURL("instance -continue -type process -name "
                        + overlay.get("processName")
                        + " -start " + START_INSTANCE));
    }

    @Test(enabled = TEST_ENABLED)
    public void testInvalidCLIInstanceCommands() throws Exception {
        // no command
        Assert.assertEquals(-1, executeWithURL(" -kill -type process -name "
                + "name" + " -start 2010-01-01T01:00Z  -end 2010-01-01T03:00Z"));

        Assert.assertEquals(-1, executeWithURL("instance -kill  " + "name"
                + " -start 2010-01-01T01:00Z  -end 2010-01-01T01:00Z"));

        Assert.assertEquals(-1,
                executeWithURL("instance -kill -type process -name " + "name"
                        + " -end 2010-01-01T03:00Z"));

        Assert.assertEquals(-1,
                executeWithURL("instance -kill -type process -name "
                        + " -start 2010-01-01T01:00Z  -end 2010-01-01T03:00Z"));

    }

    @Test(enabled = TEST_ENABLED)
    public void testFalconURL() throws Exception {
        Assert.assertEquals(-1, new FalconCLI()
                .run(("instance -status -type process -name " + "processName"
                        + " -start 2010-01-01T01:00Z  -end 2010-01-01T03:00Z")
                        .split("\\s")));

        Assert.assertEquals(-1, new FalconCLI()
                .run(("instance -status -type process -name "
                        + "processName -url http://unknownhost:1234/"
                        + " -start 2010-01-01T01:00Z  -end 2010-01-01T03:00Z")
                        .split("\\s")));


    }

    @Test(enabled = TEST_ENABLED)
    public void testClientProperties() throws Exception {
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        submitTestFiles(context, overlay);

        Assert.assertEquals(
                0,
                new FalconCLI().run(("entity -schedule -type feed -name "
                        + overlay.get("outputFeedName") + " -url "
                        + TestContext.BASE_URL).split("\\s+")));

        Assert.assertEquals(0,
                new FalconCLI().run(("entity -schedule -type process -name "
                        + overlay.get("processName")+ " -url "
                        + TestContext.BASE_URL).split("\\s+")));

    }

    @Test(enabled = TEST_ENABLED)
    public void testGetVersion() throws Exception {
        Assert.assertEquals(0,
                new FalconCLI().run("admin -version".split("\\s")));

        Assert.assertEquals(0,
                new FalconCLI().run("admin -stack".split("\\s")));
    }

    @Test(enabled = TEST_ENABLED)
    public void testInstanceGetLogs() throws Exception {
        TestContext context = new TestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        submitTestFiles(context, overlay);

        Assert.assertEquals(0,
                executeWithURL("entity -schedule -type process -name "
                        + overlay.get("processName")));

        Assert.assertEquals(0,
                executeWithURL("instance -logs -type process -name "
                        + overlay.get("processName")
                        + " -start " + START_INSTANCE + " -end " + START_INSTANCE));

    }


    private int executeWithURL(String command) throws Exception {
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

    public void submitTestFiles(TestContext context, Map<String, String> overlay) throws Exception {

        String filePath = context.overlayParametersOverTemplate(context.getClusterFileTemplate(),
                overlay);
        Assert.assertEquals(
                0,
                executeWithURL("entity -submit -type cluster -file " + filePath));
        context.setCluster(filePath);

        filePath = context.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE1, overlay);
        Assert.assertEquals(0,
                executeWithURL("entity -submit -type feed -file " + filePath));

        filePath = context.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE2, overlay);
        Assert.assertEquals(0,
                executeWithURL("entity -submit -type feed -file " + filePath));

        filePath = context.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
        Assert.assertEquals(
                0,
                executeWithURL("entity -submit -type process -file " + filePath));
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

        public String getBuffer() {
            return buffer.toString();
        }

        public void clear() {
            buffer.delete(0, buffer.length());
        }
    }
}
