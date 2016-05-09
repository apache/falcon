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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Map;

/**
 * Test for Falcon CLI.
 */
@Test(groups = {"exhaustive"})
public class FalconClusterUpdateCLIIT {
    private InMemoryWriter stream = new InMemoryWriter(System.out);
    private TestContext context = new TestContext();
    private Map<String, String> overlay;

    @BeforeClass
    public void prepare() throws Exception {
        context.prepare();
        FalconCLI.OUT.set(stream);
    }

    @AfterClass
    public void tearDown() throws Exception {
        clearSafemode();
        context.deleteEntitiesFromStore();
    }


    public void testUpdateClusterCommands() throws Exception {

        FalconCLI.OUT.set(stream);

        String filePath;
        overlay = context.getUniqueOverlay();

        filePath = TestContext.overlayParametersOverTemplate(TestContext.CLUSTER_TEMPLATE, overlay);
        Assert.assertEquals(executeWithURL("entity -submit -type cluster -file " + filePath), 0);
        context.setCluster(overlay.get("cluster"));
        Assert.assertEquals(stream.buffer.toString().trim(),
                "falcon/default/Submit successful (cluster) " + context.getClusterName());

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE1, overlay);
        Assert.assertEquals(executeWithURL("entity -submit -type feed -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE2, overlay);
        Assert.assertEquals(executeWithURL("entity -submit -type feed -file " + filePath), 0);

        filePath = TestContext.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
        Assert.assertEquals(executeWithURL("entity -submit -type process -file " + filePath), 0);


        // Update cluster here and test that it works

        initSafemode();
        filePath = TestContext.overlayParametersOverTemplate(TestContext.CLUSTER_UPDATED_TEMPLATE, overlay);
        Assert.assertEquals(executeWithURL("entity -update -type cluster -file "
                + filePath + " -name " + overlay.get("cluster")), 0);
        clearSafemode();

        // Try to update dependent entities
        Assert.assertEquals(executeWithURL("entity -updateClusterDependents -cluster "
                + overlay.get("cluster") + " -skipDryRun "), 0);

        // try to update cluster with wrong name, it should fail.
        initSafemode();
        overlay = context.getUniqueOverlay();
        filePath = TestContext.overlayParametersOverTemplate(TestContext.CLUSTER_UPDATED_TEMPLATE, overlay);
        Assert.assertEquals(executeWithURL("entity -update -type cluster -file "
                + filePath + " -name " + overlay.get("cluster")), -1);
        clearSafemode();
    }


    private void initSafemode() throws Exception {
        // Set safemode
        Assert.assertEquals(new FalconCLI().run(("admin -setsafemode true -url "
                + TestContext.BASE_URL).split("\\s")), 0);
    }

    private void clearSafemode() throws Exception {
        Assert.assertEquals(new FalconCLI().run(("admin -setsafemode false -url "
                + TestContext.BASE_URL).split("\\s")), 0);
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
