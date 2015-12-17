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
package org.apache.falcon.resource;

import org.apache.commons.io.FileUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Property;
import org.apache.falcon.util.OozieTestUtils;
import org.apache.oozie.client.BundleJob;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Test class for Entity REST APIs.
 *
 * Tests should be enabled only in local environments as they need running instance of the web server.
 */
@Test
public class EntityManagerJerseySmokeIT extends AbstractSchedulerManagerJerseyIT {

    private UnitTestContext context;

    @BeforeClass
    @Override
    public void setup() throws Exception {
        String version = System.getProperty("project.version");
        String buildDir = System.getProperty("project.build.directory");
        System.setProperty("falcon.libext", buildDir + "/../../unit/target/falcon-unit-" + version + ".jar");
        super.setup();
    }

    @AfterClass
    public void tearDown() throws Exception {
        TestContext.deleteEntitiesFromStore();
    }

    private ThreadLocal<UnitTestContext> contexts = new ThreadLocal<UnitTestContext>();

    private UnitTestContext newContext() throws FalconException, IOException {
        contexts.set(new UnitTestContext());
        return contexts.get();
    }

    @AfterMethod
    @Override
    public void cleanUpActionXml() throws IOException, FalconException {
        //Needed since oozie writes action xml to current directory.
        FileUtils.deleteQuietly(new File("action.xml"));
        FileUtils.deleteQuietly(new File(".action.xml.crc"));
        contexts.remove();
    }

    @Test (dependsOnMethods = "testFeedSchedule")
    public void testProcessDeleteAndSchedule() throws Exception {
        //Schedule process, delete and then submitAndSchedule again should create new bundle
        String tmpFileName = TestContext.overlayParametersOverTemplate(UnitTestContext.PROCESS_TEMPLATE,
                context.overlay);
        Process process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(new File(tmpFileName));
        Property prop = new Property();
        prop.setName("newProp");
        prop.setValue("${instanceTime()}");
        process.getProperties().getProperties().add(prop);
        File tmpFile = TestContext.getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);
        context.prepare();
        submitProcess(tmpFile.getAbsolutePath(), context.overlay);
        scheduleProcess(context);

        APIResult result = falconUnitClient.delete(EntityType.PROCESS, context.getProcessName(), null);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

        process.getWorkflow().setPath("/falcon/test/workflow");
        tmpFile = TestContext.getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);

        falconUnitClient.submitAndSchedule(EntityType.PROCESS.name(), tmpFile.getAbsolutePath(), true,
                null, null);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

        //Assert that new schedule creates new bundle
        List<BundleJob> bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 2);
    }

    @Test
    public void testFeedSchedule() throws Exception {
        context = newContext();
        Map<String, String> overlay = context.overlay;

        submitCluster(context);

        submitFeed(UnitTestContext.FEED_TEMPLATE1, overlay);
        submitFeed(UnitTestContext.FEED_TEMPLATE2, overlay);

        createTestData();
        APIResult result = schedule(EntityType.FEED, context.getInputFeedName(), context.getClusterName());
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    }
}
