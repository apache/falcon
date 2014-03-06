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

import com.sun.jersey.api.client.ClientResponse;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Property;
import org.apache.falcon.util.OozieTestUtils;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.Job.Status;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;
import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Test class for Entity REST APIs.
 *
 * Tests should be enabled only in local environments as they need running instance of the web server.
 */
@Test
public class EntityManagerJerseySmokeIT {

    @BeforeClass
    public void prepare() throws Exception {
        TestContext.prepare();
    }

    private ThreadLocal<TestContext> contexts = new ThreadLocal<TestContext>();

    private TestContext newContext() {
        contexts.set(new TestContext());
        return contexts.get();
    }

    @AfterMethod
    public void cleanup() throws Exception {
        TestContext testContext = contexts.get();
        if (testContext != null) {
            OozieTestUtils.killOozieJobs(testContext);
        }
        contexts.remove();
    }

    @Test (dependsOnMethods = "testFeedSchedule")
    public void testProcessDeleteAndSchedule() throws Exception {
        //Submit process with invalid property so that coord submit fails and bundle goes to failed state
        TestContext context = newContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        String tmpFileName = TestContext.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
        Process process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(new File(tmpFileName));
        Property prop = new Property();
        prop.setName("newProp");
        prop.setValue("${formatTim()}");
        process.getProperties().getProperties().add(prop);
        File tmpFile = TestContext.getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);
        context.scheduleProcess(tmpFile.getAbsolutePath(), overlay);
        OozieTestUtils.waitForBundleStart(context, Status.FAILED);

        //Delete and re-submit the process with correct workflow
        ClientResponse clientResponse = context.service
                .path("api/entities/delete/process/" + context.processName)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML)
                .delete(ClientResponse.class);
        context.assertSuccessful(clientResponse);

        process.getWorkflow().setPath("/falcon/test/workflow");
        tmpFile = TestContext.getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);

        clientResponse = context.service.path("api/entities/submitAndSchedule/process")
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class, context.getServletInputStream(tmpFile.getAbsolutePath()));
        context.assertSuccessful(clientResponse);

        //Assert that new schedule creates new bundle
        List<BundleJob> bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 2);
    }

    @Test
    public void testFeedSchedule() throws Exception {
        TestContext context = newContext();
        ClientResponse response;
        Map<String, String> overlay = context.getUniqueOverlay();

        response = context.submitToFalcon(TestContext.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.FEED_TEMPLATE1, overlay, EntityType.FEED);
        context.assertSuccessful(response);

        EntityManagerJerseyIT.createTestData(context);
        ClientResponse clientRepsonse = context.service
                .path("api/entities/schedule/feed/" + overlay.get("inputFeedName"))
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class);
        context.assertSuccessful(clientRepsonse);
    }
}
