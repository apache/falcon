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
import com.sun.jersey.api.client.WebResource;
import org.apache.falcon.client.FalconClient;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Property;
import org.apache.falcon.entity.v0.process.Validity;
import org.apache.falcon.resource.EntityList.EntityElement;
import org.apache.falcon.util.BuildProperties;
import org.apache.falcon.util.DeploymentProperties;
import org.apache.falcon.util.OozieTestUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.Job.Status;
import org.apache.oozie.client.ProxyOozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.servlet.ServletInputStream;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Pattern;

/**
 * Test class for Entity REST APIs.
 *
 * Tests should be enabled only in local environments as they need running instance of the web server.
 */
@Test(groups = {"exhaustive"})
public class EntityManagerJerseyIT {

    @BeforeClass
    public void prepare() throws Exception {
        TestContext.prepare();
    }

    static void assertLibs(FileSystem fs, Path path) throws IOException {
        FileStatus[] libs = fs.listStatus(path);
        Assert.assertNotNull(libs);
        Assert.assertEquals(libs.length, 1);
        Assert.assertTrue(libs[0].getPath().getName().startsWith("falcon-hadoop-dependencies"));
    }

    private Entity getDefinition(TestContext context, EntityType type, String name) throws Exception {
        ClientResponse response =
                context.service.path("api/entities/definition/" + type.name().toLowerCase() + "/" + name)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML)
                .get(ClientResponse.class);
        return (Entity) type.getUnmarshaller().unmarshal(new StringReader(response.getEntity(String.class)));
    }

    private void updateEndtime(Process process) {
        Validity processValidity = process.getClusters().getClusters().get(0).getValidity();
        processValidity.setEnd(new Date(new Date().getTime() + 2 * 24 * 60 * 60 * 1000));
    }

    @Test
    public void testLibExtensions() throws Exception {
        TestContext context = newContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        ClientResponse response = context.submitToFalcon(TestContext.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);
        FileSystem fs = context.getCluster().getFileSystem();
        assertLibs(fs, new Path("/project/falcon/working/libext/FEED/retention"));
        assertLibs(fs, new Path("/project/falcon/working/libext/PROCESS"));

        String tmpFileName = TestContext.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE1, overlay);
        Feed feed = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(new File(tmpFileName));
        Location location = new Location();
        location.setPath("fsext://global:00/falcon/test/input/${YEAR}/${MONTH}/${DAY}/${HOUR}");
        location.setType(LocationType.DATA);
        Cluster cluster = feed.getClusters().getClusters().get(0);
        cluster.setLocations(new Locations());
        feed.getClusters().getClusters().get(0).getLocations().getLocations().add(location);

        File tmpFile = TestContext.getTempFile();
        EntityType.FEED.getMarshaller().marshal(feed, tmpFile);
        response = context.submitAndSchedule(tmpFileName, overlay, EntityType.FEED);
        context.assertSuccessful(response);
    }

    private void update(TestContext context, Entity entity) throws Exception {
        update(context, entity, null);
    }

    private void update(TestContext context, Entity entity, Date endTime) throws Exception {
        File tmpFile = TestContext.getTempFile();
        entity.getEntityType().getMarshaller().marshal(entity, tmpFile);
        WebResource resource = context.service.path("api/entities/update/"
                + entity.getEntityType().name().toLowerCase() + "/" + entity.getName());
        if (endTime != null) {
            resource = resource.queryParam("effective", SchemaHelper.formatDateUTC(endTime));
        }
        ClientResponse response = resource
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML)
                .post(ClientResponse.class, context.getServletInputStream(tmpFile.getAbsolutePath()));
        context.assertSuccessful(response);
    }

    @Test
    public void testUpdateCheckUser() throws Exception {
        TestContext context = newContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        String tmpFileName = TestContext.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
        Process process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(new File(tmpFileName));
        updateEndtime(process);
        File tmpFile = TestContext.getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);
        context.scheduleProcess(tmpFile.getAbsolutePath(), overlay);
        OozieTestUtils.waitForBundleStart(context, Status.RUNNING);

        List<BundleJob> bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 1);
        Assert.assertEquals(bundles.get(0).getUser(), TestContext.REMOTE_USER);

        Feed feed = (Feed) getDefinition(context, EntityType.FEED, context.outputFeedName);

        //change output feed path and update feed as another user
        feed.getLocations().getLocations().get(0).setPath("/falcon/test/output2/${YEAR}/${MONTH}/${DAY}");
        update(context, feed);

        bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 2);
        Assert.assertEquals(bundles.get(0).getUser(), TestContext.REMOTE_USER);
        Assert.assertEquals(bundles.get(1).getUser(), TestContext.REMOTE_USER);
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

    public void testOptionalInput() throws Exception {
        TestContext context = newContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        String tmpFileName = TestContext.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
        Process process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(new File(tmpFileName));

        Input in1 = process.getInputs().getInputs().get(0);
        Input in2 = new Input();
        in2.setFeed(in1.getFeed());
        in2.setName("input2");
        in2.setOptional(true);
        in2.setPartition(in1.getPartition());
        in2.setStart("now(-1,0)");
        in2.setEnd("now(0,0)");
        process.getInputs().getInputs().add(in2);

        File tmpFile = TestContext.getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);
        context.scheduleProcess(tmpFile.getAbsolutePath(), overlay);
        OozieTestUtils.waitForWorkflowStart(context, context.processName);
    }

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
        OozieTestUtils.waitForBundleStart(context, Status.FAILED, Status.KILLED);

        FalconClient client = new FalconClient(TestContext.BASE_URL);
        EntityList deps = client.getDependency(EntityType.PROCESS.name(), process.getName());
        for (EntityElement dep : deps.getElements()) {
            if (dep.name.equals(process.getInputs().getInputs().get(0).getName())) {
                Assert.assertEquals("Input", dep.tag);
            } else if (dep.name.equals(process.getOutputs().getOutputs().get(0).getName())) {
                Assert.assertEquals("Output", dep.tag);
            }
        }

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
                .accept(MediaType.TEXT_XML)
                .type(MediaType.TEXT_XML)
                .post(ClientResponse.class, context.getServletInputStream(tmpFile.getAbsolutePath()));
        context.assertSuccessful(clientResponse);

        //Assert that new schedule creates new bundle
        List<BundleJob> bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 2);
    }

    @Test
    public void testUserWorkflowUpdate() throws Exception {
        //schedule a process
        TestContext context = newContext();
        context.scheduleProcess();
        OozieTestUtils.waitForBundleStart(context, Job.Status.RUNNING);
        List<BundleJob> bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 1);

        //create new file in user workflow
        FileSystem fs = context.cluster.getFileSystem();
        fs.create(new Path("/falcon/test/workflow", "newfile")).close();

        //update process should create new bundle
        Process process = (Process) getDefinition(context, EntityType.PROCESS, context.processName);
        updateEndtime(process);
        update(context, process);
        bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 2);
    }

    @Test
    public void testProcessInputUpdate() throws Exception {
        TestContext context = newContext();
        context.scheduleProcess();
        OozieTestUtils.waitForBundleStart(context, Job.Status.RUNNING);
        List<BundleJob> bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 1);
        ProxyOozieClient ozClient = OozieTestUtils.getOozieClient(context.getCluster().getCluster());
        String coordId = ozClient.getBundleJobInfo(bundles.get(0).getId()).getCoordinators().get(0).getId();

        Process process = (Process) getDefinition(context, EntityType.PROCESS, context.processName);

        String feed3 = "f3" + System.currentTimeMillis();
        Map<String, String> overlay = new HashMap<String, String>();
        overlay.put("inputFeedName", feed3);
        overlay.put("cluster", context.clusterName);
        overlay.put("user", System.getProperty("user.name"));
        ClientResponse response = context.submitToFalcon(TestContext.FEED_TEMPLATE1, overlay, EntityType.FEED);
        context.assertSuccessful(response);

        Input input = new Input();
        input.setFeed(feed3);
        input.setName("inputData2");
        input.setStart("today(20,0)");
        input.setEnd("today(20,20)");
        process.getInputs().getInputs().add(input);

        updateEndtime(process);
        Date endTime = getEndTime();
        update(context, process, endTime);

        //Assert that update creates new bundle and old coord is running
        bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 2);
        CoordinatorJob coord = ozClient.getCoordJobInfo(coordId);
        Assert.assertEquals(coord.getStatus(), Status.RUNNING);
        Assert.assertEquals(coord.getEndTime(), endTime);
    }

    public void testProcessEndtimeUpdate() throws Exception {
        TestContext context = newContext();
        context.scheduleProcess();
        OozieTestUtils.waitForBundleStart(context, Job.Status.RUNNING);

        Process process = (Process) getDefinition(context, EntityType.PROCESS, context.processName);

        updateEndtime(process);
        update(context, process);

        //Assert that update does not create new bundle
        List<BundleJob> bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 1);
    }

    public void testStatus() throws Exception {
        TestContext context = newContext();
        ClientResponse response;
        Map<String, String> overlay = context.getUniqueOverlay();

        response = context.submitToFalcon(TestContext.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.FEED_TEMPLATE1, overlay, EntityType.FEED);
        context.assertSuccessful(response);

        response = context.service
                .path("api/entities/status/feed/" + overlay.get("inputFeedName"))
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML)
                .get(ClientResponse.class);

        APIResult result = (APIResult) context.unmarshaller.
                unmarshal(new StringReader(response.getEntity(String.class)));
        Assert.assertTrue(result.getMessage().contains("SUBMITTED"));

    }

    public void testIdempotentSubmit() throws Exception {
        TestContext context = newContext();
        ClientResponse response;
        Map<String, String> overlay = context.getUniqueOverlay();

        response = context.submitToFalcon(TestContext.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);
    }

    public void testNotFoundStatus() {
        TestContext context = newContext();
        ClientResponse response;
        String feed1 = "f1" + System.currentTimeMillis();
        response = context.service
                .path("api/entities/status/feed/" + feed1)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_PLAIN)
                .get(ClientResponse.class);

        Assert.assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    }

    public void testVersion() {
        TestContext context = newContext();
        ClientResponse response;
        response = context.service
                .path("api/admin/version")
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.APPLICATION_JSON)
                .get(ClientResponse.class);
        String json = response.getEntity(String.class);
        String buildVersion = BuildProperties.get().getProperty("build.version");
        String deployMode = DeploymentProperties.get().getProperty("deploy.mode");
        Assert.assertTrue(Pattern.matches(
                ".*\\{\\s*\"key\"\\s*:\\s*\"Version\"\\s*,\\s*\"value\"\\s*:\\s*\""
                        + buildVersion + "\"\\s*}.*", json),
                "No build.version found in /api/admin/version");
        Assert.assertTrue(Pattern.matches(
                ".*\\{\\s*\"key\"\\s*:\\s*\"Mode\"\\s*,\\s*\"value\"\\s*:\\s*\""
                        + deployMode + "\"\\s*}.*", json),
                "No deploy.mode found in /api/admin/version");
    }

    public void testValidate() {
        TestContext context = newContext();
        ServletInputStream stream = context.getServletInputStream(getClass().
                getResourceAsStream(TestContext.SAMPLE_PROCESS_XML));

        ClientResponse clientResponse = context.service
                .path("api/entities/validate/process")
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class, stream);

        context.assertFailure(clientResponse);
    }

    public void testClusterValidate() throws Exception {
        TestContext context = newContext();
        ClientResponse clientResponse;
        Map<String, String> overlay = context.getUniqueOverlay();

        InputStream stream = context.getServletInputStream(
                TestContext.overlayParametersOverTemplate(TestContext.CLUSTER_TEMPLATE, overlay));

        clientResponse = context.service.path("api/entities/validate/cluster")
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class, stream);
        context.assertSuccessful(clientResponse);
    }

    public void testClusterSubmitScheduleSuspendResumeDelete() throws Exception {
        TestContext context = newContext();
        ClientResponse clientResponse;
        Map<String, String> overlay = context.getUniqueOverlay();

        clientResponse = context.submitToFalcon(TestContext.CLUSTER_TEMPLATE, overlay,
                EntityType.CLUSTER);
        context.assertSuccessful(clientResponse);

        clientResponse = context.service
                .path("api/entities/schedule/cluster/" + context.clusterName)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class);
        context.assertFailure(clientResponse);

        clientResponse = context.service
                .path("api/entities/suspend/cluster/" + context.clusterName)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class);
        context.assertFailure(clientResponse);

        clientResponse = context.service
                .path("api/entities/resume/cluster/" + context.clusterName)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class);
        context.assertFailure(clientResponse);

        clientResponse = context.service
                .path("api/entities/delete/cluster/" + context.clusterName)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML)
                .delete(ClientResponse.class);
        context.assertSuccessful(clientResponse);
    }

    public void testSubmit() throws Exception {
        TestContext context = newContext();
        ClientResponse response;
        Map<String, String> overlay = context.getUniqueOverlay();

        response = context.submitToFalcon(TestContext.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.FEED_TEMPLATE1, overlay, EntityType.FEED);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.FEED_TEMPLATE2, overlay, EntityType.FEED);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.PROCESS_TEMPLATE, overlay, EntityType.PROCESS);
        context.assertSuccessful(response);
    }

    public void testGetEntityDefinition() throws Exception {
        TestContext context = newContext();
        ClientResponse response;
        Map<String, String> overlay = context.getUniqueOverlay();

        response = context.submitToFalcon(TestContext.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.FEED_TEMPLATE1, overlay, EntityType.FEED);
        context.assertSuccessful(response);

        response = context.service
                .path("api/entities/definition/feed/" + overlay.get("inputFeedName"))
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML)
                .get(ClientResponse.class);

        String feedXML = response.getEntity(String.class);
        try {
            Feed result = (Feed) context.unmarshaller.
                    unmarshal(new StringReader(feedXML));
            Assert.assertEquals(result.getName(), overlay.get("inputFeedName"));
        } catch (JAXBException e) {
            Assert.fail("Reponse " + feedXML + " is not valid", e);
        }
    }

    public void testInvalidGetEntityDefinition() {
        TestContext context = newContext();
        ClientResponse clientResponse = context.service
                .path("api/entities/definition/process/sample1")
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML)
                .get(ClientResponse.class);
        context.assertFailure(clientResponse);
    }

    public void testScheduleSuspendResume() throws Exception {
        TestContext context = newContext();
        context.scheduleProcess();

        ClientResponse clientResponse = context.service
                .path("api/entities/suspend/process/" + context.processName)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML)
                .post(ClientResponse.class);
        context.assertSuccessful(clientResponse);

        clientResponse = context.service
                .path("api/entities/resume/process/" + context.processName)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML)
                .post(ClientResponse.class);
        context.assertSuccessful(clientResponse);
    }

    public void testFeedSchedule() throws Exception {
        TestContext context = newContext();
        ClientResponse response;
        Map<String, String> overlay = context.getUniqueOverlay();

        response = context.submitToFalcon(TestContext.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.FEED_TEMPLATE1, overlay, EntityType.FEED);
        context.assertSuccessful(response);

        createTestData(context);
        ClientResponse clientResponse = context.service
                .path("api/entities/schedule/feed/" + overlay.get("inputFeedName"))
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class);
        context.assertSuccessful(clientResponse);
    }

    static List<Path> createTestData(TestContext context) throws Exception {
        List<Path> list = new ArrayList<Path>();
        FileSystem fs = context.cluster.getFileSystem();
        fs.mkdirs(new Path("/user/guest"));
        fs.setOwner(new Path("/user/guest"), TestContext.REMOTE_USER, "users");

        DateFormat formatter = new SimpleDateFormat("yyyy/MM/dd/HH/mm");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date date = new Date(System.currentTimeMillis() + 3 * 3600000);
        Path path = new Path("/examples/input-data/rawLogs/" + formatter.format(date) + "/file");
        fs.create(path).close();
        date = new Date(date.getTime() - 3600000);
        path = new Path("/examples/input-data/rawLogs/" + formatter.format(date) + "/file");
        fs.create(path).close();
        date = new Date(date.getTime() - 3600000);
        path = new Path("/examples/input-data/rawLogs/" + formatter.format(date) + "/file");
        fs.create(path).close();
        date = new Date(date.getTime() - 3600000);
        path = new Path("/examples/input-data/rawLogs/" + formatter.format(date) + "/file");
        list.add(path);
        fs.create(path).close();
        date = new Date(date.getTime() - 3600000);
        path = new Path("/examples/input-data/rawLogs/" + formatter.format(date) + "/file");
        list.add(path);
        fs.create(path).close();
        date = new Date(date.getTime() - 3600000);
        path = new Path("/examples/input-data/rawLogs/" + formatter.format(date) + "/file");
        list.add(path);
        fs.create(path).close();
        date = new Date(date.getTime() - 3600000);
        path = new Path("/examples/input-data/rawLogs/" + formatter.format(date) + "/file");
        list.add(path);
        fs.create(path).close();
        date = new Date(date.getTime() - 3600000);
        path = new Path("/examples/input-data/rawLogs/" + formatter.format(date) + "/file");
        list.add(path);
        fs.create(path).close();
        date = new Date(date.getTime() - 3600000);
        path = new Path("/examples/input-data/rawLogs/" + formatter.format(date) + "/file");
        list.add(path);
        fs.create(path).close();
        date = new Date(date.getTime() - 3600000);
        path = new Path("/examples/input-data/rawLogs/" + formatter.format(date) + "/file");
        list.add(path);
        fs.create(path).close();
        new FsShell(context.cluster.getConf()).
                run(new String[]{"-chown", "-R", "guest:users", "/examples/input-data/rawLogs"});
        return list;
    }

    public void testDeleteDataSet() throws Exception {
        TestContext context = newContext();
        ClientResponse response;
        Map<String, String> overlay = context.getUniqueOverlay();

        response = context.submitToFalcon(TestContext.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.FEED_TEMPLATE1, overlay, EntityType.FEED);
        context.assertSuccessful(response);

        response = context.service
                .path("api/entities/delete/feed/" + overlay.get("inputFeedName"))
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML)
                .delete(ClientResponse.class);
        context.assertSuccessful(response);
    }

    public void testDelete() throws Exception {
        TestContext context = newContext();
        ClientResponse response;
        Map<String, String> overlay = context.getUniqueOverlay();

        response = context.submitToFalcon(TestContext.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.FEED_TEMPLATE1, overlay, EntityType.FEED);
        context.assertSuccessful(response);

        response = context.service
                .path("api/entities/delete/cluster/" + context.clusterName)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML)
                .delete(ClientResponse.class);
        context.assertFailure(response);

        response = context.submitToFalcon(TestContext.FEED_TEMPLATE2, overlay, EntityType.FEED);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.PROCESS_TEMPLATE, overlay, EntityType.PROCESS);
        context.assertSuccessful(response);

        //Delete a referred feed
        response = context.service
                .path("api/entities/delete/feed/" + overlay.get("inputFeedName"))
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML)
                .delete(ClientResponse.class);
        context.assertFailure(response);

        //Delete a submitted process
        response = context.service
                .path("api/entities/delete/process/" + context.processName)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML)
                .delete(ClientResponse.class);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.PROCESS_TEMPLATE, overlay, EntityType.PROCESS);
        context.assertSuccessful(response);

        ClientResponse clientResponse = context.service
                .path("api/entities/schedule/process/" + context.processName)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class);
        context.assertSuccessful(clientResponse);

        //Delete a scheduled process
        response = context.service
                .path("api/entities/delete/process/" + context.processName)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML)
                .delete(ClientResponse.class);
        context.assertSuccessful(response);
    }

    @Test
    public void testGetEntityList() throws Exception {
        TestContext context = newContext();
        ClientResponse response;
        response = context.service
                .path("api/entities/list/process/")
                .header("Cookie", context.getAuthenticationToken())
                .type(MediaType.TEXT_XML)
                .accept(MediaType.TEXT_XML)
                .get(ClientResponse.class);
        Assert.assertEquals(response.getStatus(), 200);

        EntityList result = response.getEntity(EntityList.class);
        Assert.assertNotNull(result);
        for (EntityList.EntityElement entityElement : result.getElements()) {
            Assert.assertNull(entityElement.status); // status is null
        }

        response = context.service
                .path("api/entities/list/process/")
                .queryParam("orderBy", "name").queryParam("offset", "2")
                .queryParam("numResults", "2").queryParam("fields", "status")
                .header("Cookie", context.getAuthenticationToken())
                .type(MediaType.TEXT_XML)
                .accept(MediaType.TEXT_XML)
                .get(ClientResponse.class);
        Assert.assertEquals(response.getStatus(), 200);
        result = response.getEntity(EntityList.class);
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getElements().length, 2);

        response = context.service
                .path("api/entities/list/process/")
                .queryParam("orderBy", "name").queryParam("offset", "50").queryParam("numResults", "2")
                .header("Cookie", context.getAuthenticationToken())
                .type(MediaType.TEXT_XML)
                .accept(MediaType.TEXT_XML)
                .get(ClientResponse.class);
        Assert.assertEquals(response.getStatus(), 200);
        result = response.getEntity(EntityList.class);
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getElements(), null);

        Map<String, String> overlay = context.getUniqueOverlay();
        overlay.put("cluster", "WTF-" + overlay.get("cluster"));
        response = context.submitToFalcon(TestContext.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);

        response = context.service
                .path("api/entities/list/cluster/")
                .header("Cookie", context.getAuthenticationToken())
                .type(MediaType.TEXT_XML)
                .accept(MediaType.TEXT_XML)
                .get(ClientResponse.class);
        Assert.assertEquals(response.getStatus(), 200);
        result = response.getEntity(EntityList.class);
        Assert.assertNotNull(result);
        for (EntityList.EntityElement entityElement : result.getElements()) {
            Assert.assertNull(entityElement.status); // status is null
        }

        response = context.service
                .path("api/entities/list/cluster/")
                .queryParam("fields", "status")
                .header("Cookie", context.getAuthenticationToken())
                .type(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .get(ClientResponse.class);
        Assert.assertEquals(response.getStatus(), 200);
        result = response.getEntity(EntityList.class);
        Assert.assertNotNull(result);
        for (EntityList.EntityElement entityElement : result.getElements()) {
            Assert.assertNotNull(entityElement.status); // status is null
        }
    }

    public Date getEndTime() {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.add(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }
}
