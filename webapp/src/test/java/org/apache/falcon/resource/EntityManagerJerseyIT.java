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
import org.apache.oozie.client.OozieClient;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
        assertLibs(fs, new Path("/projects/falcon/working/libext/FEED/retention"));
        assertLibs(fs, new Path("/projects/falcon/working/libext/PROCESS"));

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

    private ClientResponse update(TestContext context, Entity entity,
                                  Date endTime, Boolean skipDryRun) throws Exception {
        File tmpFile = TestContext.getTempFile();
        entity.getEntityType().getMarshaller().marshal(entity, tmpFile);
        WebResource resource = context.service.path("api/entities/update/"
                + entity.getEntityType().name().toLowerCase() + "/" + entity.getName());
        if (endTime != null) {
            resource = resource.queryParam("effective", SchemaHelper.formatDateUTC(endTime));
        }
        if (null != skipDryRun) {
            resource = resource.queryParam("skipDryRun", String.valueOf(skipDryRun));
        }
        return resource.header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML)
                .post(ClientResponse.class, context.getServletInputStream(tmpFile.getAbsolutePath()));
    }

    private ClientResponse touch(TestContext context, Entity entity, Boolean skipDryRun) {
        WebResource resource = context.service.path("api/entities/touch/"
                + entity.getEntityType().name().toLowerCase() + "/" + entity.getName());
        if (null != skipDryRun) {
            resource = resource.queryParam("skipDryRun", String.valueOf(skipDryRun));
        }
        return resource
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class);
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
        ClientResponse response = update(context, feed, null, false);
        context.assertSuccessful(response);

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
    }

    public void testDryRun() throws Exception {
        //Schedule of invalid process should fail because of dryRun, and should pass when dryrun is skipped
        TestContext context = newContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        String tmpFileName = TestContext.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
        Process process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(new File(tmpFileName));
        Property prop = new Property();
        prop.setName("newProp");
        prop.setValue("${instanceTim()}");  //invalid property
        process.getProperties().getProperties().add(prop);
        File tmpFile = TestContext.getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);

        ClientResponse response = context.validate(tmpFile.getAbsolutePath(), overlay, EntityType.PROCESS);
        context.assertFailure(response);

        context.scheduleProcess(tmpFile.getAbsolutePath(), overlay, false, null);

        //Fix the process and then submitAndSchedule should succeed
        Iterator<Property> itr = process.getProperties().getProperties().iterator();
        while (itr.hasNext()) {
            Property myProp = itr.next();
            if (myProp.getName().equals("newProp")) {
                itr.remove();
            }
        }
        tmpFile = TestContext.getTempFile();
        process.setName("process" + System.currentTimeMillis());
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);
        response = context.submitAndSchedule(tmpFile.getAbsolutePath(), overlay, EntityType.PROCESS);
        context.assertSuccessful(response);

        //Update with invalid property should fail again
        process.getProperties().getProperties().add(prop);
        updateEndtime(process);
        response = update(context, process, null, null);
        context.assertFailure(response);

        // update where dryrun is disabled should succeed.
        response = update(context, process, null, true);
        context.assertSuccessful(response);

    }

    @Test
    public void testUpdateSuspendedEntity() throws Exception {
        TestContext context = newContext();
        context.scheduleProcess();
        OozieTestUtils.waitForBundleStart(context, Job.Status.RUNNING);

        //Suspend entity
        Process process = (Process) getDefinition(context, EntityType.PROCESS, context.processName);
        ClientResponse response = suspend(context, process);
        context.assertSuccessful(response);

        process.getProperties().getProperties().get(0).setName("newprop");
        Date endTime = getEndTime();
        process.getClusters().getClusters().get(0).getValidity().setEnd(endTime);
        response = update(context, process, endTime, null);
        context.assertSuccessful(response);

        //Since the process endtime = update effective time, it shouldn't create new bundle
        List<BundleJob> bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 1);

        //Since the entity was suspended before update, it should still be suspended
        Assert.assertEquals(bundles.get(0).getStatus(), Status.SUSPENDED);
    }

    @Test
    public void testProcessInputUpdate() throws Exception {
        TestContext context = newContext();
        context.scheduleProcess();
        OozieTestUtils.waitForBundleStart(context, Job.Status.RUNNING);
        List<BundleJob> bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 1);
        OozieClient ozClient = OozieTestUtils.getOozieClient(context.getCluster().getCluster());
        String bundle = bundles.get(0).getId();
        String coordId = ozClient.getBundleJobInfo(bundle).getCoordinators().get(0).getId();

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
        response = update(context, process, endTime, null);
        context.assertSuccessful(response);

        //Assert that update creates new bundle and old coord is running
        bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 2);
        CoordinatorJob coord = ozClient.getCoordJobInfo(coordId);
        Assert.assertEquals(coord.getStatus(), Status.RUNNING);
        Assert.assertEquals(coord.getEndTime(), endTime);

        //Assert on new bundle/coord
        String newBundle = null;
        for (BundleJob myBundle : bundles) {
            if (!myBundle.getId().equals(bundle)) {
                newBundle = myBundle.getId();
                break;
            }
        }

        assert newBundle != null;
        OozieTestUtils.waitForBundleStart(context, newBundle, Job.Status.RUNNING, Status.PREP);
        coord = ozClient.getCoordJobInfo(ozClient.getBundleJobInfo(newBundle).getCoordinators().get(0).getId());
        Assert.assertTrue(coord.getStatus() == Status.RUNNING || coord.getStatus() == Status.PREP);
        Assert.assertEquals(coord.getStartTime(), endTime);
    }

    public void testProcessEndtimeUpdate() throws Exception {
        TestContext context = newContext();
        context.scheduleProcess();
        OozieTestUtils.waitForBundleStart(context, Job.Status.RUNNING);

        Process process = (Process) getDefinition(context, EntityType.PROCESS, context.processName);

        updateEndtime(process);
        ClientResponse response = update(context, process, null, null);
        context.assertSuccessful(response);

        //Assert that update does not create new bundle
        List<BundleJob> bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 1);
    }

    @Test
    public void testTouchEntity() throws Exception {
        TestContext context = newContext();
        context.scheduleProcess();
        OozieTestUtils.waitForBundleStart(context, Job.Status.RUNNING);
        List<BundleJob> bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 1);
        OozieClient ozClient = OozieTestUtils.getOozieClient(context.getCluster().getCluster());
        String bundle = bundles.get(0).getId();
        String coordId = ozClient.getBundleJobInfo(bundle).getCoordinators().get(0).getId();

        //Update end time of process required for touch
        Process process = (Process) getDefinition(context, EntityType.PROCESS, context.processName);
        updateEndtime(process);
        ClientResponse response = update(context, process, null, null);
        context.assertSuccessful(response);
        bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 1);

        //Calling force update
        response = touch(context, process, true);
        context.assertSuccessful(response);
        OozieTestUtils.waitForBundleStart(context, Status.PREP, Status.RUNNING);

        //Assert that touch creates new bundle and old coord is running
        bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 2);
        CoordinatorJob coord = ozClient.getCoordJobInfo(coordId);
        Assert.assertTrue(coord.getStatus() == Status.RUNNING || coord.getStatus() == Status.SUCCEEDED);

        //Assert on new bundle/coord
        String newBundle = null;
        for (BundleJob myBundle : bundles) {
            if (!myBundle.getId().equals(bundle)) {
                newBundle = myBundle.getId();
                break;
            }
        }

        assert newBundle != null;
        coord = ozClient.getCoordJobInfo(ozClient.getBundleJobInfo(newBundle).getCoordinators().get(0).getId());
        Assert.assertTrue(coord.getStatus() == Status.RUNNING || coord.getStatus() == Status.PREP);
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

    ClientResponse suspend(TestContext context, Entity entity) {
        return suspend(context, entity.getEntityType(), entity.getName());
    }

    private ClientResponse suspend(TestContext context, EntityType entityType, String name) {
        return context.service
            .path("api/entities/suspend/" + entityType.name().toLowerCase() + "/" + name)
            .header("Cookie", context.getAuthenticationToken())
            .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
            .post(ClientResponse.class);
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

        clientResponse = suspend(context, EntityType.CLUSTER, context.clusterName);
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

    @Test
    public void testDuplicateSubmitCommands() throws Exception {
        TestContext context = newContext();
        Map<String, String> overlay = context.getUniqueOverlay();

        ExecutorService service = Executors.newSingleThreadExecutor();
        ExecutorService duplicateService = Executors.newSingleThreadExecutor();

        Future<ClientResponse> future = service.submit(new SubmitCommand(context, overlay));
        Future<ClientResponse> duplicateFuture = duplicateService.submit(new SubmitCommand(context, overlay));

        ClientResponse response = future.get();
        ClientResponse duplicateSubmitThreadResponse = duplicateFuture.get();

        // since there are duplicate threads for submits, there is no guarantee which request will succeed.
        testDuplicateCommandsResponse(context, response, duplicateSubmitThreadResponse);
    }

    @Test
    public void testDuplicateDeleteCommands() throws Exception {
        TestContext context = newContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        context.submitToFalcon(TestContext.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);

        ExecutorService service = Executors.newSingleThreadExecutor();
        ExecutorService duplicateService = Executors.newSingleThreadExecutor();

        Future<ClientResponse> future = service.submit(new DeleteCommand(context, overlay.get("cluster"), "cluster"));
        Future<ClientResponse> duplicateFuture = duplicateService.submit(new DeleteCommand(context,
                overlay.get("cluster"), "cluster"));

        ClientResponse response = future.get();
        ClientResponse duplicateSubmitThreadResponse = duplicateFuture.get();

        // since there are duplicate threads for deletion, there is no guarantee which request will succeed.
        testDuplicateCommandsResponse(context, response, duplicateSubmitThreadResponse);
    }

    private void testDuplicateCommandsResponse(TestContext context, ClientResponse response,
                                               ClientResponse duplicateSubmitThreadResponse) {
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            context.assertSuccessful(response);
            context.assertFailure(duplicateSubmitThreadResponse);
        } else {
            context.assertFailure(response);
            context.assertSuccessful(duplicateSubmitThreadResponse);
        }
    }

    public void testProcesssScheduleAndDelete() throws Exception {
        TestContext context = newContext();
        ClientResponse clientResponse;
        Map<String, String> overlay = context.getUniqueOverlay();
        String tmpFileName = TestContext.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
        Process process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(new File(tmpFileName));
        updateEndtime(process);
        File tmpFile = TestContext.getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);
        context.scheduleProcess(tmpFile.getAbsolutePath(), overlay);
        OozieTestUtils.waitForBundleStart(context, Status.RUNNING);

        //Delete a scheduled process
        clientResponse = context.service
                .path("api/entities/delete/process/" + context.processName)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.TEXT_XML)
                .delete(ClientResponse.class);
        context.assertSuccessful(clientResponse);
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

        ClientResponse clientResponse = suspend(context, EntityType.PROCESS, context.processName);
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
                .path("api/entities/list/feed,process/")
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
                .path("api/entities/list/")
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
    }

    @Test
    public void testDuplicateUpdateCommands() throws Exception {
        TestContext context = newContext();
        context.scheduleProcess();
        OozieTestUtils.waitForBundleStart(context, Job.Status.RUNNING);
        List<BundleJob> bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 1);

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
        ExecutorService service =  Executors.newSingleThreadExecutor();
        Future<ClientResponse> future = service.submit(new UpdateCommand(context, process, endTime));
        response = update(context, process, endTime, false);
        ClientResponse duplicateUpdateThreadResponse = future.get();

        // since there are duplicate threads for updates, there is no guarantee which request will succeed
        testDuplicateCommandsResponse(context, response, duplicateUpdateThreadResponse);

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

    class UpdateCommand implements Callable<ClientResponse> {
        private TestContext context;
        private Process process;
        private Date endTime;

        public TestContext getContext() {
            return context;
        }
        public Process getProcess() {
            return process;
        }
        public Date getEndTime() {
            return endTime;
        }

        public UpdateCommand(TestContext context, Process process, Date endTime) {
            this.context = context;
            this.process = process;
            this.endTime = endTime;
        }

        @Override
        public ClientResponse call() throws Exception {
            return update(context, process, endTime, false);
        }
    }

    class SubmitCommand implements Callable<ClientResponse> {
        private Map<String, String> overlay;
        private TestContext context;

        public TestContext getContext() {
            return context;
        }

        public Map<String, String> getOverlay() {
            return overlay;
        }

        public SubmitCommand(TestContext context, Map<String, String> overlay) {
            this.context = context;
            this.overlay = overlay;
        }

        @Override
        public ClientResponse call() throws Exception {
            return context.submitToFalcon(TestContext.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        }
    }

    class DeleteCommand implements Callable<ClientResponse> {
        private TestContext context;
        private String entityName;
        private String entityType;

        public TestContext getContext() {
            return context;
        }

        public DeleteCommand(TestContext context, String entityName, String entityType) {
            this.context = context;
            this.entityName = entityName;
            this.entityType = entityType;
        }

        @Override
        public ClientResponse call() throws Exception {
            return context.deleteFromFalcon(entityName, entityType);
        }
    }

}
