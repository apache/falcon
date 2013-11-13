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

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Pattern;

import javax.servlet.ServletInputStream;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBException;

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.*;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Property;
import org.apache.falcon.entity.v0.process.Validity;
import org.apache.falcon.util.BuildProperties;
import org.apache.falcon.util.DeploymentProperties;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.Job.Status;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.sun.jersey.api.client.ClientResponse;

/**
 * Test class for Entity REST APIs.
 *
 * Tests should be enabled only in local environments as they need running instance of the web server.
 */
public class EntityManagerJerseyIT {

    @BeforeClass
    public void prepare() throws Exception {
        TestContext.prepare();
    }

    private void assertLibs(FileSystem fs, Path path) throws IOException {
        FileStatus[] libs = fs.listStatus(path);
        Assert.assertNotNull(libs);
        Assert.assertEquals(libs.length, 1);
        Assert.assertTrue(libs[0].getPath().getName().startsWith("falcon-hadoop-dependencies"));
    }

    @Test
    public void testLibExtensions() throws Exception {
        TestContext context = newContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        ClientResponse response = context.submitToFalcon(context.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);
        FileSystem fs = context.getCluster().getFileSystem();
        assertLibs(fs, new Path("/project/falcon/working/libext/FEED/retention"));
        assertLibs(fs, new Path("/project/falcon/working/libext/PROCESS"));

        String tmpFileName = context.overlayParametersOverTemplate(TestContext.FEED_TEMPLATE1, overlay);
        Feed feed = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(new File(tmpFileName));
        Location location = new Location();
        location.setPath("fsext://localhost:41020/falcon/test/input/${YEAR}/${MONTH}/${DAY}/${HOUR}");
        location.setType(LocationType.DATA);
        Cluster cluster = feed.getClusters().getClusters().get(0);
        cluster.setLocations(new Locations());
        feed.getClusters().getClusters().get(0).getLocations().getLocations().add(location);

        File tmpFile = context.getTempFile();
        EntityType.FEED.getMarshaller().marshal(feed, tmpFile);
        response = context.submitAndSchedule(tmpFileName, overlay, EntityType.FEED);
        context.assertSuccessful(response);
    }

    @Test
    public void testUpdateCheckUser() throws Exception {
        TestContext context = newContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        String tmpFileName = context.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
        Process process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(new File(tmpFileName));
        Validity processValidity = process.getClusters().getClusters().get(0).getValidity();
        processValidity.setEnd(new Date(new Date().getTime() + 2 * 24 * 60 * 60 * 1000));
        File tmpFile = context.getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);
        context.scheduleProcess(tmpFile.getAbsolutePath(), overlay);
        context.waitForBundleStart(Status.RUNNING);

        List<BundleJob> bundles = context.getBundles();
        Assert.assertEquals(bundles.size(), 1);
        Assert.assertEquals(bundles.get(0).getUser(), TestContext.REMOTE_USER);

        ClientResponse response = context.service.path("api/entities/definition/feed/"
                + context.outputFeedName).header(
                "Remote-User", TestContext.REMOTE_USER)
                .accept(MediaType.TEXT_XML).get(ClientResponse.class);
        Feed feed = (Feed) EntityType.FEED.getUnmarshaller()
                .unmarshal(new StringReader(response.getEntity(String.class)));

        //change output feed path and update feed as another user
        feed.getLocations().getLocations().get(0).setPath("/falcon/test/output2/${YEAR}/${MONTH}/${DAY}");
        tmpFile = context.getTempFile();
        EntityType.FEED.getMarshaller().marshal(feed, tmpFile);
        response = context.service.path("api/entities/update/feed/"
                + context.outputFeedName).header("Remote-User",
                TestContext.REMOTE_USER).accept(MediaType.TEXT_XML)
                .post(ClientResponse.class, context.getServletInputStream(tmpFile.getAbsolutePath()));
        context.assertSuccessful(response);

        bundles = context.getBundles();
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
            testContext.killOozieJobs();
        }
        contexts.remove();
    }

    @Test(enabled = false)
    public void testOptionalInput() throws Exception {
        TestContext context = newContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        String tmpFileName = context.
                overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
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

        File tmpFile = context.getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);
        context.scheduleProcess(tmpFile.getAbsolutePath(), overlay);
        context.waitForWorkflowStart(context.processName);
    }

    @Test
    public void testProcessDeleteAndSchedule() throws Exception {
        //Submit process with invalid property so that coord submit fails and bundle goes to failed state
        TestContext context = newContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        String tmpFileName = context.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, overlay);
        Process process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(new File(tmpFileName));
        Property prop = new Property();
        prop.setName("newProp");
        prop.setValue("${formatTim()}");
        process.getProperties().getProperties().add(prop);
        File tmpFile = context.getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);
        context.scheduleProcess(tmpFile.getAbsolutePath(), overlay);
        context.waitForBundleStart(Status.FAILED);

        //Delete and re-submit the process with correct workflow
        ClientResponse clientRepsonse = context.service.path("api/entities/delete/process/"
                + context.processName).header(
                "Remote-User", TestContext.REMOTE_USER)
                .accept(MediaType.TEXT_XML).delete(ClientResponse.class);
        context.assertSuccessful(clientRepsonse);
        process.getWorkflow().setPath("/falcon/test/workflow");
        tmpFile = context.getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);
        clientRepsonse = context.service.path("api/entities/submitAndSchedule/process").
                header("Remote-User", TestContext.REMOTE_USER)
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class, context.getServletInputStream(tmpFile.getAbsolutePath()));
        context.assertSuccessful(clientRepsonse);

        //Assert that new schedule creates new bundle
        List<BundleJob> bundles = context.getBundles();
        Assert.assertEquals(bundles.size(), 2);
    }

    @Test
    public void testProcessInputUpdate() throws Exception {
        TestContext context = newContext();
        context.scheduleProcess();
        context.waitForBundleStart(Job.Status.RUNNING);
        List<BundleJob> bundles = context.getBundles();
        Assert.assertEquals(bundles.size(), 1);
        OozieClient ozClient = context.getOozieClient();
        String coordId = ozClient.getBundleJobInfo(bundles.get(0).getId()).getCoordinators().get(0).getId();

        ClientResponse response = context.service.path("api/entities/definition/process/"
                + context.processName).header(
                "Remote-User", TestContext.REMOTE_USER)
                .accept(MediaType.TEXT_XML).get(ClientResponse.class);
        Process process = (Process) EntityType.PROCESS.getUnmarshaller()
                .unmarshal(new StringReader(response.getEntity(String.class)));

        String feed3 = "f3" + System.currentTimeMillis();
        Map<String, String> overlay = new HashMap<String, String>();
        overlay.put("inputFeedName", feed3);
        overlay.put("cluster", context.clusterName);
        response = context.submitToFalcon(TestContext.FEED_TEMPLATE1, overlay, EntityType.FEED);
        context.assertSuccessful(response);

        Input input = new Input();
        input.setFeed(feed3);
        input.setName("inputData2");
        input.setStart("today(20,0)");
        input.setEnd("today(20,20)");
        process.getInputs().getInputs().add(input);

        Validity processValidity = process.getClusters().getClusters().get(0).getValidity();
        processValidity.setEnd(new Date(new Date().getTime() + 2 * 24 * 60 * 60 * 1000));
        File tmpFile = context.getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);
        response = context.service.path("api/entities/update/process/"
                + context.processName).header("Remote-User",
                TestContext.REMOTE_USER).accept(MediaType.TEXT_XML)
                .post(ClientResponse.class, context.getServletInputStream(tmpFile.getAbsolutePath()));
        context.assertSuccessful(response);

        //Assert that update creates new bundle and old coord is running
        bundles = context.getBundles();
        Assert.assertEquals(bundles.size(), 2);
        Assert.assertEquals(ozClient.getCoordJobInfo(coordId).getStatus(), Status.RUNNING);
    }

    @Test
    public void testProcessEndtimeUpdate() throws Exception {
        TestContext context = newContext();
        context.scheduleProcess();
        context.waitForBundleStart(Job.Status.RUNNING);

        ClientResponse response = context.service.path("api/entities/definition/process/"
                + context.processName).header(
                "Remote-User", TestContext.REMOTE_USER)
                .accept(MediaType.TEXT_XML).get(ClientResponse.class);
        Process process = (Process) EntityType.PROCESS.getUnmarshaller()
                .unmarshal(new StringReader(response.getEntity(String.class)));

        Validity processValidity = process.getClusters().getClusters().get(0).getValidity();
        processValidity.setEnd(new Date(new Date().getTime() + 60 * 60 * 1000));
        File tmpFile = context.getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);
        response = context.service.path("api/entities/update/process/" + context.processName).header("Remote-User",
                TestContext.REMOTE_USER).accept(MediaType.TEXT_XML)
                .post(ClientResponse.class, context.getServletInputStream(tmpFile.getAbsolutePath()));
        context.assertSuccessful(response);

        //Assert that update does not create new bundle
        List<BundleJob> bundles = context.getBundles();
        Assert.assertEquals(bundles.size(), 1);
    }

    @Test
    public void testStatus() throws Exception {
        TestContext context = newContext();
        ClientResponse response;
        Map<String, String> overlay = context.getUniqueOverlay();

        response = context.submitToFalcon(context.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.FEED_TEMPLATE1, overlay, EntityType.FEED);
        context.assertSuccessful(response);

        response = context.service
                .path("api/entities/status/feed/" + overlay.get("inputFeedName"))
                .header("Remote-User", TestContext.REMOTE_USER)
                .accept(MediaType.TEXT_XML).get(ClientResponse.class);

        APIResult result = (APIResult) context.unmarshaller.
                unmarshal(new StringReader(response.getEntity(String.class)));
        Assert.assertTrue(result.getMessage().contains("SUBMITTED"));

    }

    @Test
    public void testIdempotentSubmit() throws Exception {
        TestContext context = newContext();
        ClientResponse response;
        Map<String, String> overlay = context.getUniqueOverlay();

        response = context.submitToFalcon(context.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);

        response = context.submitToFalcon(context.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);
    }

    @Test
    public void testNotFoundStatus() {
        TestContext context = newContext();
        ClientResponse response;
        String feed1 = "f1" + System.currentTimeMillis();
        response = context.service
                .path("api/entities/status/feed/" + feed1)
                .header("Remote-User", TestContext.REMOTE_USER)
                .accept(MediaType.TEXT_PLAIN).get(ClientResponse.class);
        String status = response.getEntity(String.class);
        Assert.assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    }

    @Test
    public void testVersion() {
        TestContext context = newContext();
        ClientResponse response;
        response = context.service
                .path("api/admin/version")
                .header("Remote-User", TestContext.REMOTE_USER)
                .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
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

    @Test
    public void testValidate() {
        TestContext context = newContext();
        ServletInputStream stream = context.getServletInputStream(getClass().
                getResourceAsStream(TestContext.SAMPLE_PROCESS_XML));

        ClientResponse clientRepsonse = context.service
                .path("api/entities/validate/process")
                .header("Remote-User", TestContext.REMOTE_USER)
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class, stream);

        context.assertFailure(clientRepsonse);
    }

    @Test
    public void testClusterValidate() throws Exception {
        TestContext context = newContext();
        ClientResponse clientRepsonse;
        Map<String, String> overlay = context.getUniqueOverlay();

        InputStream stream = context.getServletInputStream(
                context.overlayParametersOverTemplate(context.CLUSTER_TEMPLATE, overlay));

        clientRepsonse = context.service.path("api/entities/validate/cluster")
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .header("Remote-User", TestContext.REMOTE_USER)
                .post(ClientResponse.class, stream);
        context.assertSuccessful(clientRepsonse);
    }

    @Test
    public void testClusterSubmitScheduleSuspendResumeDelete() throws Exception {
        TestContext context = newContext();
        ClientResponse clientRepsonse;
        Map<String, String> overlay = context.getUniqueOverlay();

        clientRepsonse = context.submitToFalcon(context.CLUSTER_TEMPLATE, overlay,
                EntityType.CLUSTER);
        context.assertSuccessful(clientRepsonse);

        clientRepsonse = context.service
                .path("api/entities/schedule/cluster/" + context.clusterName)
                .header("Remote-User", TestContext.REMOTE_USER)
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class);
        context.assertFailure(clientRepsonse);

        clientRepsonse = context.service
                .path("api/entities/suspend/cluster/" + context.clusterName)
                .header("Remote-User", TestContext.REMOTE_USER)
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class);
        context.assertFailure(clientRepsonse);

        clientRepsonse = context.service
                .path("api/entities/resume/cluster/" + context.clusterName)
                .header("Remote-User", TestContext.REMOTE_USER)
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class);
        context.assertFailure(clientRepsonse);

        clientRepsonse = context.service
                .path("api/entities/delete/cluster/" + context.clusterName)
                .header("Remote-User", TestContext.REMOTE_USER)
                .accept(MediaType.TEXT_XML).delete(ClientResponse.class);
        context.assertSuccessful(clientRepsonse);
    }

    @Test
    public void testSubmit() throws Exception {
        TestContext context = newContext();
        ClientResponse response;
        Map<String, String> overlay = context.getUniqueOverlay();

        response = context.submitToFalcon(context.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.FEED_TEMPLATE1, overlay, EntityType.FEED);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.FEED_TEMPLATE2, overlay, EntityType.FEED);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.PROCESS_TEMPLATE, overlay, EntityType.PROCESS);
        context.assertSuccessful(response);
    }

    @Test
    public void testGetEntityDefinition() throws Exception {
        TestContext context = newContext();
        ClientResponse response;
        Map<String, String> overlay = context.getUniqueOverlay();

        response = context.submitToFalcon(context.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.FEED_TEMPLATE1, overlay, EntityType.FEED);
        context.assertSuccessful(response);

        response = context.service
                .path("api/entities/definition/feed/" + overlay.get("inputFeedName"))
                .header("Remote-User", TestContext.REMOTE_USER)
                .accept(MediaType.TEXT_XML).get(ClientResponse.class);

        String feedXML = response.getEntity(String.class);
        try {
            Feed result = (Feed) context.unmarshaller.
                    unmarshal(new StringReader(feedXML));
            Assert.assertEquals(result.getName(), overlay.get("inputFeedName"));
        } catch (JAXBException e) {
            Assert.fail("Reponse " + feedXML + " is not valid", e);
        }
    }

    @Test
    public void testInvalidGetEntityDefinition() {
        TestContext context = newContext();
        ClientResponse clientRepsonse = context.service
                .path("api/entities/definition/process/sample1")
                .header("Remote-User", TestContext.REMOTE_USER)
                .accept(MediaType.TEXT_XML).get(ClientResponse.class);
        context.assertFailure(clientRepsonse);
    }

    @Test
    public void testScheduleSuspendResume() throws Exception {
        TestContext context = newContext();
        context.scheduleProcess();

        ClientResponse clientRepsonse = context.service
                .path("api/entities/suspend/process/" + context.processName)
                .header("Remote-User", TestContext.REMOTE_USER)
                .accept(MediaType.TEXT_XML).post(ClientResponse.class);
        context.assertSuccessful(clientRepsonse);

        clientRepsonse = context.service
                .path("api/entities/resume/process/" + context.processName)
                .header("Remote-User", TestContext.REMOTE_USER)
                .accept(MediaType.TEXT_XML).post(ClientResponse.class);
        context.assertSuccessful(clientRepsonse);
    }

    @Test(enabled = true)
    public void testFeedSchedule() throws Exception {
        TestContext context = newContext();
        ClientResponse response;
        Map<String, String> overlay = context.getUniqueOverlay();

        response = context.submitToFalcon(context.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.FEED_TEMPLATE1, overlay, EntityType.FEED);
        context.assertSuccessful(response);

        createTestData(context);
        ClientResponse clientRepsonse = context.service
                .path("api/entities/schedule/feed/" + overlay.get("inputFeedName"))
                .header("Remote-User", TestContext.REMOTE_USER)
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class);
        context.assertSuccessful(clientRepsonse);
    }

    private List<Path> createTestData(TestContext context) throws Exception {
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

    @Test
    public void testDeleteDataSet() throws Exception {
        TestContext context = newContext();
        ClientResponse response;
        Map<String, String> overlay = context.getUniqueOverlay();

        response = context.submitToFalcon(context.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.FEED_TEMPLATE1, overlay, EntityType.FEED);
        context.assertSuccessful(response);

        response = context.service
                .path("api/entities/delete/feed/" + overlay.get("inputFeedName"))
                .header("Remote-User", TestContext.REMOTE_USER)
                .accept(MediaType.TEXT_XML).delete(ClientResponse.class);
        context.assertSuccessful(response);
    }

    @Test
    public void testDelete() throws Exception {
        TestContext context = newContext();
        ClientResponse response;
        Map<String, String> overlay = context.getUniqueOverlay();

        response = context.submitToFalcon(context.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.FEED_TEMPLATE1, overlay, EntityType.FEED);
        context.assertSuccessful(response);

        response = context.service
                .path("api/entities/delete/cluster/" + context.clusterName)
                .header("Remote-User", TestContext.REMOTE_USER)
                .accept(MediaType.TEXT_XML).delete(ClientResponse.class);
        context.assertFailure(response);

        response = context.submitToFalcon(TestContext.FEED_TEMPLATE2, overlay, EntityType.FEED);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.PROCESS_TEMPLATE, overlay, EntityType.PROCESS);
        context.assertSuccessful(response);

        //Delete a referred feed
        response = context.service
                .path("api/entities/delete/feed/" + overlay.get("inputFeedName"))
                .header("Remote-User", TestContext.REMOTE_USER)
                .accept(MediaType.TEXT_XML).delete(ClientResponse.class);
        context.assertFailure(response);

        //Delete a submitted process
        response = context.service
                .path("api/entities/delete/process/" + context.processName)
                .header("Remote-User", TestContext.REMOTE_USER)
                .accept(MediaType.TEXT_XML).delete(ClientResponse.class);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.PROCESS_TEMPLATE, overlay, EntityType.PROCESS);
        context.assertSuccessful(response);

        ClientResponse clientRepsonse = context.service
                .path("api/entities/schedule/process/" + context.processName)
                .header("Remote-User", TestContext.REMOTE_USER)
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class);
        context.assertSuccessful(clientRepsonse);

        //Delete a scheduled process
        response = context.service
                .path("api/entities/delete/process/" + context.processName)
                .header("Remote-User", TestContext.REMOTE_USER)
                .accept(MediaType.TEXT_XML).delete(ClientResponse.class);
        context.assertSuccessful(response);
    }

    @Test
    public void testGetDependencies() throws Exception {
        TestContext context = newContext();
        ClientResponse response;
        response = context.service
                .path("api/entities/list/process/")
                .header("Remote-User", TestContext.REMOTE_USER).type(MediaType.TEXT_XML)
                .accept(MediaType.TEXT_XML).get(ClientResponse.class);
        Assert.assertEquals(response.getStatus(), 200);

        Map<String, String> overlay = context.getUniqueOverlay();

        response = context.submitToFalcon(context.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);

        response = context.service
                .path("api/entities/list/cluster/")
                .header("Remote-User", TestContext.REMOTE_USER).type(MediaType.TEXT_XML)
                .accept(MediaType.TEXT_XML).get(ClientResponse.class);
        Assert.assertEquals(response.getStatus(), 200);
    }
}
