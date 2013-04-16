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
import org.apache.falcon.FalconWebException;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Property;
import org.apache.falcon.entity.v0.process.Validity;
import org.apache.falcon.util.BuildProperties;
import org.apache.falcon.util.DeploymentProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.Job.Status;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.servlet.ServletInputStream;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.InputStream;
import java.io.StringReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class EntityManagerJerseyTest extends AbstractTestBase {
    /**
     * Tests should be enabled only in local environments as they need running
     * instance of webserver
     */

    @Test
    public void testUpdateCheckUser() throws Exception {
        Map<String, String> overlay = getUniqueOverlay();
        String tmpFileName = overlayParametersOverTemplate(PROCESS_TEMPLATE, overlay);
        Process process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(new File(tmpFileName));
        Validity processValidity = process.getClusters().getClusters().get(0).getValidity();
        processValidity.setEnd(new Date(new Date().getTime() + 2 * 24 * 60 * 60 * 1000));
        File tmpFile = getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);
        scheduleProcess(tmpFile.getAbsolutePath(), overlay);
        waitForBundleStart(Status.RUNNING);

        List<BundleJob> bundles = getBundles();
        Assert.assertEquals(bundles.size(), 1);
        Assert.assertEquals(bundles.get(0).getUser(), REMOTE_USER);

        ClientResponse response = this.service.path("api/entities/definition/feed/" + outputFeedName).header(
                "Remote-User", REMOTE_USER)
                .accept(MediaType.TEXT_XML).get(ClientResponse.class);
        Feed feed = (Feed) EntityType.FEED.getUnmarshaller()
                .unmarshal(new StringReader(response.getEntity(String.class)));

        //change output feed path and update feed as another user
        feed.getLocations().getLocations().get(0).setPath("/falcon/test/output2/${YEAR}/${MONTH}/${DAY}");
        tmpFile = getTempFile();
        EntityType.FEED.getMarshaller().marshal(feed, tmpFile);
        response = this.service.path("api/entities/update/feed/" + outputFeedName).header("Remote-User",
                "testuser").accept(MediaType.TEXT_XML)
                .post(ClientResponse.class, getServletInputStream(tmpFile.getAbsolutePath()));
        assertSuccessful(response);

        bundles = getBundles();
        Assert.assertEquals(bundles.size(), 2);
        Assert.assertEquals(bundles.get(0).getUser(), REMOTE_USER);
        Assert.assertEquals(bundles.get(1).getUser(), REMOTE_USER);
    }


    @Test(enabled = false)
    public void testOptionalInput() throws Exception {
        Map<String, String> overlay = getUniqueOverlay();
        String tmpFileName = overlayParametersOverTemplate(PROCESS_TEMPLATE, overlay);
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

        File tmpFile = getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);
        scheduleProcess(tmpFile.getAbsolutePath(), overlay);
        waitForWorkflowStart(processName);
    }

    @Test
    public void testProcessDeleteAndSchedule() throws Exception {
        //Submit process with invalid property so that coord submit fails and bundle goes to failed state
        Map<String, String> overlay = getUniqueOverlay();
        String tmpFileName = overlayParametersOverTemplate(PROCESS_TEMPLATE, overlay);
        Process process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(new File(tmpFileName));
        Property prop = new Property();
        prop.setName("newProp");
        prop.setValue("${formatTim()}");
        process.getProperties().getProperties().add(prop);
        File tmpFile = getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);
        scheduleProcess(tmpFile.getAbsolutePath(), overlay);
        waitForBundleStart(Status.FAILED);

        //Delete and re-submit the process with correct workflow
        ClientResponse clientRepsonse = this.service.path("api/entities/delete/process/" + processName).header(
                "Remote-User", REMOTE_USER)
                .accept(MediaType.TEXT_XML).delete(ClientResponse.class);
        assertSuccessful(clientRepsonse);
        process.getWorkflow().setPath("/falcon/test/workflow");
        tmpFile = getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);
        clientRepsonse = this.service.path("api/entities/submitAndSchedule/process").header("Remote-User", REMOTE_USER)
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class, getServletInputStream(tmpFile.getAbsolutePath()));
        assertSuccessful(clientRepsonse);

        //Assert that new schedule creates new bundle
        List<BundleJob> bundles = getBundles();
        Assert.assertEquals(bundles.size(), 2);
    }

    @Test
    public void testProcessInputUpdate() throws Exception {
        scheduleProcess();
        waitForBundleStart(Job.Status.RUNNING);

        ClientResponse response = this.service.path("api/entities/definition/process/" + processName).header(
                "Remote-User", REMOTE_USER)
                .accept(MediaType.TEXT_XML).get(ClientResponse.class);
        Process process = (Process) EntityType.PROCESS.getUnmarshaller()
                .unmarshal(new StringReader(response.getEntity(String.class)));

        String feed3 = "f3" + System.currentTimeMillis();
        Map<String, String> overlay = new HashMap<String, String>();
        overlay.put("inputFeedName", feed3);
        overlay.put("cluster", clusterName);
        response = submitToFalcon(FEED_TEMPLATE1, overlay, EntityType.FEED);
        assertSuccessful(response);

        Input input = new Input();
        input.setFeed(feed3);
        input.setName("inputData2");
        input.setStart("today(20,0)");
        input.setEnd("today(20,20)");
        process.getInputs().getInputs().add(input);

        Validity processValidity = process.getClusters().getClusters().get(0).getValidity();
        processValidity.setEnd(new Date(new Date().getTime() + 2 * 24 * 60 * 60 * 1000));
        File tmpFile = getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);
        response = this.service.path("api/entities/update/process/" + processName).header("Remote-User",
                REMOTE_USER).accept(MediaType.TEXT_XML)
                .post(ClientResponse.class, getServletInputStream(tmpFile.getAbsolutePath()));
        assertSuccessful(response);

        //Assert that update creates new bundle
        List<BundleJob> bundles = getBundles();
        Assert.assertEquals(bundles.size(), 2);
    }

    @Test
    public void testProcessEndtimeUpdate() throws Exception {
        scheduleProcess();
        waitForBundleStart(Job.Status.RUNNING);

        ClientResponse response = this.service.path("api/entities/definition/process/" + processName).header(
                "Remote-User", REMOTE_USER)
                .accept(MediaType.TEXT_XML).get(ClientResponse.class);
        Process process = (Process) EntityType.PROCESS.getUnmarshaller()
                .unmarshal(new StringReader(response.getEntity(String.class)));

        Validity processValidity = process.getClusters().getClusters().get(0).getValidity();
        processValidity.setEnd(new Date(new Date().getTime() + 60 * 60 * 1000));
        File tmpFile = getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);
        response = this.service.path("api/entities/update/process/" + processName).header("Remote-User",
                REMOTE_USER).accept(MediaType.TEXT_XML)
                .post(ClientResponse.class, getServletInputStream(tmpFile.getAbsolutePath()));
        assertSuccessful(response);

        //Assert that update does not create new bundle
        List<BundleJob> bundles = getBundles();
        Assert.assertEquals(bundles.size(), 1);
    }

    @Test
    public void testStatus() throws Exception {
        ClientResponse response;
        Map<String, String> overlay = getUniqueOverlay();

        response = submitToFalcon(CLUSTER_FILE_TEMPLATE, overlay, EntityType.CLUSTER);
        assertSuccessful(response);

        response = submitToFalcon(FEED_TEMPLATE1, overlay, EntityType.FEED);
        assertSuccessful(response);

        response = this.service
                .path("api/entities/status/feed/" + overlay.get("inputFeedName"))
                .header("Remote-User", REMOTE_USER)
                .accept(MediaType.TEXT_XML).get(ClientResponse.class);

        APIResult result = (APIResult) unmarshaller.
                unmarshal(new StringReader(response.getEntity(String.class)));
        Assert.assertTrue(result.getMessage().contains("SUBMITTED"));

    }

    @Test
    public void testIdempotentSubmit() throws Exception {
        ClientResponse response;
        Map<String, String> overlay = getUniqueOverlay();

        response = submitToFalcon(CLUSTER_FILE_TEMPLATE, overlay, EntityType.CLUSTER);
        assertSuccessful(response);

        response = submitToFalcon(CLUSTER_FILE_TEMPLATE, overlay, EntityType.CLUSTER);
        assertSuccessful(response);
    }

    @Test
    public void testNotFoundStatus() throws FalconWebException {
        ClientResponse response;
        String feed1 = "f1" + System.currentTimeMillis();
        response = this.service
                .path("api/entities/status/feed/" + feed1)
                .header("Remote-User", REMOTE_USER)
                .accept(MediaType.TEXT_PLAIN).get(ClientResponse.class);
        String status = response.getEntity(String.class);
        Assert.assertEquals(response.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());

    }

    @Test
    public void testVersion() throws FalconWebException {
        ClientResponse response;
        response = this.service
                .path("api/admin/version")
                .header("Remote-User", REMOTE_USER)
                .accept(MediaType.TEXT_PLAIN).get(ClientResponse.class);
        String status = response.getEntity(String.class);
        Assert.assertEquals(status, "{Version:\"" +
                BuildProperties.get().getProperty("build.version") + "\",Mode:\"" +
                DeploymentProperties.get().getProperty("deploy.mode") + "\"}");

    }

    @Test
    public void testValidate() {

        ServletInputStream stream = getServletInputStream(getClass().
                getResourceAsStream(SAMPLE_PROCESS_XML));

        ClientResponse clientRepsonse = this.service
                .path("api/entities/validate/process")
                .header("Remote-User", REMOTE_USER)
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class, stream);

        assertFailure(clientRepsonse);
    }

    @Test
    public void testClusterValidate() throws Exception {
        ClientResponse clientRepsonse;
        Map<String, String> overlay = getUniqueOverlay();

        InputStream stream = getServletInputStream(overlayParametersOverTemplate(
                CLUSTER_FILE_TEMPLATE, overlay));

        clientRepsonse = this.service.path("api/entities/validate/cluster")
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .header("Remote-User", REMOTE_USER)
                .post(ClientResponse.class, stream);
        assertSuccessful(clientRepsonse);
    }

    @Test
    public void testClusterSubmitScheduleSuspendResumeDelete() throws Exception {
        ClientResponse clientRepsonse;
        Map<String, String> overlay = getUniqueOverlay();

        clientRepsonse = submitToFalcon(CLUSTER_FILE_TEMPLATE, overlay,
                EntityType.CLUSTER);
        assertSuccessful(clientRepsonse);

        clientRepsonse = this.service
                .path("api/entities/schedule/cluster/" + clusterName)
                .header("Remote-User", REMOTE_USER)
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class);
        assertFailure(clientRepsonse);

        clientRepsonse = this.service
                .path("api/entities/suspend/cluster/" + clusterName)
                .header("Remote-User", REMOTE_USER)
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class);
        assertFailure(clientRepsonse);

        clientRepsonse = this.service
                .path("api/entities/resume/cluster/" + clusterName)
                .header("Remote-User", REMOTE_USER)
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class);
        assertFailure(clientRepsonse);

        clientRepsonse = this.service
                .path("api/entities/delete/cluster/" + clusterName)
                .header("Remote-User", REMOTE_USER)
                .accept(MediaType.TEXT_XML).delete(ClientResponse.class);
        assertSuccessful(clientRepsonse);
    }

    @Test
    public void testSubmit() throws Exception {

        ClientResponse response;
        Map<String, String> overlay = getUniqueOverlay();

        response = submitToFalcon(CLUSTER_FILE_TEMPLATE, overlay, EntityType.CLUSTER);
        assertSuccessful(response);

        response = submitToFalcon(FEED_TEMPLATE1, overlay, EntityType.FEED);
        assertSuccessful(response);

        response = submitToFalcon(FEED_TEMPLATE2, overlay, EntityType.FEED);
        assertSuccessful(response);

        response = submitToFalcon(PROCESS_TEMPLATE, overlay, EntityType.PROCESS);
        assertSuccessful(response);
    }

    @Test
    public void testGetEntityDefinition() throws Exception {
        ClientResponse response;
        Map<String, String> overlay = getUniqueOverlay();

        response = submitToFalcon(CLUSTER_FILE_TEMPLATE, overlay, EntityType.CLUSTER);
        assertSuccessful(response);

        response = submitToFalcon(FEED_TEMPLATE1, overlay, EntityType.FEED);
        assertSuccessful(response);

        response = this.service
                .path("api/entities/definition/feed/" + overlay.get("inputFeedName"))
                .header("Remote-User", REMOTE_USER)
                .accept(MediaType.TEXT_XML).get(ClientResponse.class);

        String feedXML = response.getEntity(String.class);
        try {
            Feed result = (Feed) unmarshaller.
                    unmarshal(new StringReader(feedXML));
            Assert.assertEquals(result.getName(), overlay.get("inputFeedName"));
        } catch (JAXBException e) {
            Assert.fail("Reponse " + feedXML + " is not valid", e);
        }
    }

    @Test
    public void testInvalidGetEntityDefinition() {
        ClientResponse clientRepsonse = this.service
                .path("api/entities/definition/process/sample1")
                .header("Remote-User", REMOTE_USER)
                .accept(MediaType.TEXT_XML).get(ClientResponse.class);
        assertFailure(clientRepsonse);
    }

    @Test
    public void testScheduleSuspendResume() throws Exception {
        scheduleProcess();

        ClientResponse clientRepsonse = this.service
                .path("api/entities/suspend/process/" + processName)
                .header("Remote-User", REMOTE_USER)
                .accept(MediaType.TEXT_XML).post(ClientResponse.class);
        assertSuccessful(clientRepsonse);

        clientRepsonse = this.service
                .path("api/entities/resume/process/" + processName)
                .header("Remote-User", REMOTE_USER)
                .accept(MediaType.TEXT_XML).post(ClientResponse.class);
        assertSuccessful(clientRepsonse);
    }

    @Test(enabled = true)
    public void testFeedSchedule() throws Exception {
        ClientResponse response;
        Map<String, String> overlay = getUniqueOverlay();

        response = submitToFalcon(CLUSTER_FILE_TEMPLATE, overlay, EntityType.CLUSTER);
        assertSuccessful(response);

        response = submitToFalcon(FEED_TEMPLATE1, overlay, EntityType.FEED);
        assertSuccessful(response);

        createTestData();
        ClientResponse clientRepsonse = this.service
                .path("api/entities/schedule/feed/" + overlay.get("inputFeedName"))
                .header("Remote-User", REMOTE_USER)
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class);
        assertSuccessful(clientRepsonse);
    }

    private List<Path> createTestData() throws Exception {
        List<Path> list = new ArrayList<Path>();
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://localhost:8020");
        FileSystem fs = FileSystem.get(conf);
        fs.mkdirs(new Path("/user/guest"));
        fs.setOwner(new Path("/user/guest"), REMOTE_USER, "users");

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
        new FsShell(conf).run(new String[]{"-chown", "-R", "guest:users", "/examples/input-data/rawLogs"});
        return list;
    }

    @Test
    public void testDeleteDataSet() throws Exception {
        ClientResponse response;
        Map<String, String> overlay = getUniqueOverlay();

        response = submitToFalcon(CLUSTER_FILE_TEMPLATE, overlay, EntityType.CLUSTER);
        assertSuccessful(response);

        response = submitToFalcon(FEED_TEMPLATE1, overlay, EntityType.FEED);
        assertSuccessful(response);

        response = this.service
                .path("api/entities/delete/feed/" + overlay.get("inputFeedName"))
                .header("Remote-User", REMOTE_USER)
                .accept(MediaType.TEXT_XML).delete(ClientResponse.class);
        assertSuccessful(response);
    }

    @Test
    public void testDelete() throws Exception {

        ClientResponse response;
        Map<String, String> overlay = getUniqueOverlay();

        response = submitToFalcon(CLUSTER_FILE_TEMPLATE, overlay, EntityType.CLUSTER);
        assertSuccessful(response);

        response = submitToFalcon(FEED_TEMPLATE1, overlay, EntityType.FEED);
        assertSuccessful(response);

        response = this.service
                .path("api/entities/delete/cluster/" + clusterName)
                .header("Remote-User", REMOTE_USER)
                .accept(MediaType.TEXT_XML).delete(ClientResponse.class);
        assertFailure(response);

        response = submitToFalcon(FEED_TEMPLATE2, overlay, EntityType.FEED);
        assertSuccessful(response);

        response = submitToFalcon(PROCESS_TEMPLATE, overlay, EntityType.PROCESS);
        assertSuccessful(response);

        //Delete a referred feed
        response = this.service
                .path("api/entities/delete/feed/" + overlay.get("inputFeedName"))
                .header("Remote-User", REMOTE_USER)
                .accept(MediaType.TEXT_XML).delete(ClientResponse.class);
        assertFailure(response);

        //Delete a submitted process
        response = this.service
                .path("api/entities/delete/process/" + processName)
                .header("Remote-User", REMOTE_USER)
                .accept(MediaType.TEXT_XML).delete(ClientResponse.class);
        assertSuccessful(response);

        response = submitToFalcon(PROCESS_TEMPLATE, overlay, EntityType.PROCESS);
        assertSuccessful(response);

        ClientResponse clientRepsonse = this.service
                .path("api/entities/schedule/process/" + processName)
                .header("Remote-User", REMOTE_USER)
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class);
        assertSuccessful(clientRepsonse);

        //Delete a scheduled process
        response = this.service
                .path("api/entities/delete/process/" + processName)
                .header("Remote-User", REMOTE_USER)
                .accept(MediaType.TEXT_XML).delete(ClientResponse.class);
        assertSuccessful(response);

    }

    @Test
    public void testGetDependencies() throws Exception {
        ClientResponse response;
        response = this.service
                .path("api/entities/list/process/")
                .header("Remote-User", REMOTE_USER).type(MediaType.TEXT_XML)
                .accept(MediaType.TEXT_XML).get(ClientResponse.class);
        Assert.assertEquals(response.getStatus(), 200);

        Map<String, String> overlay = getUniqueOverlay();

        response = submitToFalcon(CLUSTER_FILE_TEMPLATE, overlay, EntityType.CLUSTER);
        assertSuccessful(response);

        response = this.service
                .path("api/entities/list/cluster/")
                .header("Remote-User", REMOTE_USER).type(MediaType.TEXT_XML)
                .accept(MediaType.TEXT_XML).get(ClientResponse.class);
        Assert.assertEquals(response.getStatus(), 200);

    }
}
