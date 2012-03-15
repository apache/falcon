/*
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
package org.apache.ivory.resource;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.servlet.ServletInputStream;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.process.Input;
import org.apache.ivory.entity.v0.process.Process;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.sun.jersey.api.client.ClientResponse;

public class EntityManagerJerseyTest extends AbstractTestBase{
    /**
     * Tests should be enabled only in local environments as they need running
     * instance of webserver
     */
    
    @Test(enabled=false)
    public void testProcessUpdate() throws Exception {
        scheduleProcess();
        waitForProcessStart();
        
        ClientResponse response = this.service.path("api/entities/definition/process/" + processName).header("Remote-User", "guest")
                .accept(MediaType.TEXT_XML).get(ClientResponse.class);
        Process process = (Process) EntityType.PROCESS.getUnmarshaller()
                .unmarshal(new StringReader(response.getEntity(String.class)));

        String feed3 = "f3" + System.currentTimeMillis();
        Map<String, String> overlay = new HashMap<String, String>();
        overlay.put("name", feed3);
        overlay.put("cluster", clusterName);
        response = submitToIvory(FEED_TEMPLATE1, overlay, EntityType.FEED);
        assertSuccessful(response);

        Input input = new Input();
        input.setFeed(feed3);
        input.setName("inputData2");
        input.setStartInstance("today(20,0)");
        input.setEndInstance("today(20,20)");
        process.getInputs().getInput().add(input);

        File tmpFile = getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);
        response = this.service.path("api/entities/update/process/" + processName).header("Remote-User", "guest").accept(MediaType.TEXT_XML)
                .post(ClientResponse.class, getServletInputStream(tmpFile.getAbsolutePath()));
        assertSuccessful(response);    
    }
    
    @Test
    public void testStatus() throws Exception {
        ClientResponse response;
        Map<String, String> overlay = new HashMap<String, String>();

        String cluster = "local" + System.currentTimeMillis();
        overlay.put("name", cluster);
        response = submitToIvory(CLUSTER_FILE_TEMPLATE, overlay, EntityType.CLUSTER);
        assertSuccessful(response);

        String feed1 = "f1" + System.currentTimeMillis();
        overlay.put("name", feed1);
        overlay.put("cluster", cluster);
        response = submitToIvory(FEED_TEMPLATE1, overlay, EntityType.FEED);
        assertSuccessful(response);

        response = this.service
                .path("api/entities/status/feed/" + feed1)
                .header("Remote-User", "testuser")
                .accept(MediaType.TEXT_PLAIN).get(ClientResponse.class);

        String status = response.getEntity(String.class);
        Assert.assertEquals(status, "NOT_SCHEDULED");
    }

    @Test
    public void testValidate() throws IOException {

        ServletInputStream stream = getServletInputStream(getClass().
                getResourceAsStream(SAMPLE_PROCESS_XML));

        ClientResponse clientRepsonse = this.service
                .path("api/entities/validate/process")
                .header("Remote-User", "testuser")
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class, stream);

        checkIfBadRequest(clientRepsonse);
    }

    @Test
    public void testClusterSubmit() throws Exception {
        ClientResponse clientRepsonse;
        Map<String, String> overlay = new HashMap<String, String>();

        String cluster = "local" + System.currentTimeMillis();
        overlay.put("name", cluster);
        InputStream stream = getServletInputStream(overlayParametersOverTemplate(
                CLUSTER_FILE_TEMPLATE, overlay));

        clientRepsonse = this.service.path("api/entities/validate/cluster")
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .header("Remote-User", "testuser")
                .post(ClientResponse.class, stream);
        assertSuccessful(clientRepsonse);
    }

	@Test
	public void testClusterSubmitScheduleSuspendResumeDelete() throws Exception {
		ClientResponse clientRepsonse;
		Map<String, String> overlay = new HashMap<String, String>();

        String cluster = "local" + System.currentTimeMillis();
        overlay.put("name", cluster);
		clientRepsonse = submitToIvory(CLUSTER_FILE_TEMPLATE, overlay,
				EntityType.CLUSTER);
		assertSuccessful(clientRepsonse);

		clientRepsonse = this.service
				.path("api/entities/schedule/cluster/" + cluster)
                .header("Remote-User", "testuser")
				.accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
				.post(ClientResponse.class);
		checkIfBadRequest(clientRepsonse);

		clientRepsonse = this.service
				.path("api/entities/suspend/cluster/" + cluster)
                .header("Remote-User", "testuser")
				.accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
				.post(ClientResponse.class);
		checkIfBadRequest(clientRepsonse);

		clientRepsonse = this.service
				.path("api/entities/resume/cluster/" + cluster)
                .header("Remote-User", "testuser")
				.accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
				.post(ClientResponse.class);
		checkIfBadRequest(clientRepsonse);

		clientRepsonse = this.service
				.path("api/entities/delete/cluster/" + cluster)
                .header("Remote-User", "testuser")
				.accept(MediaType.TEXT_XML).delete(ClientResponse.class);
		assertSuccessful(clientRepsonse);
	}

    @Test
    public void testSubmit() throws Exception {

        ClientResponse response;
        Map<String, String> overlay = new HashMap<String, String>();

        String cluster = "local" + System.currentTimeMillis();
        overlay.put("name", cluster);
        response = submitToIvory(CLUSTER_FILE_TEMPLATE, overlay, EntityType.CLUSTER);
        assertSuccessful(response);

        String feed1 = "f1" + System.currentTimeMillis();
        overlay.put("name", feed1);
        overlay.put("cluster", cluster);
        response = submitToIvory(FEED_TEMPLATE1, overlay, EntityType.FEED);
        assertSuccessful(response);

        String feed2 = "f2" + System.currentTimeMillis();
        overlay.put("name", feed2);
        response = submitToIvory(FEED_TEMPLATE2, overlay, EntityType.FEED);
        assertSuccessful(response);

        String process = "p1" + System.currentTimeMillis();
        overlay.put("name", process);
        overlay.put("f1", feed1);
        overlay.put("f2", feed2);
        response = submitToIvory(PROCESS_TEMPLATE, overlay, EntityType.PROCESS);
        assertSuccessful(response);
    }

    @Test
    public void testGetEntityDefinition() throws Exception {
        ClientResponse response;
        Map<String, String> overlay = new HashMap<String, String>();

        String cluster = "local" + System.currentTimeMillis();
        overlay.put("name", cluster);
        response = submitToIvory(CLUSTER_FILE_TEMPLATE, overlay, EntityType.CLUSTER);
        assertSuccessful(response);

        String feed1 = "f1" + System.currentTimeMillis();
        overlay.put("name", feed1);
        overlay.put("cluster", cluster);
        response = submitToIvory(FEED_TEMPLATE1, overlay, EntityType.FEED);
        assertSuccessful(response);

        response = this.service
                .path("api/entities/definition/feed/" + feed1)
                .header("Remote-User", "testuser")
                .accept(MediaType.TEXT_XML).get(ClientResponse.class);

        String feedXML = response.getEntity(String.class);
        try {
            Feed result = (Feed)unmarshaller.
                    unmarshal(new StringReader(feedXML));
            Assert.assertEquals(result.getName(), feed1);
        } catch (JAXBException e) {
            Assert.fail("Reponse " + feedXML + " is not valid", e);
        }
    }

    @Test
    public void testInvalidGetEntityDefinition() {
        ClientResponse clientRepsonse = this.service
                .path("api/entities/definition/process/sample1")
                .header("Remote-User", "testuser")
                .accept(MediaType.TEXT_XML).get(ClientResponse.class);
        checkIfBadRequest(clientRepsonse);
    }

    private void checkIfBadRequest(ClientResponse clientRepsonse) {
        Assert.assertEquals(clientRepsonse.getStatus(), Response.Status.
                BAD_REQUEST.getStatusCode());
    }

    @Test
    public void testScheduleSuspendResume() throws Exception {
        scheduleProcess();
        
        ClientResponse clientRepsonse = this.service
                .path("api/entities/suspend/process/" + processName)
                .header("Remote-User", "testuser")
                .accept(MediaType.TEXT_XML).post(ClientResponse.class);
        assertSuccessful(clientRepsonse);

        clientRepsonse = this.service
                .path("api/entities/resume/process/" + processName)
                .header("Remote-User", "testuser")
                .accept(MediaType.TEXT_XML).post(ClientResponse.class);
        assertSuccessful(clientRepsonse);
    }

    @Test  (enabled = false)
    public void testFeedSchedule() throws Exception {
        ClientResponse response;
        Map<String, String> overlay = new HashMap<String, String>();

        String cluster = "local" + System.currentTimeMillis();
        overlay.put("name", cluster);
        response = submitToIvory(CLUSTER_FILE_TEMPLATE, overlay, EntityType.CLUSTER);
        assertSuccessful(response);

        String feed1 = "f1" + System.currentTimeMillis();
        overlay.put("name", feed1);
        overlay.put("cluster", cluster);
        response = submitToIvory(FEED_TEMPLATE1, overlay, EntityType.FEED);
        assertSuccessful(response);

        List<Path> validInstances = createTestData();
        ClientResponse clientRepsonse = this.service
        		.path("api/entities/schedule/feed/" + feed1)
                .header("Remote-User", "guest")
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
        fs.setOwner(new Path("/user/guest"), "guest", "users");

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
        new FsShell(conf).run(new String[] {"-chown", "-R", "guest:users", "/examples/input-data/rawLogs"});
        return list;
    }

    @Test
    public void testDeleteDataSet() throws Exception {
        ClientResponse response;
        Map<String, String> overlay = new HashMap<String, String>();

        String cluster = "local" + System.currentTimeMillis();
        overlay.put("name", cluster);
        response = submitToIvory(CLUSTER_FILE_TEMPLATE, overlay, EntityType.CLUSTER);
        assertSuccessful(response);

        String feed1 = "f1" + System.currentTimeMillis();
        overlay.put("name", feed1);
        overlay.put("cluster", cluster);
        response = submitToIvory(FEED_TEMPLATE1, overlay, EntityType.FEED);
        assertSuccessful(response);

        response = this.service
                .path("api/entities/delete/feed/" + feed1)
                .header("Remote-User", "testuser")
                .accept(MediaType.TEXT_XML).delete(ClientResponse.class);
        assertSuccessful(response);
    }

    @Test
    public void testDelete() throws Exception {

        ClientResponse response;
        Map<String, String> overlay = new HashMap<String, String>();

        String cluster = "local" + System.currentTimeMillis();
        overlay.put("name", cluster);
        response = submitToIvory(CLUSTER_FILE_TEMPLATE, overlay, EntityType.CLUSTER);
        assertSuccessful(response);

        String feed1 = "f1" + System.currentTimeMillis();
        overlay.put("name", feed1);
        overlay.put("cluster", cluster);
        response = submitToIvory(FEED_TEMPLATE1, overlay, EntityType.FEED);
        assertSuccessful(response);

        String feed2 = "f2" + System.currentTimeMillis();
        overlay.put("name", feed2);
        response = submitToIvory(FEED_TEMPLATE2, overlay, EntityType.FEED);
        assertSuccessful(response);

        String process = "p1" + System.currentTimeMillis();
        overlay.put("name", process);
        overlay.put("f1", feed1);
        overlay.put("f2", feed2);
        response = submitToIvory(PROCESS_TEMPLATE, overlay, EntityType.PROCESS);
        assertSuccessful(response);

        //Delete a referred feed
        response = this.service
                .path("api/entities/delete/feed/" + feed1)
                .header("Remote-User", "testuser")
                .accept(MediaType.TEXT_XML).delete(ClientResponse.class);
        checkIfBadRequest(response);

        //Delete a submitted process
        response = this.service
                .path("api/entities/delete/process/" + process)
                .header("Remote-User", "testuser")
                .accept(MediaType.TEXT_XML).delete(ClientResponse.class);
        assertSuccessful(response);

        process = "p1" + System.currentTimeMillis();
        overlay.put("name", process);
        overlay.put("f1", feed1);
        overlay.put("f2", feed2);
        response = submitToIvory(PROCESS_TEMPLATE, overlay, EntityType.PROCESS);
        assertSuccessful(response);

        ClientResponse clientRepsonse = this.service
                .path("api/entities/schedule/process/" + process)
                .header("Remote-User", "testuser")
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class);
        assertSuccessful(clientRepsonse);

        //Delete a scheduled process
        response = this.service
                .path("api/entities/delete/process/" + process)
                .header("Remote-User", "testuser")
                .accept(MediaType.TEXT_XML).delete(ClientResponse.class);
        assertSuccessful(response);

    }
}
