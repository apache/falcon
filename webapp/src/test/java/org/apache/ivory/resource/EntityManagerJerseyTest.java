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

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletInputStream;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.ivory.cluster.util.EmbeddedCluster;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.dataset.Dataset;
import org.apache.ivory.util.EmbeddedServer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

public class EntityManagerJerseyTest {

    private WebResource service = null;

    private static final String DATASET_TEMPLATE1 = "/dataset-template1.xml";
    private static final String DATASET_TEMPLATE2 = "/dataset-template2.xml";
    private static final String CLUSTER_FILE_TEMPLATE = "target/cluster-template.xml";

    private static final String SAMPLE_PROCESS_XML = "/process-version-0.xml";
    private static final String PROCESS_TEMPLATE = "/process-template.xml";

    private static final String BASE_URL = "http://localhost:15000/";

    private EmbeddedServer server;

    private Unmarshaller unmarshaller;
    private Marshaller marshaller;

    private EmbeddedCluster cluster;

    public EntityManagerJerseyTest() {
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(
                    APIResult.class, Dataset.class, Process.class,
                    Cluster.class);
            unmarshaller = jaxbContext.createUnmarshaller();
            marshaller = jaxbContext.createMarshaller();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public void configure() throws Exception {
        if (new File("webapp/src/main/webapp").exists()) {
            this.server = new EmbeddedServer(15000, "webapp/src/main/webapp");
        } else if (new File("src/main/webapp").exists()) {
            this.server = new EmbeddedServer(15000, "src/main/webapp");
        } else{
            throw new RuntimeException("Cannot run jersey tests");
        }
        ClientConfig config = new DefaultClientConfig();
        Client client = Client.create(config);
        this.service = client.resource(getBaseURI());
        this.server.start();
        this.cluster = EmbeddedCluster.newCluster("##name##", false);
        Cluster clusterEntity = this.cluster.getCluster();
        FileOutputStream out = new FileOutputStream(CLUSTER_FILE_TEMPLATE);
        marshaller.marshal(clusterEntity, out);
        out.close();
    }

    @AfterClass
    public void tearDown() throws Exception {
        this.cluster.shutdown();
    }

    @BeforeTest
    public void cleanupStore() throws Exception {
        ConfigurationStore.get().remove(EntityType.PROCESS, "aggregator-coord");
    }

    /**
     * Tests should be enabled only in local environments as they need running
     * instance of webserver
     */
    @Test
    public void testStatus() throws IOException {
        ClientResponse response;
        Map<String, String> overlay = new HashMap<String, String>();

        String feed1 = "f1" + System.currentTimeMillis();
        overlay.put("name", feed1);
        response = submitToIvory(DATASET_TEMPLATE1, overlay, EntityType.DATASET);
        checkIfSuccessful(response);

        response = this.service
                .path("api/entities/status/dataset/" + feed1)
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
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class, stream);

        checkIfBadRequest(clientRepsonse);
    }

    @Test
    public void testClusterSubmit() throws IOException {
        ClientResponse clientRepsonse;
        Map<String, String> overlay = new HashMap<String, String>();

        String cluster = "local" + System.currentTimeMillis();
        overlay.put("name", cluster);
        InputStream stream = getServletInputStream(overlayParametersOverTemplate(
                CLUSTER_FILE_TEMPLATE, overlay));

        clientRepsonse = this.service.path("api/entities/validate/cluster")
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class, stream);
        checkIfSuccessful(clientRepsonse);
    }

	@Test
	public void testClusterSubmitScheduleSuspendResumeDelete() throws IOException {
		ClientResponse clientRepsonse;
		Map<String, String> overlay = new HashMap<String, String>();

        String cluster = "local" + System.currentTimeMillis();
        overlay.put("name", cluster);
		clientRepsonse = submitToIvory(CLUSTER_FILE_TEMPLATE, overlay,
				EntityType.CLUSTER);
		checkIfSuccessful(clientRepsonse);

		clientRepsonse = this.service
				.path("api/entities/schedule/cluster/" + cluster)
				.accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
				.post(ClientResponse.class);
		checkIfBadRequest(clientRepsonse);

		clientRepsonse = this.service
				.path("api/entities/suspend/cluster/" + cluster)
				.accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
				.post(ClientResponse.class);
		checkIfBadRequest(clientRepsonse);

		clientRepsonse = this.service
				.path("api/entities/resume/cluster/" + cluster)
				.accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
				.post(ClientResponse.class);
		checkIfBadRequest(clientRepsonse);

		clientRepsonse = this.service
				.path("api/entities/delete/cluster/" + cluster)
				.accept(MediaType.TEXT_XML).delete(ClientResponse.class);
		checkIfSuccessful(clientRepsonse);
	}

    @Test
    public void testSubmit() throws IOException {

        ClientResponse response;
        Map<String, String> overlay = new HashMap<String, String>();

        String feed1 = "f1" + System.currentTimeMillis();
        overlay.put("name", feed1);
        response = submitToIvory(DATASET_TEMPLATE1, overlay, EntityType.DATASET);
        checkIfSuccessful(response);

        String feed2 = "f2" + System.currentTimeMillis();
        overlay.put("name", feed2);
        response = submitToIvory(DATASET_TEMPLATE2, overlay, EntityType.DATASET);
        checkIfSuccessful(response);

        String cluster = "local" + System.currentTimeMillis();
        overlay.put("name", cluster);
        response = submitToIvory(CLUSTER_FILE_TEMPLATE, overlay, EntityType.CLUSTER);
        checkIfSuccessful(response);

        String process = "p1" + System.currentTimeMillis();
        overlay.put("name", process);
        overlay.put("f1", feed1);
        overlay.put("f2", feed2);
        overlay.put("cluster", cluster);
        response = submitToIvory(PROCESS_TEMPLATE, overlay, EntityType.PROCESS);
        checkIfSuccessful(response);
    }

    @Test
    public void testGetEntityDefinition() throws Exception {
        ClientResponse response;
        Map<String, String> overlay = new HashMap<String, String>();

        String feed1 = "f1" + System.currentTimeMillis();
        overlay.put("name", feed1);
        response = submitToIvory(DATASET_TEMPLATE1, overlay, EntityType.DATASET);
        checkIfSuccessful(response);

        response = this.service
                .path("api/entities/definition/dataset/" + feed1)
                .accept(MediaType.TEXT_XML).get(ClientResponse.class);

        String datasetXML = response.getEntity(String.class);
        try {
            Dataset result = (Dataset)unmarshaller.
                    unmarshal(new StringReader(datasetXML));
            Assert.assertEquals(result.getName(), feed1);
        } catch (JAXBException e) {
            Assert.fail("Reponse " + datasetXML + " is not valid", e);
        }
    }

    @Test
    public void testInvalidGetEntityDefinition() {
        ClientResponse clientRepsonse = this.service
                .path("api/entities/definition/process/sample1")
                .accept(MediaType.TEXT_XML).get(ClientResponse.class);
        checkIfBadRequest(clientRepsonse);
    }

    private ClientResponse submitToIvory(String template,
                                         Map<String, String> overlay,
                                         EntityType entityType)
            throws IOException {
        String tmpFile = overlayParametersOverTemplate(template, overlay);
        return submitFileToIvory(entityType, tmpFile);
    }

    private ClientResponse submitFileToIvory(EntityType entityType,
                                             String tmpFile)
            throws IOException {

        ServletInputStream rawlogStream = getServletInputStream(tmpFile);

        return this.service
                .path("api/entities/submit/" + entityType.name().toLowerCase())
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class, rawlogStream);
    }

    private static final Pattern varPattern = Pattern.
            compile("##[A-Za-z0-9_]*##");

    private String overlayParametersOverTemplate(String template,
                                                 Map<String, String> overlay)
            throws IOException {

        File target = new File("webapp/target");
        if (!target.exists()) {
            target = new File("target");
        }

        File tmpFile = File.createTempFile("test", ".xml", target);
        OutputStream out = new FileOutputStream(tmpFile);

        InputStreamReader in;
        if (getClass().getResourceAsStream(template) == null) {
            in = new FileReader(template);
        } else {
            in = new InputStreamReader(getClass().getResourceAsStream(template));
        }
        BufferedReader reader = new BufferedReader(in);
        String line;
        while ((line = reader.readLine()) != null) {
            Matcher matcher = varPattern.matcher(line);
            while (matcher.find()) {
                String variable = line.substring(matcher.start(), matcher.end());
                line = line.replace(variable,
                        overlay.get(variable.substring(2, variable.length() - 2)));
                matcher = varPattern.matcher(line);
            }
            out.write(line.getBytes());
            out.write("\n".getBytes());
        }
        reader.close();
        out.close();
        return tmpFile.getAbsolutePath();
    }

    private void checkIfBadRequest(ClientResponse clientRepsonse) {
        Assert.assertEquals(clientRepsonse.getStatus(), Response.Status.
                BAD_REQUEST.getStatusCode());
    }

    @Test
    public void testScheduleSuspendResume() throws Exception {
        ClientResponse response;
        Map<String, String> overlay = new HashMap<String, String>();

        String feed1 = "f1" + System.currentTimeMillis();
        overlay.put("name", feed1);
        response = submitToIvory(DATASET_TEMPLATE1, overlay, EntityType.DATASET);
        checkIfSuccessful(response);

        String feed2 = "f2" + System.currentTimeMillis();
        overlay.put("name", feed2);
        response = submitToIvory(DATASET_TEMPLATE2, overlay, EntityType.DATASET);
        checkIfSuccessful(response);

        String cluster = "local" + System.currentTimeMillis();
        overlay.put("name", cluster);
        response = submitToIvory(CLUSTER_FILE_TEMPLATE, overlay, EntityType.CLUSTER);
        checkIfSuccessful(response);

        String process = "p1" + System.currentTimeMillis();
        overlay.put("name", process);
        overlay.put("f1", feed1);
        overlay.put("f2", feed2);
        overlay.put("cluster", cluster);
        response = submitToIvory(PROCESS_TEMPLATE, overlay, EntityType.PROCESS);
        checkIfSuccessful(response);

        ClientResponse clientRepsonse = this.service
        		.path("api/entities/schedule/process/" + process)
        		.accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
        		.post(ClientResponse.class);
        checkIfSuccessful(clientRepsonse);

        clientRepsonse = this.service
                .path("api/entities/suspend/process/" + process)
                .accept(MediaType.TEXT_XML).post(ClientResponse.class);
        checkIfSuccessful(clientRepsonse);

        clientRepsonse = this.service
                .path("api/entities/resume/process/" + process)
                .accept(MediaType.TEXT_XML).post(ClientResponse.class);
        checkIfSuccessful(clientRepsonse);
    }

    @Test
    public void testDeleteDataSet() throws Exception {
        ClientResponse response;
        Map<String, String> overlay = new HashMap<String, String>();

        String feed1 = "f1" + System.currentTimeMillis();
        overlay.put("name", feed1);
        response = submitToIvory(DATASET_TEMPLATE1, overlay, EntityType.DATASET);
        checkIfSuccessful(response);

        response = this.service
                .path("api/entities/delete/dataset/" + feed1)
                .accept(MediaType.TEXT_XML).delete(ClientResponse.class);
        checkIfSuccessful(response);
    }

    @Test
    public void testDelete() throws Exception {

        ClientResponse response;
        Map<String, String> overlay = new HashMap<String, String>();

        String feed1 = "f1" + System.currentTimeMillis();
        overlay.put("name", feed1);
        response = submitToIvory(DATASET_TEMPLATE1, overlay, EntityType.DATASET);
        checkIfSuccessful(response);

        String feed2 = "f2" + System.currentTimeMillis();
        overlay.put("name", feed2);
        response = submitToIvory(DATASET_TEMPLATE2, overlay, EntityType.DATASET);
        checkIfSuccessful(response);

        String cluster = "local" + System.currentTimeMillis();
        overlay.put("name", cluster);
        response = submitToIvory(CLUSTER_FILE_TEMPLATE, overlay, EntityType.CLUSTER);
        checkIfSuccessful(response);

        String process = "p1" + System.currentTimeMillis();
        overlay.put("name", process);
        overlay.put("f1", feed1);
        overlay.put("f2", feed2);
        overlay.put("cluster", cluster);
        response = submitToIvory(PROCESS_TEMPLATE, overlay, EntityType.PROCESS);
        checkIfSuccessful(response);

        //Delete a referred dataset
        response = this.service
                .path("api/entities/delete/dataset/" + feed1)
                .accept(MediaType.TEXT_XML).delete(ClientResponse.class);
        checkIfBadRequest(response);

        //Delete a submitted process
        response = this.service
                .path("api/entities/delete/process/" + process)
                .accept(MediaType.TEXT_XML).delete(ClientResponse.class);
        checkIfSuccessful(response);

        process = "p1" + System.currentTimeMillis();
        overlay.put("name", process);
        overlay.put("f1", feed1);
        overlay.put("f2", feed2);
        response = submitToIvory(PROCESS_TEMPLATE, overlay, EntityType.PROCESS);
        checkIfSuccessful(response);

        ClientResponse clientRepsonse = this.service
                .path("api/entities/schedule/process/" + process)
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class);
        checkIfSuccessful(clientRepsonse);

        //Delete a scheduled process
        response = this.service
                .path("api/entities/delete/process/" + process)
                .accept(MediaType.TEXT_XML).delete(ClientResponse.class);
        checkIfSuccessful(response);

    }

    private void checkIfSuccessful(ClientResponse clientRepsonse) {
        String response = clientRepsonse.getEntity(String.class);
        try {
            APIResult result = (APIResult)unmarshaller.
                    unmarshal(new StringReader(response));
            Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
        } catch (JAXBException e) {
            Assert.fail("Reponse " + response + " is not valid");
        }
    }

    private static URI getBaseURI() {
        return UriBuilder.fromUri(BASE_URL).build();
    }

    /**
     * Converts a InputStream into ServletInputStream
     *
     * @param fileName
     * @return ServletInputStream
     * @throws java.io.IOException
     */
    private ServletInputStream getServletInputStream(String fileName)
            throws IOException {
        return getServletInputStream(new FileInputStream(fileName));
    }

    private ServletInputStream getServletInputStream(final InputStream stream)
            throws IOException {
        return new ServletInputStream() {

            @Override
            public int read() throws IOException {
                return stream.read();
            }
        };
    }

    @AfterClass
    public void cleanup() throws Exception {
        this.server.stop();
    }
}
