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
import java.net.URI;

import javax.servlet.ServletInputStream;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

import org.apache.ivory.util.EmbeddedServer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

public class EntityManagerJerseyTest {

	private WebResource service = null;

	private static final String RAW_LOGS_DATASET_XML = "/dataset-raw-logs.xml";
	private static final String AGG_LOGS_DATASET_XML = "/dataset-agg-logs.xml";

	private static final String SAMPLE_PROCESS_XML = "/process-version-0.xml";
	private static final String AGG_PROCESS_XML = "/process-agg.xml";

	private static final String BASE_URL = "http://localhost:15000/";

	EmbeddedServer server;

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
	}

	/**
	 * Tests should be enabled only in local environments as they need running
	 * instance of webserver
	 */
	@Test
	public void testStatus() {
		// http://localhost:15000/api/entities/status/hello/1
		System.out.println(this.service.path("api/entities/status/hello/1")
				.accept(MediaType.TEXT_PLAIN).get(String.class));
	}

	@Test
	public void testValidate() {

		ServletInputStream stream = getServletInputStream(SAMPLE_PROCESS_XML);

		ClientResponse clientRepsonse = this.service
				.path("api/entities/validate/process")
				.accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
				.post(ClientResponse.class, stream);

		Assert.assertEquals(
				clientRepsonse.getEntity(String.class),
				"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><result><status>SUCCEEDED</status><message>validate successful</message></result>");

	}

	@Test(dependsOnMethods = { "testValidate" })
	public void testSubmit() {

		ServletInputStream rawlogStream = getServletInputStream(RAW_LOGS_DATASET_XML);

		this.service
		.path("api/entities/submit/dataset")
		.accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
		.post(ClientResponse.class, rawlogStream);

		ServletInputStream outputStream = getServletInputStream(AGG_LOGS_DATASET_XML);

		this.service
		.path("api/entities/submit/dataset")
		.accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
		.post(ClientResponse.class, outputStream);		

		ServletInputStream processStream = getServletInputStream(AGG_PROCESS_XML);

		ClientResponse clientRepsonse = this.service
				.path("api/entities/submit/process")
				.accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
				.post(ClientResponse.class, processStream);

		Assert.assertEquals(
				clientRepsonse.getEntity(String.class),
				"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><result><status>SUCCEEDED</status><message>submit successful</message></result>");


	}

	@Test(dependsOnMethods = { "testSubmit" })
	public void testGetEntityDefinition() {
		ClientResponse clientRepsonse = this.service
				.path("api/entities/definition/process/aggregator-coord")
				.accept(MediaType.TEXT_XML).get(ClientResponse.class);
//TODO
//		Assert.assertEquals(
//				clientRepsonse.toString(),
//				"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><coordinator-app name=\"aggregator-coord\" frequency=\"${coord:minutes(5)}\" start=\"${start}\" end=\"${end}\" timezone=\"UTC\" xmlns=\"uri:oozie:coordinator:0.2\"><controls><concurrency>2</concurrency><execution>LIFO</execution></controls><datasets><dataset name=\"raw-logs\" frequency=\"${coord:minutes(20)}\" initial-instance=\"2010-01-01T00:00Z\"><uri-template>/feed/input/1</uri-template></dataset><dataset name=\"aggregated-logs\" frequency=\"${coord:hours(1)}\" initial-instance=\"2010-01-01T00:00Z\"><uri-template>/feed/input/1</uri-template></dataset></datasets><input-events><data-in name=\"input\" dataset=\"raw-logs\"><start-instance>${coord:current(-2)}</start-instance><end-instance>${coord:current(0)}</end-instance></data-in></input-events><output-events><data-out name=\"output\" dataset=\"aggregated-logs\"><instance>${coord:current(0)}</instance></data-out></output-events><action><workflow><app-path>${nameNode}/user/${coord:user()}/examples/apps/aggregator</app-path><configuration><property><name>jobTracker</name><value>${jobTracker}</value></property><property><name>nameNode</name><value>${nameNode}</value></property><property><name>queueName</name><value>${queueName}</value></property><property><name>inputData</name><value>${coord:dataIn('input')}</value></property><property><name>outputData</name><value>${coord:dataOut('output')}</value></property></configuration></workflow></action></coordinator-app>");

	}

	@Test(dependsOnMethods = { "testGetEntityDefinition" })
	public void testInvalidGetEntityDefinition() {
		ClientResponse clientRepsonse = this.service
				.path("api/entities/definition/process/sample1")
				.accept(MediaType.TEXT_XML).get(ClientResponse.class);
		Assert.assertEquals(clientRepsonse.getEntity(String.class), "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><result><status>FAILED</status><message>sample1 does not exists</message></result>");

	}
	
	@Test(dependsOnMethods = { "testInvalidGetEntityDefinition" })
	public void testSchedule() {
		ClientResponse clientRepsonse = this.service
				.path("api/entities/schedule/process/aggregator-coord")
				.accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
				.post(ClientResponse.class);
		
		Assert.assertEquals(
				clientRepsonse.getEntity(String.class),
				"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><result><status>SUCCEEDED</status><message>schedule successful</message></result>");	
		
	}
	
	@Test(dependsOnMethods = { "testSchedule" })
	public void testSuspend(){
		ClientResponse clientRepsonse = this.service
				.path("api/entities/suspend/process/aggregator-coord")
				.accept(MediaType.TEXT_XML).post(ClientResponse.class);

		Assert.assertEquals(
				clientRepsonse.getEntity(String.class),
				"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><result><status>SUCCEEDED</status><message>suspend successful</message></result>");	
	}

	@Test(dependsOnMethods = { "testSuspend" })
	public void testResume(){
		ClientResponse clientRepsonse = this.service
				.path("api/entities/resume/process/aggregator-coord")
				.accept(MediaType.TEXT_XML).post(ClientResponse.class);

		Assert.assertEquals(
				clientRepsonse.getEntity(String.class),
				"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><result><status>SUCCEEDED</status><message>resume successful</message></result>");	
	}

	@Test(dependsOnMethods = { "testResume" })
	public void testDelete() {

		this.service
		.path("api/entities/delete/dataset/raw-logs")
		.accept(MediaType.TEXT_XML).delete(ClientResponse.class);

		this.service
		.path("api/entities/delete/dataset/aggregated-logs")
		.accept(MediaType.TEXT_XML).delete(ClientResponse.class);

		ClientResponse clientRepsonse = this.service
				.path("api/entities/delete/process/aggregator-coord")
				.accept(MediaType.TEXT_XML).delete(ClientResponse.class);

		Assert.assertEquals(
				clientRepsonse.getEntity(String.class),
				"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><result><status>SUCCEEDED</status><message>delete successful</message></result>");	

	}


	private static URI getBaseURI() {
		return UriBuilder.fromUri(BASE_URL).build();
	}

	/**
	 * Converts a InputStream into ServletInputStream
	 * 
	 * @param resourceName
	 * @return ServletInputStream
	 */
	private ServletInputStream getServletInputStream(String resourceName) {
		final InputStream stream = this.getClass().getResourceAsStream(
				resourceName);

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
