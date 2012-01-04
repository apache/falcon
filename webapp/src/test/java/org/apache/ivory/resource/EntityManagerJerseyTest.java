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

	private static final String SAMPLE_PROCESS_XML = "/process-version-0.xml";
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
				"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><result><status>SUCCEEDED</status><message>Validate successful</message></result>");

	}
	
	@Test(dependsOnMethods = { "testValidate" })
	public void testSubmit() {
		ServletInputStream stream = getServletInputStream(SAMPLE_PROCESS_XML);

		ClientResponse clientRepsonse = this.service
				.path("api/entities/submit/process")
				.accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
				.post(ClientResponse.class, stream);

		Assert.assertEquals(
				clientRepsonse.getEntity(String.class),
				"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><result><status>SUCCEEDED</status><message>Submit successful</message></result>");
	}

	@Test(dependsOnMethods = { "testSubmit" })
	public void testGetEntityDefinition() {
		ClientResponse clientRepsonse = this.service
				.path("api/entities/definition/process/sample")
				.accept(MediaType.TEXT_XML).get(ClientResponse.class);

		Assert.assertEquals(clientRepsonse.getEntity(String.class),
				"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><process name=\"sample\"><name>sample</name><clusters><cluster name=\"prod-red\"></cluster></clusters><concurrency>1</concurrency><execution>LIFO</execution><frequency>hourly</frequency><periodicity>1</periodicity><validity start=\"2011-11-01 00:00:00\" end=\"9999-12-31 23:59:00\" timezone=\"UTC\"></validity><inputs><input name=\"impression\" feed=\"impression\" start-instance=\"$ptime-6\" end-instance=\"$ptime\"></input><input name=\"clicks\" feed=\"clicks\" start-instance=\"$ptime\" end-instance=\"$ptime\"></input></inputs><outputs><output name=\"impOutput\" feed=\"imp-click-join\" instance=\"$ptime\"></output><output name=\"clicksOutput\" feed=\"imp-click-join1\" instance=\"$ptime\"></output></outputs><properties><property name=\"name\" value=\"value\"></property><property name=\"name\" value=\"value\"></property></properties><workflow engine=\"oozie\" path=\"hdfs://path/to/workflow\" libpath=\"hdfs://path/to/workflow/lib\"></workflow><retry policy=\"backoff\" delay=\"10\" delayUnit=\"min\" attempts=\"3\"></retry><late-process policy=\"exp-backoff\" delay=\"1\" delayUnit=\"hour\"><late-input feed=\"impression\" workflow-path=\"hdfs://impression/late/workflow\"></late-input><late-input feed=\"clicks\" workflow-path=\"hdfs://clicks/late/workflow\"></late-input></late-process></process>");


	}
	
	@Test(dependsOnMethods = { "testGetEntityDefinition" })
	public void testDelete() {
		ClientResponse clientRepsonse = this.service
				.path("api/entities/delete/process/sample")
				.accept(MediaType.TEXT_XML).delete(ClientResponse.class);

		Assert.assertEquals(
				clientRepsonse.getEntity(String.class),
				"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><result><status>SUCCEEDED</status><message>Delete successful</message></result>");
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
