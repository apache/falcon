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

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import javax.servlet.ServletInputStream;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

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

	@BeforeClass
	public void configure() {
		ClientConfig config = new DefaultClientConfig();
		Client client = Client.create(config);
		service = client.resource(getBaseURI());
	}

	/**
	 * Tests should be enabled only in local environments as they need running
	 * instance of webserver
	 */
	@Test(enabled = false)
	public void testStatus() {
		// http://localhost:15000/api/entities/status/hello/1
		System.out.println(service.path("api/entities/status/hello/1")
				.accept(MediaType.TEXT_PLAIN).get(String.class));
	}

	@Test(enabled = false)
	public void testValidate() {

		ServletInputStream stream = getServletInputStream(SAMPLE_PROCESS_XML);

		ClientResponse clientRepsonse = service
				.path("api/entities/validate/process")
				.accept(MediaType.APPLICATION_XML).type(MediaType.TEXT_XML)
				.post(ClientResponse.class, stream);

		System.out.println(clientRepsonse.getEntity(String.class));

	}

	private static URI getBaseURI() {
		return UriBuilder.fromUri("http://localhost:15000/").build();
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

}
