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

package org.apache.ivory.client;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.servlet.ServletInputStream;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

import org.apache.ivory.resource.APIResult;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;

/**
 * Client API to submit and manage Ivory Entities (Cluster, Feed, Process) jobs
 * against an Ivory instance.
 */
public class IvoryClient {

	private String baseUrl;
	private String version;
	protected static WebResource service;

	private final Map<String, String> headers = new HashMap<String, String>();
	private static JAXBContext jaxbContext;

	static {
		try {
			jaxbContext = JAXBContext.newInstance(APIResult.class);
		} catch (JAXBException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Create a Ivory client instance.
	 * 
	 * @param Ivory
	 *            URL of the server to which client interacts
	 */
	public IvoryClient(String ivoryUrl) {
		this.baseUrl = notEmpty(ivoryUrl, "oozieUrl");
		if (!this.baseUrl.endsWith("/")) {
			this.baseUrl += "/";
		}
		IvoryClient.service = Client.create(new DefaultClientConfig())
				.resource(UriBuilder.fromUri(baseUrl).build());
	}

	/**
	 * Methods allowed on Resources
	 */
	protected static enum Entities {
		VALIDATE("api/entities/validate/", HttpMethod.POST, MediaType.TEXT_XML), SUBMIT(
				"api/entities/submit/", HttpMethod.POST, MediaType.TEXT_XML), SUBMITandSCHEDULE(
				"api/entities/submitAndSchedule/", HttpMethod.POST,
				MediaType.TEXT_XML), SCHEDULE("api/entities/schedule/",
				HttpMethod.POST, MediaType.TEXT_XML), SUSPEND(
				"api/entities/suspend/", HttpMethod.POST, MediaType.TEXT_XML), RESUME(
				"api/entities/resume/", HttpMethod.POST, MediaType.TEXT_XML), DELETE(
				"api/entities/delete/", HttpMethod.DELETE, MediaType.TEXT_XML), STATUS(
				"api/entities/status/", HttpMethod.GET, MediaType.TEXT_PLAIN), DEFINITION(
				"api/entities/definition/", HttpMethod.GET, MediaType.TEXT_XML), ;

		private String path;
		private String method;
		private String mimeType;

		Entities(String path, String method, String mimeType) {
			this.path = path;
			this.method = method;
			this.mimeType = mimeType;
		}
	}

	/**
	 * Set a HTTP header to be used in the WS requests by the workflow instance.
	 * 
	 * @param name
	 *            header name.
	 * @param value
	 *            header value.
	 */
	public void setHeader(String name, String value) {
		headers.put(notEmpty(name, "name"), notNull(value, "value"));
	}

	/**
	 * Get the value of a set HTTP header from the ivory instance.
	 * 
	 * @param name
	 *            header name.
	 * @return header value, <code>null</code> if not set.
	 */
	public String getHeader(String name) {
		return headers.get(notEmpty(name, "name"));
	}

	public String notEmpty(String str, String name) {
		if (str == null) {
			throw new IllegalArgumentException(name + " cannot be null");
		}
		if (str.length() == 0) {
			throw new IllegalArgumentException(name + " cannot be empty");
		}
		return str;
	}

	/**
	 * Check if the object is not null.
	 * 
	 * @param <T>
	 * @param obj
	 * @param name
	 * @return string
	 */
	public static <T> T notNull(T obj, String name) {
		if (obj == null) {
			throw new IllegalArgumentException(name + " cannot be null");
		}
		return obj;
	}

	/**
	 * Return an iterator with all the header names set in the workflow
	 * instance.
	 * 
	 * @return header names.
	 */
	public Iterator<String> getHeaderNames() {
		return Collections.unmodifiableMap(headers).keySet().iterator();
	}

	public String schedule(String entityType, String entityName)
			throws IvoryCLIException {

		return sendRequest(Entities.SCHEDULE, entityType, entityName);

	}

	public String suspend(String entityType, String entityName)
			throws IvoryCLIException {

		return sendRequest(Entities.SUSPEND, entityType, entityName);

	}

	public String resume(String entityType, String entityName)
			throws IvoryCLIException {

		return sendRequest(Entities.RESUME, entityType, entityName);

	}

	public String delete(String entityType, String entityName)
			throws IvoryCLIException {

		return sendRequest(Entities.DELETE, entityType, entityName);

	}

	public String validate(String entityType, String filePath)
			throws IvoryCLIException {
		ServletInputStream entityStream = getServletInputStream(filePath);
		return sendRequestWithObject(Entities.VALIDATE, entityType,
				entityStream);
	}

	public String submit(String entityType, String filePath)
			throws IvoryCLIException {
		ServletInputStream entityStream = getServletInputStream(filePath);
		return sendRequestWithObject(Entities.SUBMIT, entityType, entityStream);
	}

	public String submitAndSchedule(String entityType, String filePath)
			throws IvoryCLIException {
		ServletInputStream entityStream = getServletInputStream(filePath);
		return sendRequestWithObject(Entities.SUBMITandSCHEDULE, entityType,
				entityStream);
	}

	public String getStatus(String entityType, String entityName)
			throws IvoryCLIException {

		return sendRequest(Entities.STATUS, entityType, entityName);

	}

	public String getDefinition(String entityType, String entityName)
			throws IvoryCLIException {

		return sendRequest(Entities.DEFINITION, entityType, entityName);

	}

	/**
	 * Converts a InputStream into ServletInputStream
	 * 
	 * @param filePath
	 * @return ServletInputStream
	 * @throws IvoryCLIException
	 * @throws java.io.IOException
	 */
	private ServletInputStream getServletInputStream(String filePath)
			throws IvoryCLIException {
		ServletInputStream stream = null;
		try {
			stream = getServletInputStream(new FileInputStream(filePath));
		} catch (FileNotFoundException e) {
			throw new IvoryCLIException("File not found:", e);
		} catch (IOException e) {
			throw new IvoryCLIException("Unable to read file: ", e);
		}
		return stream;
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

	private String sendRequest(Entities entities, String entityType,
			String entityName) throws IvoryCLIException {

		ClientResponse clientResponse = service.path(entities.path)
				.path(entityType).path(entityName)
				.header("Remote-User", "testuser").accept(entities.mimeType)
				.type(MediaType.TEXT_XML)
				.method(entities.method, ClientResponse.class);

		if (entities.method == HttpMethod.GET) {
			return parseGetResponse(clientResponse);
		} else {
			return parsePostResponse(clientResponse);
		}

	}

	private String sendRequestWithObject(Entities entities, String entityType,
			Object requestObject) throws IvoryCLIException {

		ClientResponse clientResponse = service.path(entities.path)
				.path(entityType).header("Remote-User", "testuser")
				.accept(entities.mimeType).type(MediaType.TEXT_XML)
				.method(entities.method, ClientResponse.class, requestObject);

		return parsePostResponse(clientResponse);

	}

	private String parsePostResponse(ClientResponse clientResponse)
			throws IvoryCLIException {

		checkIfSuccessfull(clientResponse);
		try {
			APIResult result = (APIResult) jaxbContext.createUnmarshaller()
					.unmarshal(
							new StringReader(clientResponse
									.getEntity(String.class)));
			return result.getMessage();
		} catch (JAXBException e) {
			throw new IvoryCLIException("Unable to read Ivory response: ", e);
		}
	}

	private void checkIfSuccessfull(ClientResponse clientResponse)
			throws IvoryCLIException {
		if (clientResponse.getStatus() == Response.Status.BAD_REQUEST
				.getStatusCode()) {
			throw new IvoryCLIException(clientResponse);
		}

	}

	private String parseGetResponse(ClientResponse clientResponse)
			throws IvoryCLIException {
		checkIfSuccessfull(clientResponse);
		return clientResponse.getEntity(String.class);
	}

	public static void main(String[] args) throws IvoryCLIException {

		// IvoryClient ivoryClient = new IvoryClient("http://localhost:15000");

		// System.out.println(ivoryClient.validate("cluster","/Users/shaik.idris/Work/Tests/entities/corp.xml"));
		// System.out.println(ivoryClient.submit("cluster","/Users/shaik.idris/Work/Tests/entities/corp.xml"));
		// System.out.println(ivoryClient.getDefinition("cluster", "corp"));
		// System.out.println(ivoryClient.getStatus("cluster", "corp"));
		// System.out.println(ivoryClient.submit(
		// "feed","/Users/shaik.idris/Work/Tests/entities/agg-logs.xml"));
		// System.out.println(ivoryClient.schedule("feed", "agg-logs"));
		// System.out.println(ivoryClient.submit("feed","/Users/shaik.idris/Work/Tests/entities/raw-logs.xml"));
		// System.out.println(ivoryClient.submit(
		// "process","/Users/shaik.idris/Work/Tests/entities/agg-coord.xml));
		// System.out.println(ivoryClient.delete("process", "agg-coord"));
		// System.out.println(ivoryClient.delete("feed", "agg-logs"));

	}

}
