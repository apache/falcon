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
import java.util.Properties;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import org.apache.ivory.entity.v0.SchemaHelper;
import org.apache.ivory.resource.APIResult;
import org.apache.ivory.resource.EntityList;
import org.apache.ivory.resource.InstancesResult;

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
	public static final String WS_HEADER_PREFIX = "header:";
	private static final String REMOTE_USER = "Remote-User";

	private static final String USER = System.getProperty("user.name");

	/**
	 * Create a Ivory client instance.
	 * 
	 * @param ivoryUrl
	 *            of the server to which client interacts
	 * @throws IOException
	 */
	public IvoryClient(String ivoryUrl) throws IOException {
		this.baseUrl = notEmpty(ivoryUrl, "IvoryUrl");
		if (!this.baseUrl.endsWith("/")) {
			this.baseUrl += "/";
		}
		Client client = Client.create(new DefaultClientConfig());
		setIvoryTimeOut(client);
		IvoryClient.service = client.resource(UriBuilder.fromUri(baseUrl)
				.build());
		client.resource(UriBuilder.fromUri(baseUrl).build());

		// addHeaders();
	}

	private void setIvoryTimeOut(Client client) throws IOException {
		Properties prop = new Properties();
		InputStream input = IvoryClient.class
				.getResourceAsStream("/client.properties");
		int readTimeout = 0;
		int connectTimeout = 0;
		if (input != null) {
			prop.load(input);
			readTimeout = prop.containsKey("ivory.read.timeout") ? Integer
					.parseInt(prop.getProperty("ivory.read.timeout")) : 180000;
			connectTimeout = prop.containsKey("ivory.connect.timeout") ? Integer
					.parseInt(prop.getProperty("ivory.connect.timeout"))
					: 180000;
		} else {
			readTimeout = 180000;
			connectTimeout = 180000;
		}
		client.setConnectTimeout(connectTimeout);
		client.setReadTimeout(readTimeout);
	}

	/**
	 * Methods allowed on Entity Resources
	 */
	protected static enum Entities {
		VALIDATE("api/entities/validate/", HttpMethod.POST, MediaType.TEXT_XML), SUBMIT(
				"api/entities/submit/", HttpMethod.POST, MediaType.TEXT_XML), UPDATE(
				"api/entities/update/", HttpMethod.POST, MediaType.TEXT_XML), SUBMITandSCHEDULE(
				"api/entities/submitAndSchedule/", HttpMethod.POST,
				MediaType.TEXT_XML), SCHEDULE("api/entities/schedule/",
				HttpMethod.POST, MediaType.TEXT_XML), SUSPEND(
				"api/entities/suspend/", HttpMethod.POST, MediaType.TEXT_XML), RESUME(
				"api/entities/resume/", HttpMethod.POST, MediaType.TEXT_XML), DELETE(
				"api/entities/delete/", HttpMethod.DELETE, MediaType.TEXT_XML), STATUS(
				"api/entities/status/", HttpMethod.GET, MediaType.TEXT_XML), DEFINITION(
				"api/entities/definition/", HttpMethod.GET, MediaType.TEXT_XML), LIST(
				"api/entities/list/", HttpMethod.GET, MediaType.TEXT_XML), DEPENDENCY(
				"api/entities/dependencies/", HttpMethod.GET,
				MediaType.TEXT_XML);

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
	 * Methods allowed on Process Instance Resources
	 */
	protected static enum Instances {
		RUNNING("api/instance/running/", HttpMethod.GET,
				MediaType.APPLICATION_JSON), STATUS("api/instance/status/",
				HttpMethod.GET, MediaType.APPLICATION_JSON), KILL(
				"api/instance/kill/", HttpMethod.POST,
				MediaType.APPLICATION_JSON), SUSPEND("api/instance/suspend/",
				HttpMethod.POST, MediaType.APPLICATION_JSON), RESUME(
				"api/instance/resume/", HttpMethod.POST,
				MediaType.APPLICATION_JSON), RERUN("api/instance/rerun/",
				HttpMethod.POST, MediaType.APPLICATION_JSON) ,LOG("api/instance/logs/",
				HttpMethod.GET, MediaType.APPLICATION_JSON);
		private String path;
		private String method;
		private String mimeType;

		Instances(String path, String method, String mimeType) {
			this.path = path;
			this.method = method;
			this.mimeType = mimeType;
		}
	}
	
	protected static enum AdminOperations {
		
		STACK("api/admin/stack", HttpMethod.GET,
				MediaType.TEXT_PLAIN), 
		VERSION("api/admin/version", HttpMethod.GET,
				MediaType.TEXT_PLAIN);
		private String path;
		private String method;
		private String mimeType;
		
		AdminOperations(String path, String method, String mimeType) {
			this.path = path;
			this.method = method;
			this.mimeType = mimeType;
		}
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

	public String schedule(String entityType, String entityName, String colo)
			throws IvoryCLIException {

		return sendEntityRequest(Entities.SCHEDULE, entityType, entityName,
				colo);

	}

	public String suspend(String entityType, String entityName, String colo)
			throws IvoryCLIException {

		return sendEntityRequest(Entities.SUSPEND, entityType, entityName, colo);

	}

	public String resume(String entityType, String entityName, String colo)
			throws IvoryCLIException {

		return sendEntityRequest(Entities.RESUME, entityType, entityName, colo);

	}

	public String delete(String entityType, String entityName)
			throws IvoryCLIException {

		return sendEntityRequest(Entities.DELETE, entityType, entityName, null);

	}

	public String validate(String entityType, String filePath)
			throws IvoryCLIException {
		InputStream entityStream = getServletInputStream(filePath);
		return sendEntityRequestWithObject(Entities.VALIDATE, entityType,
				entityStream, null);
	}

	public String submit(String entityType, String filePath)
			throws IvoryCLIException {
		InputStream entityStream = getServletInputStream(filePath);
		return sendEntityRequestWithObject(Entities.SUBMIT, entityType,
				entityStream, null);
	}

	public String update(String entityType, String entityName, String filePath) 
			throws IvoryCLIException {
		InputStream entityStream = getServletInputStream(filePath);
		return sendEntityRequestWithNameAndObject(Entities.UPDATE, entityType,
				entityName, entityStream);
	}

	public String submitAndSchedule(String entityType, String filePath) 
			throws IvoryCLIException {
		InputStream entityStream = getServletInputStream(filePath);
		return sendEntityRequestWithObject(Entities.SUBMITandSCHEDULE,
				entityType, entityStream, null);
	}

	public String getStatus(String entityType, String entityName, String colo)
			throws IvoryCLIException {

		return sendEntityRequest(Entities.STATUS, entityType, entityName, colo);

	}

	public String getDefinition(String entityType, String entityName)
			throws IvoryCLIException {

		return sendDefinitionRequest(Entities.DEFINITION, entityType,
				entityName);

	}

	public String getDependency(String entityType, String entityName)
			throws IvoryCLIException {
		return sendDependencyRequest(Entities.DEPENDENCY, entityType,
				entityName);
	}

	public String getEntityList(String entityType) throws IvoryCLIException {
		return sendListRequest(Entities.LIST, entityType);
	}

	public String getRunningInstances(String type, String entity, String colo)
			throws IvoryCLIException {

		return sendInstanceRequest(Instances.RUNNING, type, entity, null, null,
				null, null, colo);
	}

	public String getStatusOfInstances(String type, String entity,
			String start, String end, String runid, String colo)
			throws IvoryCLIException {

		return sendInstanceRequest(Instances.STATUS, type, entity, start, end,
				null, null, colo);
	}

	public String killInstances(String type, String entity, String start,
			String end, String colo) throws IvoryCLIException {

		return sendInstanceRequest(Instances.KILL, type, entity, start, end,
				null, null, colo);
	}

	public String suspendInstances(String type, String entity, String start,
			String end, String colo) throws IvoryCLIException {

		return sendInstanceRequest(Instances.SUSPEND, type, entity, start, end,
				null, null, colo);
	}

	public String resumeInstances(String type, String entity, String start,
			String end, String colo) throws IvoryCLIException {

		return sendInstanceRequest(Instances.RESUME, type, entity, start, end,
				null, null, colo);
	}

	public String rerunInstances(String type, String entity, String start,
			String end, String filePath, String colo) throws IvoryCLIException {

		return sendInstanceRequest(Instances.RERUN, type, entity, start, end,
				getServletInputStream(filePath), null, colo);
	}
	
	public String getLogsOfInstances(String type, String entity, String start,
			String end, String colo, String runId) throws IvoryCLIException {

		return sendInstanceRequest(Instances.LOG, type, entity, start, end,
				null, runId, colo);
	}
	
	public String getThreadDump() throws IvoryCLIException{
		return sendAdminRequest(AdminOperations.STACK);
	}
	
	public String getVersion() throws IvoryCLIException{
		return sendAdminRequest(AdminOperations.VERSION);
	}

	/**
	 * Converts a InputStream into ServletInputStream
	 * 
	 * @param filePath
	 * @return ServletInputStream
	 * @throws IvoryCLIException
	 * @throws java.io.IOException
	 */
	private InputStream getServletInputStream(String filePath)
			throws IvoryCLIException {
		if (filePath == null) {
			return null;
		}
		InputStream stream = null;
		try {
			stream = new FileInputStream(filePath);
		} catch (FileNotFoundException e) {
			throw new IvoryCLIException("File not found:", e);
		} catch (IOException e) {
			throw new IvoryCLIException("Unable to read file: ", e);
		}
		return stream;
	}

	// private ServletInputStream getServletInputStream(final InputStream
	// stream)
	// throws IOException {
	// return new ServletInputStream() {
	//
	// @Override
	// public int read() throws IOException {
	// return stream.read();
	// }
	// };
	// }

	private String sendEntityRequest(Entities entities, String entityType,
			String entityName, String colo) throws IvoryCLIException {
		
		WebResource resource = service.path(entities.path)
				.path(entityType).path(entityName);
		if (colo != null) {
			resource = resource.queryParam("colo", colo);
		}
		ClientResponse clientResponse = resource.header(REMOTE_USER, USER)
				.accept(entities.mimeType).type(MediaType.TEXT_XML)
				.method(entities.method, ClientResponse.class);

		checkIfSuccessfull(clientResponse);

		return parseAPIResult(clientResponse);
	}

	private String sendDefinitionRequest(Entities entities, String entityType,
			String entityName) throws IvoryCLIException {

		ClientResponse clientResponse = service.path(entities.path)
				.path(entityType).path(entityName).header(REMOTE_USER, USER)
				.accept(entities.mimeType).type(MediaType.TEXT_XML)
				.method(entities.method, ClientResponse.class);

		checkIfSuccessfull(clientResponse);
		return clientResponse.getEntity(String.class);
	}

	private String sendDependencyRequest(Entities entities, String entityType,
			String entityName) throws IvoryCLIException {

		ClientResponse clientResponse = service.path(entities.path)
				.path(entityType).path(entityName).header(REMOTE_USER, USER)
				.accept(entities.mimeType).type(MediaType.TEXT_XML)
				.method(entities.method, ClientResponse.class);

		checkIfSuccessfull(clientResponse);

		return parseEntityList(clientResponse);

	}

	private String sendListRequest(Entities entities, String entityType)
			throws IvoryCLIException {

		ClientResponse clientResponse = service.path(entities.path)
				.path(entityType).header(REMOTE_USER, USER)
				.accept(entities.mimeType).type(MediaType.TEXT_XML)
				.method(entities.method, ClientResponse.class);

		checkIfSuccessfull(clientResponse);

		return parseEntityList(clientResponse);

	}

	private String sendEntityRequestWithObject(Entities entities,
			String entityType, Object requestObject, String colo) throws IvoryCLIException {
		
		WebResource resource = service.path(entities.path)
				.path(entityType);
		if (colo != null) {
			resource = resource.queryParam("colo", colo);
		}
		ClientResponse clientResponse = resource.header(REMOTE_USER, USER)
				.accept(entities.mimeType).type(MediaType.TEXT_XML)
				.method(entities.method, ClientResponse.class, requestObject);

		checkIfSuccessfull(clientResponse);

		return parseAPIResult(clientResponse);

	}

	private String sendEntityRequestWithNameAndObject(Entities entities,
			String entityType, String entityName, Object requestObject)
			throws IvoryCLIException {
		
		ClientResponse clientResponse = service.path(entities.path)
				.path(entityType).path(entityName).header(REMOTE_USER, USER)
				.accept(entities.mimeType).type(MediaType.TEXT_XML)
				.method(entities.method, ClientResponse.class, requestObject);

		checkIfSuccessfull(clientResponse);

		return parseAPIResult(clientResponse);

	}

	private String sendInstanceRequest(Instances instances, String type,
			String entity, String start, String end, InputStream props,
			String runid, String colo) throws IvoryCLIException {
		WebResource resource = service.path(instances.path).path(type)
				.path(entity);
		if (start != null) {
			resource = resource.queryParam("start", start);
		}
		if (end != null) {
			resource = resource.queryParam("end", end);
		}
		if (runid != null) {
			resource = resource.queryParam("runid", runid);
		}
		if (colo != null) {
			resource = resource.queryParam("colo", colo);
		}
		

		ClientResponse clientResponse = null;
		if (props == null) {
			clientResponse = resource.header(REMOTE_USER, USER)
					.accept(instances.mimeType)
					.method(instances.method, ClientResponse.class);
		} else {
			clientResponse = resource.header(REMOTE_USER, USER)
					.accept(instances.mimeType)
					.method(instances.method, ClientResponse.class, props);
		}
		checkIfSuccessfull(clientResponse);
		
		if(instances.name().equals("LOG"))
			return parseProcessInstanceResultLogs(clientResponse, runid);
		else
			return parseProcessInstanceResult(clientResponse);

	}
	
	private String sendAdminRequest(AdminOperations job) 
			throws IvoryCLIException {
		
		ClientResponse clientResponse = service.path(job.path)
				.header(REMOTE_USER, USER).accept(job.mimeType)
				.type(MediaType.TEXT_PLAIN).method(job.method, ClientResponse.class);
		return parseStringResult(clientResponse);	
	}
	
	private String parseAPIResult(ClientResponse clientResponse)
			throws IvoryCLIException {

		APIResult result = clientResponse.getEntity(APIResult.class);
		return result.getMessage();

	}

	private String parseEntityList(ClientResponse clientResponse)
			throws IvoryCLIException {

		EntityList result = clientResponse.getEntity(EntityList.class);
		if (result == null) {
			return "";
		}
		return result.toString();

	}

	private String parseStringResult(ClientResponse clientResponse)
			throws IvoryCLIException {

		return clientResponse.getEntity(String.class);
	}

	private String parseProcessInstanceResult(ClientResponse clientResponse) {
		InstancesResult result = clientResponse
				.getEntity(InstancesResult.class);
		StringBuffer sb = new StringBuffer();
		String toAppend = null;
		
		sb.append("Consolidated Status: " + result.getStatus() + "\n");
		
		sb.append("\nInstances:\n");
		sb.append("Instance\t\tCluster\t\tSourceCluster\t\tStatus\t\tStart\t\tEnd\t\tDetails\t\t\t\t\tLog\n");
		sb.append("----------------------------------------------------------------------------------------------------------------------------------------\n");
		if(result.getInstances() != null){
			for (InstancesResult.Instance instance : result.getInstances()) {
				
				toAppend = instance.getInstance() != null  ? instance.getInstance() : "-";
				sb.append(toAppend + "\t");
				
				toAppend = instance.getCluster() != null ? instance.getCluster() : "-";
				sb.append(toAppend + "\t");
				
				toAppend = instance.getSourceCluster() != null ? instance.getSourceCluster() : "-";
				sb.append(toAppend + "\t");	
				
				toAppend =  (instance.getStatus() != null ? instance.getStatus().toString() : "-");
				sb.append(toAppend + "\t");	
				
				toAppend = instance.getStartTime() != null ? SchemaHelper.formatDateUTC(instance.getStartTime()) : "-";
				sb.append(toAppend + "\t");	
				
				toAppend = instance.getEndTime() != null ? SchemaHelper.formatDateUTC(instance.getEndTime()) : "-";
				sb.append(toAppend + "\t");
				
				toAppend = (instance.getDetails() != null && !instance.getDetails().equals("")) ? instance.getDetails() : "-";
				sb.append(toAppend + "\t");
				
				toAppend = instance.getLogFile() != null ? instance.getLogFile() : "-";
				sb.append(toAppend + "\n");	
				
			}
		}
		sb.append("\nAdditional Information:\n");
		sb.append("Response: " + result.getMessage());
		sb.append("Request Id: " + result.getRequestId() );
		return sb.toString();
	}
	
	private String parseProcessInstanceResultLogs(ClientResponse clientResponse, String runid) {
		InstancesResult result = clientResponse
				.getEntity(InstancesResult.class);
		StringBuffer sb = new StringBuffer();
		String toAppend = null;
		
		sb.append("Consolidated Status: " + result.getStatus() + "\n");
		
		sb.append("\nInstances:\n");
		sb.append("Instance\t\tCluster\t\tSourceCluster\t\tStatus\t\tRunID\t\t\tLog\n");
		sb.append("----------------------------------------------------------------------------------------------------\n");
		if(result.getInstances() != null){
			for (InstancesResult.Instance instance : result.getInstances()) {
				
				toAppend = (instance.getInstance() != null ) ? instance.getInstance() : "-";
				sb.append(toAppend + "\t");
				
				toAppend = instance.getCluster() != null ? instance.getCluster() : "-";
				sb.append(toAppend + "\t");
				
				toAppend = instance.getSourceCluster() != null ? instance.getSourceCluster() : "-";
				sb.append(toAppend + "\t");	
				
				toAppend =  (instance.getStatus() != null ? instance.getStatus().toString() : "-");
				sb.append(toAppend + "\t");	
				
				toAppend =  (runid != null ? runid : "latest");
				sb.append(toAppend + "\t");	
				
				toAppend = instance.getLogFile() != null ? instance.getLogFile() : "-";
				sb.append(toAppend + "\n");	
				
				
				if (instance.actions != null) {
					sb.append("actions:\n");
					for (InstancesResult.InstanceAction action : instance.actions) {
						sb.append("    ").append(action.getAction()+ "\t" + action.getStatus() + "\t" + action.getLogFile()).append("\n");
					}
				}
			}
		}
		sb.append("\nAdditional Information:\n");
		sb.append("Response: " + result.getMessage());
		sb.append("Request Id: " + result.getRequestId() );
		return sb.toString();
	}
	private void checkIfSuccessfull(ClientResponse clientResponse)
			throws IvoryCLIException {
		if (clientResponse.getStatus() == Response.Status.BAD_REQUEST
				.getStatusCode()) {
			throw IvoryCLIException.fromReponse(clientResponse);
		}

	}

}
