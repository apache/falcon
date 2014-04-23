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

package org.apache.falcon.client;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.apache.commons.io.IOUtils;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.EntityList;
import org.apache.falcon.resource.InstancesResult;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.falcon.resource.InstancesSummaryResult;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

/**
 * Client API to submit and manage Falcon Entities (Cluster, Feed, Process) jobs
 * against an Falcon instance.
 */
public class FalconClient {

    public static final String WS_HEADER_PREFIX = "header:";
    public static final String USER = System.getProperty("user.name");
    public static final String AUTH_URL = "api/options?" + PseudoAuthenticator.USER_NAME + "=" + USER;

    private static final String FALCON_INSTANCE_ACTION_CLUSTERS = "falcon.instance.action.clusters";
    private static final String FALCON_INSTANCE_SOURCE_CLUSTERS = "falcon.instance.source.clusters";

    /**
     * Name of the HTTP cookie used for the authentication token between the client and the server.
     */
    public static final String AUTH_COOKIE = "hadoop.auth";
    private static final String AUTH_COOKIE_EQ = AUTH_COOKIE + "=";
    private static final KerberosAuthenticator AUTHENTICATOR = new KerberosAuthenticator();

    private final WebResource service;
    private final AuthenticatedURL.Token authenticationToken;

    /**
     * Create a Falcon client instance.
     *
     * @param falconUrl of the server to which client interacts
     * @throws FalconCLIException
     */
    public FalconClient(String falconUrl) throws FalconCLIException {
        String baseUrl = notEmpty(falconUrl, "FalconUrl");
        if (!baseUrl.endsWith("/")) {
            baseUrl += "/";
        }

        Client client = Client.create(new DefaultClientConfig());
        setFalconTimeOut(client);
        service = client.resource(UriBuilder.fromUri(baseUrl).build());

        authenticationToken = getToken(baseUrl);
    }

    private void setFalconTimeOut(Client client) throws FalconCLIException {
        try {
            Properties prop = new Properties();
            int readTimeout;
            int connectTimeout;
            InputStream inputStream = null;
            try {
                inputStream = FalconClient.class.getResourceAsStream("/client.properties");
                if (inputStream != null) {
                    prop.load(inputStream);
                    readTimeout = prop.containsKey("falcon.read.timeout") ? Integer
                            .parseInt(prop.getProperty("falcon.read.timeout")) : 180000;
                    connectTimeout = prop.containsKey("falcon.connect.timeout") ? Integer
                            .parseInt(prop.getProperty("falcon.connect.timeout"))
                            : 180000;
                } else {
                    readTimeout = 180000;
                    connectTimeout = 180000;
                }
            } finally {
                IOUtils.closeQuietly(inputStream);
            }
            client.setConnectTimeout(connectTimeout);
            client.setReadTimeout(readTimeout);
        } catch (IOException e) {
            throw new FalconCLIException("An error occurred while reading client.properties file.", e);
        }
    }

    public static AuthenticatedURL.Token getToken(String baseUrl) throws FalconCLIException {
        AuthenticatedURL.Token currentToken = new AuthenticatedURL.Token();
        try {
            URL url = new URL(baseUrl + AUTH_URL);
            // using KerberosAuthenticator which falls back to PsuedoAuthenticator
            // instead of passing authentication type from the command line - bad factory
            new AuthenticatedURL(AUTHENTICATOR).openConnection(url, currentToken);
        } catch (Exception ex) {
            throw new FalconCLIException("Could not authenticate, " + ex.getMessage(), ex);
        }

        return currentToken;
    }

    /**
     * Methods allowed on Entity Resources.
     */
    protected static enum Entities {
        VALIDATE("api/entities/validate/", HttpMethod.POST, MediaType.TEXT_XML),
        SUBMIT("api/entities/submit/", HttpMethod.POST, MediaType.TEXT_XML),
        UPDATE("api/entities/update/", HttpMethod.POST, MediaType.TEXT_XML),
        SUBMITandSCHEDULE("api/entities/submitAndSchedule/", HttpMethod.POST, MediaType.TEXT_XML),
        SCHEDULE("api/entities/schedule/", HttpMethod.POST, MediaType.TEXT_XML),
        SUSPEND("api/entities/suspend/", HttpMethod.POST, MediaType.TEXT_XML),
        RESUME("api/entities/resume/", HttpMethod.POST, MediaType.TEXT_XML),
        DELETE("api/entities/delete/", HttpMethod.DELETE, MediaType.TEXT_XML),
        STATUS("api/entities/status/", HttpMethod.GET, MediaType.TEXT_XML),
        DEFINITION("api/entities/definition/", HttpMethod.GET, MediaType.TEXT_XML),
        LIST("api/entities/list/", HttpMethod.GET, MediaType.TEXT_XML),
        DEPENDENCY("api/entities/dependencies/", HttpMethod.GET, MediaType.TEXT_XML);

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
     * Methods allowed on Process Instance Resources.
     */
    protected static enum Instances {
        RUNNING("api/instance/running/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        STATUS("api/instance/status/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        KILL("api/instance/kill/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        SUSPEND("api/instance/suspend/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        RESUME("api/instance/resume/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        RERUN("api/instance/rerun/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        LOG("api/instance/logs/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        SUMMARY("api/instance/summary/", HttpMethod.GET, MediaType.APPLICATION_JSON);

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

        STACK("api/admin/stack", HttpMethod.GET, MediaType.TEXT_PLAIN),
        VERSION("api/admin/version", HttpMethod.GET, MediaType.APPLICATION_JSON);

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

    public String schedule(String entityType, String entityName, String colo)
        throws FalconCLIException {

        return sendEntityRequest(Entities.SCHEDULE, entityType, entityName,
                colo);

    }

    public String suspend(String entityType, String entityName, String colo)
        throws FalconCLIException {

        return sendEntityRequest(Entities.SUSPEND, entityType, entityName, colo);

    }

    public String resume(String entityType, String entityName, String colo)
        throws FalconCLIException {

        return sendEntityRequest(Entities.RESUME, entityType, entityName, colo);

    }

    public String delete(String entityType, String entityName)
        throws FalconCLIException {

        return sendEntityRequest(Entities.DELETE, entityType, entityName, null);

    }

    public String validate(String entityType, String filePath)
        throws FalconCLIException {

        InputStream entityStream = getServletInputStream(filePath);
        return sendEntityRequestWithObject(Entities.VALIDATE, entityType,
                entityStream, null);
    }

    public String submit(String entityType, String filePath)
        throws FalconCLIException {

        InputStream entityStream = getServletInputStream(filePath);
        return sendEntityRequestWithObject(Entities.SUBMIT, entityType,
                entityStream, null);
    }

    public String update(String entityType, String entityName, String filePath, Date effectiveTime)
        throws FalconCLIException {
        InputStream entityStream = getServletInputStream(filePath);
        Entities operation = Entities.UPDATE;
        WebResource resource = service.path(operation.path).path(entityType).path(entityName);
        if (effectiveTime != null) {
            resource = resource.queryParam("effective", SchemaHelper.formatDateUTC(effectiveTime));
        }
        ClientResponse clientResponse = resource
                .header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                .accept(operation.mimeType).type(MediaType.TEXT_XML)
                .method(operation.method, ClientResponse.class, entityStream);
        checkIfSuccessful(clientResponse);
        return parseAPIResult(clientResponse);
    }

    public String submitAndSchedule(String entityType, String filePath)
        throws FalconCLIException {

        InputStream entityStream = getServletInputStream(filePath);
        return sendEntityRequestWithObject(Entities.SUBMITandSCHEDULE,
                entityType, entityStream, null);
    }

    public String getStatus(String entityType, String entityName, String colo)
        throws FalconCLIException {

        return sendEntityRequest(Entities.STATUS, entityType, entityName, colo);
    }

    public String getDefinition(String entityType, String entityName)
        throws FalconCLIException {

        return sendDefinitionRequest(Entities.DEFINITION, entityType,
                entityName);
    }

    public EntityList getDependency(String entityType, String entityName)
        throws FalconCLIException {
        return sendDependencyRequest(Entities.DEPENDENCY, entityType, entityName);
    }

    public EntityList getEntityList(String entityType) throws FalconCLIException {
        return sendListRequest(Entities.LIST, entityType);
    }

    public String getRunningInstances(String type, String entity, String colo)
        throws FalconCLIException {

        return sendInstanceRequest(Instances.RUNNING, type, entity, null, null,
                null, null, colo);
    }

    public String getStatusOfInstances(String type, String entity,
                                       String start, String end,
                                       String colo) throws FalconCLIException {

        return sendInstanceRequest(Instances.STATUS, type, entity, start, end,
                null, null, colo);
    }

    public String getSummaryOfInstances(String type, String entity,
                                       String start, String end,
                                       String colo) throws FalconCLIException {

        return sendInstanceRequest(Instances.SUMMARY, type, entity, start, end,
                null, null, colo);
    }

    public String killInstances(String type, String entity, String start,
                                String end, String colo, String clusters,
                                String sourceClusters)
        throws FalconCLIException, UnsupportedEncodingException {

        return sendInstanceRequest(Instances.KILL, type, entity, start, end,
                getServletInputStream(clusters, sourceClusters, null), null, colo);
    }

    public String suspendInstances(String type, String entity, String start,
                                   String end, String colo, String clusters,
                                   String sourceClusters)
        throws FalconCLIException, UnsupportedEncodingException {

        return sendInstanceRequest(Instances.SUSPEND, type, entity, start, end,
                getServletInputStream(clusters, sourceClusters, null), null, colo);
    }

    public String resumeInstances(String type, String entity, String start,
                                  String end, String colo, String clusters,
                                  String sourceClusters)
        throws FalconCLIException, UnsupportedEncodingException {

        return sendInstanceRequest(Instances.RESUME, type, entity, start, end,
                getServletInputStream(clusters, sourceClusters, null), null, colo);
    }

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    public String rerunInstances(String type, String entity, String start,
                                 String end, String filePath, String colo,
                                 String clusters, String sourceClusters)
        throws FalconCLIException, IOException {

        StringBuilder buffer = new StringBuilder();
        if (filePath != null) {
            BufferedReader in = null;
            try {
                in = new BufferedReader(new FileReader(filePath));

                String str;
                while ((str = in.readLine()) != null) {
                    buffer.append(str).append("\n");
                }
            } finally {
                IOUtils.closeQuietly(in);
            }
        }
        String temp = (buffer.length() == 0) ? null : buffer.toString();
        return sendInstanceRequest(Instances.RERUN, type, entity, start, end,
                getServletInputStream(clusters, sourceClusters, temp), null, colo);
    }
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    public String rerunInstances(String type, String entity, String start,
                                 String end, String colo, String clusters, String sourceClusters)
        throws FalconCLIException, UnsupportedEncodingException {

        return sendInstanceRequest(Instances.RERUN, type, entity, start, end,
                getServletInputStream(clusters, sourceClusters, "oozie.wf.rerun.failnodes=true\n"), null, colo);
    }

    public String getLogsOfInstances(String type, String entity, String start,
                                     String end, String colo, String runId)
        throws FalconCLIException {

        return sendInstanceRequest(Instances.LOG, type, entity, start, end,
                null, runId, colo);
    }

    public String getThreadDump() throws FalconCLIException {
        return sendAdminRequest(AdminOperations.STACK);
    }

    public String getVersion() throws FalconCLIException {
        return sendAdminRequest(AdminOperations.VERSION);
    }

    public int getStatus() throws FalconCLIException {
        AdminOperations job =  AdminOperations.VERSION;
        ClientResponse clientResponse = service.path(job.path)
                .header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                .accept(job.mimeType).type(MediaType.TEXT_PLAIN)
                .method(job.method, ClientResponse.class);
        return clientResponse.getStatus();
    }

    /**
     * Converts a InputStream into ServletInputStream.
     *
     * @param filePath - Path of file to stream
     * @return ServletInputStream
     * @throws FalconCLIException
     */
    private InputStream getServletInputStream(String filePath)
        throws FalconCLIException {

        if (filePath == null) {
            return null;
        }
        InputStream stream;
        try {
            stream = new FileInputStream(filePath);
        } catch (FileNotFoundException e) {
            throw new FalconCLIException("File not found:", e);
        }
        return stream;
    }

    private InputStream getServletInputStream(String clusters,
                                              String sourceClusters, String properties)
        throws FalconCLIException, UnsupportedEncodingException {

        InputStream stream;
        StringBuilder buffer = new StringBuilder();
        if (clusters != null) {
            buffer.append(FALCON_INSTANCE_ACTION_CLUSTERS).append('=').append(clusters).append('\n');
        }
        if (sourceClusters != null) {
            buffer.append(FALCON_INSTANCE_SOURCE_CLUSTERS).append('=').append(sourceClusters).append('\n');
        }
        if (properties != null) {
            buffer.append(properties);
        }
        stream = new ByteArrayInputStream(buffer.toString().getBytes());
        return (buffer.length() == 0) ? null : stream;
    }

    private String sendEntityRequest(Entities entities, String entityType,
                                     String entityName, String colo) throws FalconCLIException {

        WebResource resource = service.path(entities.path)
                .path(entityType).path(entityName);
        if (colo != null) {
            resource = resource.queryParam("colo", colo);
        }
        ClientResponse clientResponse = resource
                .header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                .accept(entities.mimeType).type(MediaType.TEXT_XML)
                .method(entities.method, ClientResponse.class);

        checkIfSuccessful(clientResponse);

        return parseAPIResult(clientResponse);
    }

    private String sendDefinitionRequest(Entities entities, String entityType,
                                         String entityName) throws FalconCLIException {

        ClientResponse clientResponse = service
                .path(entities.path).path(entityType).path(entityName)
                .header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                .accept(entities.mimeType).type(MediaType.TEXT_XML)
                .method(entities.method, ClientResponse.class);

        checkIfSuccessful(clientResponse);
        return clientResponse.getEntity(String.class);
    }

    private EntityList sendDependencyRequest(Entities entities, String entityType,
                                         String entityName) throws FalconCLIException {

        ClientResponse clientResponse = service
                .path(entities.path).path(entityType).path(entityName)
                .header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                .accept(entities.mimeType).type(MediaType.TEXT_XML)
                .method(entities.method, ClientResponse.class);

        checkIfSuccessful(clientResponse);

        return parseEntityList(clientResponse);
    }

    private EntityList sendListRequest(Entities entities, String entityType)
        throws FalconCLIException {

        ClientResponse clientResponse = service
                .path(entities.path).path(entityType)
                .header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                .accept(entities.mimeType).type(MediaType.TEXT_XML)
                .method(entities.method, ClientResponse.class);

        checkIfSuccessful(clientResponse);

        return parseEntityList(clientResponse);
    }

    private String sendEntityRequestWithObject(Entities entities, String entityType,
                                               Object requestObject, String colo) throws FalconCLIException {
        WebResource resource = service.path(entities.path)
                .path(entityType);
        if (colo != null) {
            resource = resource.queryParam("colo", colo);
        }
        ClientResponse clientResponse = resource
                .header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                .accept(entities.mimeType).type(MediaType.TEXT_XML)
                .method(entities.method, ClientResponse.class, requestObject);

        checkIfSuccessful(clientResponse);

        return parseAPIResult(clientResponse);
    }

    //SUSPEND CHECKSTYLE CHECK VisibilityModifierCheck
    private String sendInstanceRequest(Instances instances, String type,
                                       String entity, String start, String end, InputStream props,
                                       String runid, String colo) throws FalconCLIException {
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

        ClientResponse clientResponse;
        if (props == null) {
            clientResponse = resource
                    .header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                    .accept(instances.mimeType)
                    .method(instances.method, ClientResponse.class);
        } else {
            clientResponse = resource
                    .header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                    .accept(instances.mimeType)
                    .method(instances.method, ClientResponse.class, props);
        }
        checkIfSuccessful(clientResponse);

        if (instances.name().equals("LOG")) {
            return parseProcessInstanceResultLogs(clientResponse, runid);
        } else if (instances.name().equals("SUMMARY")) {
            return summarizeProcessInstanceResult(clientResponse);
        } else {
            return parseProcessInstanceResult(clientResponse);
        }

    }
    //RESUME CHECKSTYLE CHECK VisibilityModifierCheck

    private String sendAdminRequest(AdminOperations job)
        throws FalconCLIException {

        ClientResponse clientResponse = service.path(job.path)
                .header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                .accept(job.mimeType)
                .type(job.mimeType)
                .method(job.method, ClientResponse.class);
        return parseStringResult(clientResponse);
    }

    private String parseAPIResult(ClientResponse clientResponse)
        throws FalconCLIException {

        APIResult result = clientResponse.getEntity(APIResult.class);
        return result.getMessage();
    }

    private EntityList parseEntityList(ClientResponse clientResponse)
        throws FalconCLIException {

        EntityList result = clientResponse.getEntity(EntityList.class);
        if (result == null || result.getElements() == null) {
            return null;
        }
        return result;

    }

    private String parseStringResult(ClientResponse clientResponse)
        throws FalconCLIException {

        return clientResponse.getEntity(String.class);
    }

    private String summarizeProcessInstanceResult(ClientResponse clientResponse) {
        InstancesSummaryResult result = clientResponse
                .getEntity(InstancesSummaryResult.class);
        StringBuilder sb = new StringBuilder();
        String toAppend;

        sb.append("Consolidated Status: ").append(result.getStatus()).append("\n");
        sb.append("\nInstances Summary:\n");

        if (result.getInstancesSummary() != null) {
            for (InstancesSummaryResult.InstanceSummary summary : result.getInstancesSummary()) {
                toAppend = summary.getCluster() != null ? summary.getCluster() : "-";
                sb.append("Cluster: ").append(toAppend).append("\n");

                sb.append("Status\t\tCount\n");
                sb.append("-------------------------\n");

                for (Map.Entry<String, Long> entry : summary.getSummaryMap().entrySet()) {
                    sb.append(entry.getKey()).append("\t\t").append(entry.getValue()).append("\n");
                }
            }
        }

        sb.append("\nAdditional Information:\n");
        sb.append("Response: ").append(result.getMessage());
        sb.append("Request Id: ").append(result.getRequestId());
        return sb.toString();
    }

    private String parseProcessInstanceResult(ClientResponse clientResponse) {
        InstancesResult result = clientResponse
                .getEntity(InstancesResult.class);
        StringBuilder sb = new StringBuilder();
        String toAppend;

        sb.append("Consolidated Status: ").append(result.getStatus()).append("\n");

        sb.append("\nInstances:\n");
        sb.append("Instance\t\tCluster\t\tSourceCluster\t\tStatus\t\tStart\t\tEnd\t\tDetails\t\t\t\t\tLog\n");
        sb.append("-----------------------------------------------------------------------------------------------\n");
        if (result.getInstances() != null) {
            for (InstancesResult.Instance instance : result.getInstances()) {

                toAppend = instance.getInstance() != null ? instance.getInstance() : "-";
                sb.append(toAppend).append("\t");

                toAppend = instance.getCluster() != null ? instance.getCluster() : "-";
                sb.append(toAppend).append("\t");

                toAppend = instance.getSourceCluster() != null ? instance.getSourceCluster() : "-";
                sb.append(toAppend).append("\t");

                toAppend = (instance.getStatus() != null ? instance.getStatus().toString() : "-");
                sb.append(toAppend).append("\t");

                toAppend = instance.getStartTime() != null
                        ? SchemaHelper.formatDateUTC(instance.getStartTime()) : "-";
                sb.append(toAppend).append("\t");

                toAppend = instance.getEndTime() != null
                        ? SchemaHelper.formatDateUTC(instance.getEndTime()) : "-";
                sb.append(toAppend).append("\t");

                toAppend = (instance.getDetails() != null && !instance.getDetails().equals(""))
                        ? instance.getDetails() : "-";
                sb.append(toAppend).append("\t");

                toAppend = instance.getLogFile() != null ? instance.getLogFile() : "-";
                sb.append(toAppend).append("\n");

            }
        }
        sb.append("\nAdditional Information:\n");
        sb.append("Response: ").append(result.getMessage());
        sb.append("Request Id: ").append(result.getRequestId());
        return sb.toString();
    }

    private String parseProcessInstanceResultLogs(ClientResponse clientResponse, String runid) {
        InstancesResult result = clientResponse
                .getEntity(InstancesResult.class);
        StringBuilder sb = new StringBuilder();
        String toAppend;

        sb.append("Consolidated Status: ").append(result.getStatus()).append("\n");

        sb.append("\nInstances:\n");
        sb.append("Instance\t\tCluster\t\tSourceCluster\t\tStatus\t\tRunID\t\t\tLog\n");
        sb.append("-----------------------------------------------------------------------------------------------\n");
        if (result.getInstances() != null) {
            for (InstancesResult.Instance instance : result.getInstances()) {

                toAppend = (instance.getInstance() != null) ? instance.getInstance() : "-";
                sb.append(toAppend).append("\t");

                toAppend = instance.getCluster() != null ? instance.getCluster() : "-";
                sb.append(toAppend).append("\t");

                toAppend = instance.getSourceCluster() != null ? instance.getSourceCluster() : "-";
                sb.append(toAppend).append("\t");

                toAppend = (instance.getStatus() != null ? instance.getStatus().toString() : "-");
                sb.append(toAppend).append("\t");

                toAppend = (runid != null ? runid : "latest");
                sb.append(toAppend).append("\t");

                toAppend = instance.getLogFile() != null ? instance.getLogFile() : "-";
                sb.append(toAppend).append("\n");

                if (instance.actions != null) {
                    sb.append("actions:\n");
                    for (InstancesResult.InstanceAction action : instance.actions) {
                        sb.append("    ").append(action.getAction()).append("\t");
                        sb.append(action.getStatus()).append("\t").append(action.getLogFile()).append("\n");
                    }
                }
            }
        }
        sb.append("\nAdditional Information:\n");
        sb.append("Response: ").append(result.getMessage());
        sb.append("Request Id: ").append(result.getRequestId());
        return sb.toString();
    }

    protected static enum GraphOperations {

        VERTICES("api/graphs/lineage/vertices", HttpMethod.GET, MediaType.APPLICATION_JSON),
        EDGES("api/graphs/lineage/edges", HttpMethod.GET, MediaType.APPLICATION_JSON);

        private String path;
        private String method;
        private String mimeType;

        GraphOperations(String path, String method, String mimeType) {
            this.path = path;
            this.method = method;
            this.mimeType = mimeType;
        }
    }

    public String getVertex(String id) throws FalconCLIException {
        return sendGraphRequest(GraphOperations.VERTICES, id);
    }

    public String getVertices(String key, String value) throws FalconCLIException {
        return sendGraphRequest(GraphOperations.VERTICES, key, value);
    }

    public String getVertexEdges(String id, String direction) throws FalconCLIException {
        return sendGraphRequestForEdges(GraphOperations.VERTICES, id, direction);
    }

    public String getEdge(String id) throws FalconCLIException {
        return sendGraphRequest(GraphOperations.EDGES, id);
    }

    private String sendGraphRequest(GraphOperations job, String id) throws FalconCLIException {
        ClientResponse clientResponse = service.path(job.path)
                .path(id)
                .header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                .accept(job.mimeType)
                .type(job.mimeType)
                .method(job.method, ClientResponse.class);
        return parseStringResult(clientResponse);
    }

    private String sendGraphRequest(GraphOperations job, String key,
                                    String value) throws FalconCLIException {
        ClientResponse clientResponse = service.path(job.path)
                .queryParam("key", key)
                .queryParam("value", value)
                .header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                .accept(job.mimeType)
                .type(job.mimeType)
                .method(job.method, ClientResponse.class);
        return parseStringResult(clientResponse);
    }

    private String sendGraphRequestForEdges(GraphOperations job, String id,
                                            String direction) throws FalconCLIException {
        ClientResponse clientResponse = service.path(job.path)
                .path(id)
                .path(direction)
                .header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                .accept(job.mimeType)
                .type(job.mimeType)
                .method(job.method, ClientResponse.class);
        return parseStringResult(clientResponse);
    }

    private void checkIfSuccessful(ClientResponse clientResponse)
        throws FalconCLIException {

        if (clientResponse.getStatus() == Response.Status.BAD_REQUEST
                .getStatusCode()) {
            throw FalconCLIException.fromReponse(clientResponse);
        }
    }
}
