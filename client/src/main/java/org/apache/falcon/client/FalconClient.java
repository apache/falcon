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
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import com.sun.jersey.multipart.FormDataMultiPart;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.util.TrustManagerUtils;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.ExtensionHandler;
import org.apache.falcon.entity.v0.DateValidator;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.extensions.ExtensionType;
import org.apache.falcon.metadata.RelationshipType;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.EntityList;
import org.apache.falcon.resource.EntitySummaryResult;
import org.apache.falcon.resource.ExtensionInstanceList;
import org.apache.falcon.resource.ExtensionJobList;
import org.apache.falcon.resource.FeedInstanceResult;
import org.apache.falcon.resource.FeedLookupResult;
import org.apache.falcon.resource.InstanceDependencyResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.InstancesSummaryResult;
import org.apache.falcon.resource.LineageGraphResult;
import org.apache.falcon.resource.SchedulableEntityInstanceResult;
import org.apache.falcon.resource.TriageResult;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.security.SecureRandom;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Client API to submit and manage Falcon Entities (Cluster, Feed, Process) jobs
 * against an Falcon instance.
 */
public class FalconClient extends AbstractFalconClient {

    public static final AtomicReference<PrintStream> OUT = new AtomicReference<>(System.out);
    public static final Logger LOG = LoggerFactory.getLogger(FalconClient.class);

    public static final String WS_HEADER_PREFIX = "header:";
    public static final String USER = System.getProperty("user.name");
    private static final String AUTH_URL = "api/options?" + PseudoAuthenticator.USER_NAME + "=" + USER;

    private static final String PATH = "path";
    private static final String COLO = "colo";
    private static final String KEY = "key";
    private static final String VALUE = "value";
    public static final String CLUSTER = "cluster";
    private static final String RUN_ID = "runid";
    private static final String FORCE = "force";
    private static final String SHOW_SCHEDULER = "showScheduler";
    private static final String ENTITY_NAME = "name";
    private static final String ENTITY_TYPE = "type";
    private static final String SKIP_DRYRUN = "skipDryRun";
    private static final String FILTER_BY = "filterBy";
    private static final String ORDER_BY = "orderBy";
    private static final String SORT_ORDER = "sortOrder";
    private static final String OFFSET = "offset";
    private static final String NUM_RESULTS = "numResults";
    private static final String START = "start";
    private static final String END = "end";
    private static final String INSTANCE_TIME = "instanceTime";
    private static final String INSTANCE_STATUS = "instanceStatus";
    private static final String PROPERTIES = "properties";
    private static final String FIELDS = "fields";
    private static final String NAME_SUBSEQUENCE = "nameseq";
    private static final String FILTER_TAGS = "tags";
    private static final String TAG_KEYWORDS = "tagkeys";
    private static final String LIFECYCLE = "lifecycle";
    private static final String NUM_INSTANCES = "numInstances";
    private static final String ALL_ATTEMPTS = "allAttempts";




    private static final String DO_AS_OPT = "doAs";
    private static final String JOB_NAME_OPT = "jobName";
    public static final String ENTITIES_OPT = "entities";
    /**
     * Name of the HTTP cookie used for the authentication token between the client and the server.
     */
    private static final String AUTH_COOKIE = "hadoop.auth";
    private static final String AUTH_COOKIE_EQ = AUTH_COOKIE + "=";

    private static final KerberosAuthenticator AUTHENTICATOR = new KerberosAuthenticator();
    private static final String TEMPLATE_SUFFIX = "-template.xml";


    private static final String PROPERTIES_SUFFIX = ".properties";
    private static final HostnameVerifier ALL_TRUSTING_HOSTNAME_VERIFIER = new HostnameVerifier() {
        public boolean verify(String hostname, SSLSession sslSession) {
            return true;
        }
    };
    private static final String FEEDS = "feeds";
    private static final String PROCESSES = "processes";
    private static final String CONFIG = "config";
    private final WebResource service;
    private final AuthenticatedURL.Token authenticationToken;

    /**
     * debugMode=false means no debugging. debugMode=true means debugging on.
     */
    private boolean debugMode = false;

    private final Properties clientProperties;

    /**
     * Create a Falcon client instance.
     *
     * @param falconUrl of the server to which client interacts
     * @ - If unable to initialize SSL Props
     */
    public FalconClient(String falconUrl) {
        this(falconUrl, new Properties());
    }

    /**
     * Create a Falcon client instance.
     *
     * @param falconUrl of the server to which client interacts
     * @param properties client properties
     * @ - If unable to initialize SSL Props
     */
    public FalconClient(String falconUrl, Properties properties)  {
        try {
            String baseUrl = notEmpty(falconUrl, "FalconUrl");
            if (!baseUrl.endsWith("/")) {
                baseUrl += "/";
            }
            this.clientProperties = properties;
            SSLContext sslContext = getSslContext();
            DefaultClientConfig config = new DefaultClientConfig();
            config.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES,
                    new HTTPSProperties(ALL_TRUSTING_HOSTNAME_VERIFIER, sslContext)
            );
            Client client = Client.create(config);
            client.setConnectTimeout(Integer.parseInt(clientProperties.getProperty("falcon.connect.timeout",
                    "180000")));
            client.setReadTimeout(Integer.parseInt(clientProperties.getProperty("falcon.read.timeout", "180000")));
            service = client.resource(UriBuilder.fromUri(baseUrl).build());
            client.resource(UriBuilder.fromUri(baseUrl).build());
            authenticationToken = getToken(baseUrl);
        } catch (Exception e) {
            LOG.error("Unable to initialize Falcon Client object. Cause : ", e);
            throw new FalconCLIException("Unable to initialize Falcon Client object. Cause : " + e.getMessage(), e);
        }
    }

    private static SSLContext getSslContext() throws Exception {
        SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(
                null,
                new TrustManager[]{TrustManagerUtils.getValidateServerCertificateTrustManager()},
                new SecureRandom());
        return sslContext;
    }

    /**
     * @return current debug Mode
     */
    public boolean getDebugMode() {
        return debugMode;
    }

    /**
     * Set debug mode.
     *
     * @param debugMode : debugMode=false means no debugging. debugMode=true means debugging on
     */
    public void setDebugMode(boolean debugMode) {
        this.debugMode = debugMode;
    }

    public static AuthenticatedURL.Token getToken(String baseUrl)  {
        AuthenticatedURL.Token currentToken = new AuthenticatedURL.Token();
        try {
            URL url = new URL(baseUrl + AUTH_URL);
            // using KerberosAuthenticator which falls back to PsuedoAuthenticator
            // instead of passing authentication type from the command line - bad factory
            HttpsURLConnection.setDefaultSSLSocketFactory(getSslContext().getSocketFactory());
            HttpsURLConnection.setDefaultHostnameVerifier(ALL_TRUSTING_HOSTNAME_VERIFIER);
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
        UPDATEDEPENDENTS("api/entities/updateClusterDependents/", HttpMethod.POST, MediaType.TEXT_XML),
        SUBMITANDSCHEDULE("api/entities/submitAndSchedule/", HttpMethod.POST, MediaType.TEXT_XML),
        SCHEDULE("api/entities/schedule/", HttpMethod.POST, MediaType.TEXT_XML),
        SUSPEND("api/entities/suspend/", HttpMethod.POST, MediaType.TEXT_XML),
        RESUME("api/entities/resume/", HttpMethod.POST, MediaType.TEXT_XML),
        DELETE("api/entities/delete/", HttpMethod.DELETE, MediaType.TEXT_XML),
        STATUS("api/entities/status/", HttpMethod.GET, MediaType.TEXT_XML),
        DEFINITION("api/entities/definition/", HttpMethod.GET, MediaType.TEXT_XML),
        LIST("api/entities/list/", HttpMethod.GET, MediaType.TEXT_XML),
        SUMMARY("api/entities/summary", HttpMethod.GET, MediaType.APPLICATION_JSON),
        LOOKUP("api/entities/lookup/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        DEPENDENCY("api/entities/dependencies/", HttpMethod.GET, MediaType.TEXT_XML),
        SLA("api/entities/sla-alert", HttpMethod.GET, MediaType.APPLICATION_JSON),
        TOUCH("api/entities/touch", HttpMethod.POST, MediaType.TEXT_XML);

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
     * Methods allowed on Metadata Discovery Resources.
     */
    protected static enum MetadataOperations {

        LIST("api/metadata/discovery/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        RELATIONS("api/metadata/discovery/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        VERTICES("api/metadata/lineage/vertices", HttpMethod.GET, MediaType.APPLICATION_JSON),
        EDGES("api/metadata/lineage/edges", HttpMethod.GET, MediaType.APPLICATION_JSON),
        LINEAGE("api/metadata/lineage/entities", HttpMethod.GET, MediaType.APPLICATION_JSON);

        private String path;
        private String method;
        private String mimeType;

        MetadataOperations(String path, String method, String mimeType) {
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
        LIST("api/instance/list", HttpMethod.GET, MediaType.APPLICATION_JSON),
        KILL("api/instance/kill/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        SUSPEND("api/instance/suspend/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        RESUME("api/instance/resume/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        RERUN("api/instance/rerun/", HttpMethod.POST, MediaType.APPLICATION_JSON),
        LOG("api/instance/logs/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        SUMMARY("api/instance/summary/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        PARAMS("api/instance/params/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        DEPENDENCY("api/instance/dependencies/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        TRIAGE("api/instance/triage/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        LISTING("api/instance/listing/", HttpMethod.GET, MediaType.APPLICATION_JSON),
        SEARCH("api/instance/search/", HttpMethod.GET, MediaType.APPLICATION_JSON);

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
        VERSION("api/admin/version", HttpMethod.GET, MediaType.APPLICATION_JSON),
        SAFEMODE("api/admin/setSafeMode", HttpMethod.GET, MediaType.APPLICATION_JSON);

        private String path;
        private String method;
        private String mimeType;

        AdminOperations(String path, String method, String mimeType) {
            this.path = path;
            this.method = method;
            this.mimeType = mimeType;
        }
    }

    /**
     * Methods allowed on Extension Resources.
     */
    protected static enum ExtensionOperations {

        ENUMERATE("api/extension/enumerate/", HttpMethod.GET, MediaType.TEXT_XML),
        DESCRIBE("api/extension/describe/", HttpMethod.GET, MediaType.TEXT_XML),
        DEFINITION("api/extension/definition", HttpMethod.GET, MediaType.TEXT_XML),
        LIST("api/extension/list", HttpMethod.GET, MediaType.APPLICATION_JSON),
        INSTANCES("api/extension/instances", HttpMethod.GET, MediaType.APPLICATION_JSON),
        SUBMIT("api/extension/submit", HttpMethod.POST, MediaType.TEXT_XML),
        SUBMIT_AND_SCHEDULE("api/extension/submitAndSchedule", HttpMethod.POST, MediaType.TEXT_XML),
        UPDATE("api/extension/update", HttpMethod.POST, MediaType.TEXT_XML),
        VALIDATE("api/extension/validate", HttpMethod.POST, MediaType.TEXT_XML),
        SCHEDULE("api/extension/schedule", HttpMethod.POST, MediaType.TEXT_XML),
        SUSPEND("api/extension/suspend", HttpMethod.POST, MediaType.TEXT_XML),
        RESUME("api/extension/resume", HttpMethod.POST, MediaType.TEXT_XML),
        DELETE("api/extension/delete", HttpMethod.POST, MediaType.TEXT_XML),
        UNREGISTER("api/extension/unregister/", HttpMethod.POST, MediaType.TEXT_XML),
        DETAIL("api/extension/detail/", HttpMethod.GET, MediaType.TEXT_XML),
        JOB_DETAILS("api/extension/extensionJobDetails/", HttpMethod.GET, MediaType.TEXT_XML),
        REGISTER("api/extension/register/", HttpMethod.POST, MediaType.TEXT_XML),
        ENABLE("api/extension/enable", HttpMethod.POST, MediaType.TEXT_XML),
        DISABLE("api/extension/disable", HttpMethod.POST, MediaType.TEXT_XML);

        private String path;
        private String method;
        private String mimeType;

        ExtensionOperations(String path, String method, String mimeType) {
            this.path = path;
            this.method = method;
            this.mimeType = mimeType;
        }
    }

    private String notEmpty(String str, String name) {
        if (str == null) {
            throw new IllegalArgumentException(name + " cannot be null");
        }
        if (str.length() == 0) {
            throw new IllegalArgumentException(name + " cannot be empty");
        }
        return str;
    }

    public APIResult schedule(EntityType entityType, String entityName, String colo,
                              Boolean skipDryRun, String doAsUser, String properties) {
        String type = entityType.toString().toLowerCase();
        ClientResponse clientResponse = new ResourceBuilder().path(Entities.SCHEDULE.path, type, entityName)
            .addQueryParam(COLO, colo).addQueryParam(SKIP_DRYRUN, skipDryRun)
            .addQueryParam(PROPERTIES, properties).addQueryParam(DO_AS_OPT, doAsUser).call(Entities.SCHEDULE);
        return getResponse(APIResult.class, clientResponse);
    }

    public APIResult suspend(EntityType entityType, String entityName, String colo, String doAsUser) {
        String type = entityType.toString().toLowerCase();
        ClientResponse clientResponse = new ResourceBuilder().path(Entities.SUSPEND.path, type, entityName)
            .addQueryParam(COLO, colo).addQueryParam(DO_AS_OPT, doAsUser).call(Entities.SUSPEND);
        return getResponse(APIResult.class, clientResponse);
    }

    public APIResult resume(EntityType entityType, String entityName, String colo, String doAsUser) {
        String type = entityType.toString().toLowerCase();
        ClientResponse clientResponse = new ResourceBuilder().path(Entities.RESUME.path, type, entityName)
            .addQueryParam(COLO, colo).addQueryParam(DO_AS_OPT, doAsUser).call(Entities.RESUME);
        return getResponse(APIResult.class, clientResponse);
    }

    public APIResult delete(EntityType entityType, String entityName, String doAsUser) {
        String type = entityType.toString().toLowerCase();
        ClientResponse clientResponse = new ResourceBuilder().path(Entities.DELETE.path, type, entityName)
            .addQueryParam(DO_AS_OPT, doAsUser).call(Entities.DELETE);
        return getResponse(APIResult.class, clientResponse);
    }

    public APIResult validate(String entityType, String filePath, Boolean skipDryRun, String doAsUser) {
        InputStream entityStream = getServletInputStream(filePath);
        ClientResponse clientResponse = new ResourceBuilder().path(Entities.VALIDATE.path, entityType)
            .addQueryParam(SKIP_DRYRUN, skipDryRun).addQueryParam(DO_AS_OPT, doAsUser)
            .call(Entities.VALIDATE, entityStream);
        return getResponse(APIResult.class, clientResponse);
    }

    public APIResult submit(String entityType, String filePath, String doAsUser) {
        InputStream entityStream = getServletInputStream(filePath);
        ClientResponse clientResponse = new ResourceBuilder().path(Entities.SUBMIT.path, entityType)
            .addQueryParam(DO_AS_OPT, doAsUser).call(Entities.SUBMIT, entityStream);
        return getResponse(APIResult.class, clientResponse);
    }

    public APIResult update(String entityType, String entityName, String filePath,
                            Boolean skipDryRun, String doAsUser) {
        InputStream entityStream = getServletInputStream(filePath);
        Entities operation = Entities.UPDATE;
        ClientResponse clientResponse = new ResourceBuilder().path(operation.path, entityType, entityName)
            .addQueryParam(SKIP_DRYRUN, skipDryRun).addQueryParam(DO_AS_OPT, doAsUser)
            .call(operation, entityStream);
        return getResponse(APIResult.class, clientResponse);
    }

    public APIResult updateClusterDependents(String clusterName, Boolean skipDryRun,
                                             String doAsUser)  {
        ClientResponse clientResponse = new ResourceBuilder().path(Entities.UPDATEDEPENDENTS.path, clusterName)
                .addQueryParam(SKIP_DRYRUN, skipDryRun).addQueryParam(DO_AS_OPT, doAsUser)
                .call(Entities.UPDATEDEPENDENTS);
        return getResponse(APIResult.class, clientResponse);
    }

    @Override
    public APIResult submitAndSchedule(String entityType, String filePath, Boolean skipDryRun,
                                       String doAsUser, String properties) {
        InputStream entityStream = getServletInputStream(filePath);
        ClientResponse clientResponse = new ResourceBuilder().path(Entities.SUBMITANDSCHEDULE.path, entityType)
            .addQueryParam(SKIP_DRYRUN, skipDryRun).addQueryParam(DO_AS_OPT, doAsUser)
            .addQueryParam(PROPERTIES, properties).call(Entities.SUBMITANDSCHEDULE, entityStream);
        return getResponse(APIResult.class, clientResponse);
    }

    public APIResult getStatus(EntityType entityType, String entityName, String colo,
                               String doAsUser, boolean showScheduler) {
        String type = entityType.toString().toLowerCase();
        ClientResponse clientResponse = new ResourceBuilder().path(Entities.STATUS.path, type, entityName)
            .addQueryParam(COLO, colo).addQueryParam(DO_AS_OPT, doAsUser)
            .addQueryParam(SHOW_SCHEDULER, showScheduler).call(Entities.STATUS);

        return getResponse(APIResult.class, clientResponse);
    }

    public Entity getDefinition(String entityType, String entityName, String doAsUser) {
        ClientResponse clientResponse = new ResourceBuilder().path(Entities.DEFINITION.path, entityType, entityName)
            .call(Entities.DEFINITION);
        String entity = getResponseAsString(clientResponse);
        return Entity.fromString(EntityType.getEnum(entityType), entity);
    }

    public EntityList getDependency(String entityType, String entityName, String doAsUser) {

        ClientResponse clientResponse = new ResourceBuilder().path(Entities.DEPENDENCY.path, entityType, entityName)
            .addQueryParam(DO_AS_OPT, doAsUser).call(Entities.DEPENDENCY);

        printClientResponse(clientResponse);
        checkIfSuccessful(clientResponse);

        EntityList result = clientResponse.getEntity(EntityList.class);
        if (result == null) {
            return new EntityList();
        }
        return result;
    }

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck

    public SchedulableEntityInstanceResult getFeedSlaMissPendingAlerts(String entityType, String entityName,
                                           String startTime, String endTime, String colo) {
        ClientResponse clientResponse  = new ResourceBuilder().path(Entities.SLA.path, entityType)
            .addQueryParam(START, startTime).addQueryParam(COLO, colo).addQueryParam(END, endTime)
            .addQueryParam(ENTITY_NAME, entityName).call(Entities.SLA);
        return getResponse(SchedulableEntityInstanceResult.class, clientResponse);
    }

    public TriageResult triage(String entityType, String entityName, String instanceTime,
                               String colo) {
        ClientResponse clientResponse = new ResourceBuilder().path(Instances.TRIAGE.path, entityType, entityName)
            .addQueryParam(START, instanceTime).addQueryParam(COLO, colo).call(Instances.TRIAGE);
        return getResponse(TriageResult.class, clientResponse);
    }

    @Override
    public EntityList getEntityList(String entityType, String fields, String nameSubsequence, String tagKeywords,
                                    String filterBy, String filterTags, String orderBy, String sortOrder,
                                    Integer offset, Integer numResults, String doAsUser) {
        Entities operation = Entities.LIST;
        ClientResponse clientResponse = new ResourceBuilder().path(operation.path, entityType)
            .addQueryParam(DO_AS_OPT, doAsUser).addQueryParam(NUM_RESULTS, numResults)
            .addQueryParam(OFFSET, offset).addQueryParam(SORT_ORDER, sortOrder)
            .addQueryParam(ORDER_BY, orderBy).addQueryParam(FILTER_BY, filterBy)
            .addQueryParam(FIELDS, fields).addQueryParam(NAME_SUBSEQUENCE, nameSubsequence)
            .addQueryParam(TAG_KEYWORDS, tagKeywords).addQueryParam(FILTER_TAGS, filterTags)
            .call(operation);

        printClientResponse(clientResponse);
        checkIfSuccessful(clientResponse);

        EntityList result = clientResponse.getEntity(EntityList.class);
        if (result == null || result.getElements() == null) {
            return null;
        }
        return result;
    }

    @Override
    public EntitySummaryResult getEntitySummary(String entityType, String cluster, String start, String end,
                                   String fields, String filterBy, String filterTags,
                                   String orderBy, String sortOrder, Integer offset, Integer numResults,
                                   Integer numInstances, String doAsUser) {
        ClientResponse clientResponse = new ResourceBuilder().path(Entities.SUMMARY.path, entityType)
            .addQueryParam(CLUSTER, cluster).addQueryParam(START, start).addQueryParam(END, end)
            .addQueryParam(SORT_ORDER, sortOrder).addQueryParam(ORDER_BY, orderBy)
            .addQueryParam(OFFSET, offset).addQueryParam(NUM_RESULTS, numResults)
            .addQueryParam(DO_AS_OPT, doAsUser).addQueryParam(FILTER_BY, filterBy)
            .addQueryParam(NUM_INSTANCES, numInstances).addQueryParam(FIELDS, fields)
            .addQueryParam(FILTER_TAGS, filterTags).call(Entities.SUMMARY);
        return getResponse(EntitySummaryResult.class, clientResponse);
    }

    @Override
    public APIResult touch(String entityType, String entityName, String colo,
                           Boolean skipDryRun, String doAsUser) {
        Entities operation = Entities.TOUCH;
        ClientResponse clientResponse = new ResourceBuilder().path(operation.path, entityType, entityName)
            .addQueryParam(COLO, colo).addQueryParam(SKIP_DRYRUN, skipDryRun)
            .addQueryParam(DO_AS_OPT, doAsUser).call(operation);
        return getResponse(APIResult.class, clientResponse);
    }
    public InstancesResult getRunningInstances(String type, String entity, String colo, List<LifeCycle> lifeCycles,
                                      String filterBy, String orderBy, String sortOrder,
                                      Integer offset, Integer numResults, String doAsUser) {
        ClientResponse clientResponse = new ResourceBuilder().path(Instances.RUNNING.path, type, entity)
            .addQueryParam(FILTER_BY, filterBy).addQueryParam(ORDER_BY, orderBy)
            .addQueryParam(SORT_ORDER, sortOrder).addQueryParam(OFFSET, offset)
            .addQueryParam(NUM_RESULTS, numResults).addQueryParam(COLO, colo)
            .addQueryParam(LIFECYCLE, lifeCycles, type).addQueryParam(USER, doAsUser).call(Instances.RUNNING);
        return getResponse(InstancesResult.class, clientResponse);
    }
    @Override
    public InstancesResult getStatusOfInstances(String type, String entity, String start, String end, String colo,
                                                List<LifeCycle> lifeCycles, String filterBy, String orderBy,
                                                String sortOrder, Integer offset, Integer numResults,
                                                String doAsUser, Boolean allAttempts) {
        ClientResponse clientResponse = new ResourceBuilder().path(Instances.STATUS.path, type, entity)
            .addQueryParam(START, start).addQueryParam(END, end).addQueryParam(COLO, colo)
            .addQueryParam(LIFECYCLE, lifeCycles, type).addQueryParam(FILTER_BY, filterBy)
            .addQueryParam(ORDER_BY, orderBy).addQueryParam(SORT_ORDER, sortOrder)
            .addQueryParam(OFFSET, offset).addQueryParam(NUM_RESULTS, numResults)
            .addQueryParam(ALL_ATTEMPTS, allAttempts).addQueryParam(USER, doAsUser).call(Instances.STATUS);
        return getResponse(InstancesResult.class, clientResponse);
    }

    public InstancesSummaryResult getSummaryOfInstances(String type, String entity,
                                        String start, String end,
                                        String colo, List<LifeCycle> lifeCycles,
                                        String filterBy, String orderBy, String sortOrder,
                                        String doAsUser) {
        ClientResponse clientResponse = new ResourceBuilder().path(Instances.SUMMARY.path, type, entity)
            .addQueryParam(START, start).addQueryParam(END, end).addQueryParam(COLO, colo)
            .addQueryParam(LIFECYCLE, lifeCycles, type).addQueryParam(USER, doAsUser).call(Instances.SUMMARY);
        return getResponse(InstancesSummaryResult.class, clientResponse);
    }


    public FeedInstanceResult getFeedListing(String type, String entity, String start,
                                     String end, String colo, String doAsUser) {
        ClientResponse clientResponse = new ResourceBuilder().path(Instances.KILL.path, type, entity)
            .addQueryParam(START, start).addQueryParam(END, end).addQueryParam(COLO, colo)
            .addQueryParam(USER, doAsUser).call(Instances.LISTING);
        return getResponse(FeedInstanceResult.class, clientResponse);
    }

    public InstancesResult searchInstances(String type, String nameSubsequence, String tagKeywords,
                                           String start, String end, String status, String orderBy,
                                           Integer offset, Integer numResults)  {
        ClientResponse clientResponse = new ResourceBuilder().path(Instances.SEARCH.path)
                .addQueryParam(ENTITY_TYPE, type)
                .addQueryParam(NAME_SUBSEQUENCE, nameSubsequence)
                .addQueryParam(TAG_KEYWORDS, tagKeywords)
                .addQueryParam(START, start)
                .addQueryParam(END, end)
                .addQueryParam(INSTANCE_STATUS, status)
                .addQueryParam(ORDER_BY, orderBy)
                .addQueryParam(OFFSET, offset)
                .addQueryParam(NUM_RESULTS, numResults)
                .call(Instances.SEARCH);
        return getResponse(InstancesResult.class, clientResponse);
    }

    public InstancesResult killInstances(String type, String entity, String start,
                                String end, String colo, String clusters,
                                String sourceClusters, List<LifeCycle> lifeCycles,
                                String doAsUser) throws UnsupportedEncodingException {
        InputStream props = getServletInputStream(clusters, sourceClusters, null);
        ClientResponse clientResponse = new ResourceBuilder().path(Instances.KILL.path, type, entity)
            .addQueryParam(START, start).addQueryParam(END, end).addQueryParam(COLO, colo)
            .addQueryParam(LIFECYCLE, lifeCycles, type).addQueryParam(USER, doAsUser).call(Instances.KILL, props);
        return getResponse(InstancesResult.class, clientResponse);
    }

    public InstancesResult suspendInstances(String type, String entity, String start, String end, String colo,
                                            String clusters, String sourceClusters, List<LifeCycle> lifeCycles,
                                            String doAsUser) throws UnsupportedEncodingException {
        ClientResponse clientResponse = new ResourceBuilder().path(Instances.SUSPEND.path, type, entity)
            .addQueryParam(START, start).addQueryParam(END, end).addQueryParam(COLO, colo)
            .addQueryParam(LIFECYCLE, lifeCycles, type).addQueryParam(USER, doAsUser).call(Instances.SUSPEND);
        return getResponse(InstancesResult.class, clientResponse);
    }

    public InstancesResult resumeInstances(String type, String entity, String start, String end, String colo,
                                           String clusters, String sourceClusters, List<LifeCycle> lifeCycles,
                                           String doAsUser) throws UnsupportedEncodingException {
        ClientResponse clientResponse = new ResourceBuilder().path(Instances.RESUME.path, type, entity)
            .addQueryParam(START, start).addQueryParam(END, end).addQueryParam(COLO, colo)
            .addQueryParam(LIFECYCLE, lifeCycles, type).addQueryParam(USER, doAsUser).call(Instances.RESUME);
        return getResponse(InstancesResult.class, clientResponse);
    }

    public InstancesResult rerunInstances(String type, String entity, String start,
                                 String end, String filePath, String colo,
                                 String clusters, String sourceClusters, List<LifeCycle> lifeCycles,
                                 Boolean isForced, String doAsUser) throws IOException {

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
        InputStream props = getServletInputStream(clusters, sourceClusters, temp);
        ClientResponse clientResponse = new ResourceBuilder().path(Instances.RERUN.path, type, entity)
            .addQueryParam(START, start).addQueryParam(END, end).addQueryParam(COLO, colo)
            .addQueryParam(LIFECYCLE, lifeCycles, type).addQueryParam(FORCE, isForced)
            .addQueryParam(USER, doAsUser).call(Instances.RERUN, props);
        return getResponse(InstancesResult.class, clientResponse);
    }

    public InstancesResult getLogsOfInstances(String type, String entity, String start,
                                              String end, String colo, String runId,
                                              List<LifeCycle> lifeCycles, String filterBy,
                                              String orderBy, String sortOrder, Integer offset,
                                              Integer numResults, String doAsUser) {
        ClientResponse clientResponse = new ResourceBuilder().path(Instances.LOG.path, type, entity)
            .addQueryParam(START, start).addQueryParam(END, end).addQueryParam(COLO, colo)
            .addQueryParam(RUN_ID, runId).addQueryParam(LIFECYCLE, lifeCycles, type)
            .addQueryParam(FILTER_BY, filterBy).addQueryParam(ORDER_BY, orderBy)
            .addQueryParam(SORT_ORDER, sortOrder).addQueryParam(OFFSET, offset)
            .addQueryParam(NUM_RESULTS, numResults).addQueryParam(USER, doAsUser).call(Instances.LOG);
        return getResponse(InstancesResult.class, clientResponse);
    }

    public InstancesResult getParamsOfInstance(String type, String entity,
                                      String start, String colo,
                                      List<LifeCycle> lifeCycles,
                                      String doAsUser)
        throws UnsupportedEncodingException {
        if (!DateValidator.validate(start)) {
            throw new FalconCLIException("Start date is mandatory and should be"
                    + " a valid date in  YYYY-MM-DDTHH:MMZ format.");
        }

        ClientResponse clientResponse = new ResourceBuilder().path(Instances.PARAMS.path, type, entity)
            .addQueryParam(START, start).addQueryParam(LIFECYCLE, lifeCycles, type)
            .addQueryParam(USER, doAsUser).call(Instances.PARAMS);
        return getResponse(InstancesResult.class, clientResponse);
    }

    public String getThreadDump(String doAsUser) {
        return sendAdminRequest(AdminOperations.STACK, doAsUser);
    }

    @Override
    public String getVersion(String doAsUser) {
        return sendAdminRequest(AdminOperations.VERSION, doAsUser);
    }

    public int getStatus(String doAsUser) {
        AdminOperations job =  AdminOperations.VERSION;
        ClientResponse clientResponse = new ResourceBuilder().path(job.path).addQueryParam(DO_AS_OPT, doAsUser)
            .call(job);
        printClientResponse(clientResponse);
        return clientResponse.getStatus();
    }

    public ClientResponse setSafemode(String safemode, String doAsUser)  {
        AdminOperations job =  AdminOperations.SAFEMODE;
        ClientResponse clientResponse = new ResourceBuilder().path(job.path).path(safemode)
                .addQueryParam(DO_AS_OPT, doAsUser).call(job);
        printClientResponse(clientResponse);
        return clientResponse;
    }

    public String getDimensionList(String dimensionType, String cluster, String doAsUser)  {
        return sendMetadataDiscoveryRequest(MetadataOperations.LIST, dimensionType, null, cluster, doAsUser);
    }

    public String getReplicationMetricsDimensionList(String schedEntityType, String schedEntityName,
                                                     Integer numResults, String doAsUser) {
        return sendRequestForReplicationMetrics(MetadataOperations.LIST,
                schedEntityType, schedEntityName, numResults, doAsUser);
    }

    public LineageGraphResult getEntityLineageGraph(String pipelineName, String doAsUser) {
        MetadataOperations operation = MetadataOperations.LINEAGE;
        ClientResponse clientResponse = new ResourceBuilder().path(operation.path).addQueryParam(DO_AS_OPT, doAsUser)
            .addQueryParam(FalconCLIConstants.PIPELINE_OPT, pipelineName).call(operation);
        printClientResponse(clientResponse);
        checkIfSuccessful(clientResponse);
        return clientResponse.getEntity(LineageGraphResult.class);
    }

    public String getDimensionRelations(String dimensionType, String dimensionName,
                                        String doAsUser) {
        return sendMetadataDiscoveryRequest(MetadataOperations.RELATIONS, dimensionType, dimensionName, null, doAsUser);
    }

    private <T> T getResponse(Class<T> clazz, ClientResponse clientResponse) {
        printClientResponse(clientResponse);
        checkIfSuccessful(clientResponse);
        return clientResponse.getEntity(clazz);
    }

    private String getResponseAsString(ClientResponse clientResponse) {
        printClientResponse(clientResponse);
        checkIfSuccessful(clientResponse);
        return clientResponse.getEntity(String.class);
    }

    private class ResourceBuilder {
        WebResource resource;

        private ResourceBuilder path(String... paths) {
            for (String path : paths) {
                if (resource == null) {
                    resource = service.path(path);
                } else {
                    resource = resource.path(path);
                }
            }
            return this;
        }

        private ResourceBuilder addQueryParam(String paramName, Integer value) {
            if (value != null) {
                resource = resource.queryParam(paramName, value.toString());
            }
            return this;
        }

        private ResourceBuilder addQueryParam(String paramName, Boolean paramValue) {
            if (paramValue != null) {
                resource = resource.queryParam(paramName, String.valueOf(paramValue));
            }
            return this;
        }

        private ResourceBuilder addQueryParam(String paramName, String paramValue) {
            if (StringUtils.isNotBlank(paramValue)) {
                resource = resource.queryParam(paramName, paramValue);
            }
            return this;
        }

        private ResourceBuilder addQueryParam(String paramName, List<LifeCycle> lifeCycles,
                                             String type) {
            if (lifeCycles != null) {
                checkLifeCycleOption(lifeCycles, type);
                for (LifeCycle lifeCycle : lifeCycles) {
                    resource = resource.queryParam(paramName, lifeCycle.toString());
                }
            }
            return this;
        }

        private ClientResponse call(Entities entities) {
            return resource.header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                .accept(entities.mimeType).type(MediaType.TEXT_XML)
                .method(entities.method, ClientResponse.class);
        }

        private ClientResponse call(AdminOperations operation) {
            return resource.header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                .accept(operation.mimeType).type(MediaType.TEXT_XML)
                .method(operation.method, ClientResponse.class);
        }

        private ClientResponse call(MetadataOperations metadataOperations) {
            return resource.header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                .accept(metadataOperations.mimeType).type(MediaType.TEXT_XML)
                .method(metadataOperations.method, ClientResponse.class);
        }

        private ClientResponse call(Instances operation) {
            return resource.header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                .accept(operation.mimeType).type(MediaType.TEXT_XML)
                .method(operation.method, ClientResponse.class);
        }

        private ClientResponse call(ExtensionOperations operation) {
            return resource.header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                    .accept(operation.mimeType).type(MediaType.TEXT_XML)
                    .method(operation.method, ClientResponse.class);
        }

        private ClientResponse call(Entities operation, InputStream entityStream) {
            return resource.header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                .accept(operation.mimeType).type(MediaType.TEXT_XML)
                .method(operation.method, ClientResponse.class, entityStream);
        }

        private ClientResponse call(Instances operation, InputStream entityStream) {
            return resource.header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                .accept(operation.mimeType).type(MediaType.TEXT_XML)
                .method(operation.method, ClientResponse.class, entityStream);
        }

        private ClientResponse call(ExtensionOperations operation, InputStream entityStream) {
            return resource.header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                    .accept(operation.mimeType).type(MediaType.TEXT_XML)
                    .method(operation.method, ClientResponse.class, entityStream);
        }

        private ClientResponse call(ExtensionOperations submit, FormDataMultiPart formDataMultiPart) {
            return resource.header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                    .accept(submit.mimeType).type(MediaType.MULTIPART_FORM_DATA)
                    .method(submit.method, ClientResponse.class, formDataMultiPart);
        }
    }

    public FeedLookupResult reverseLookUp(String type, String path, String doAsUser) {
        Entities api = Entities.LOOKUP;
        ClientResponse response = new ResourceBuilder().path(api.path, type).addQueryParam(DO_AS_OPT, doAsUser)
            .addQueryParam(PATH, path).call(api);
        return getResponse(FeedLookupResult.class, response);
    }

    public FeedInstanceResult getFeedInstanceListing(String type, String entity, String start, String end, String colo
            , String doAsUser) {

        checkType(type);
        Instances api = Instances.LISTING;
        ClientResponse clientResponse = new ResourceBuilder().path(api.path, type, entity)
            .addQueryParam(COLO, colo).addQueryParam(DO_AS_OPT, doAsUser).addQueryParam(START, start)
            .addQueryParam(END, end).call(api);
        return getResponse(FeedInstanceResult.class, clientResponse);
    }


    public InstanceDependencyResult getInstanceDependencies(String entityType, String entityName, String instanceTime,
                                                            String colo) {
        checkType(entityType);
        Instances api = Instances.DEPENDENCY;
        ClientResponse clientResponse = new ResourceBuilder().path(api.path, entityType, entityName)
            .addQueryParam(COLO, colo).addQueryParam(INSTANCE_TIME, instanceTime).call(api);
        return getResponse(InstanceDependencyResult.class, clientResponse);
    }

    //RESUME CHECKSTYLE CHECK VisibilityModifierCheck

    private void checkLifeCycleOption(List<LifeCycle> lifeCycles, String type) {
        if (lifeCycles != null && !lifeCycles.isEmpty()) {
            EntityType entityType = EntityType.getEnum(type);
            for (LifeCycle lifeCycle : lifeCycles) {
                if (entityType != lifeCycle.getTag().getType()) {
                    throw new FalconCLIException("Incorrect lifecycle: " + lifeCycle + "for given type: " + type);
                }
            }
        }
    }

    private void checkType(String type) {
        if (type == null || type.isEmpty()) {
            throw new FalconCLIException("entity type is empty");
        } else {
            EntityType entityType = EntityType.getEnum(type);
            if (entityType == EntityType.CLUSTER) {
                throw new FalconCLIException(
                        "Instance management functions don't apply to Cluster entities");
            }
        }
    }

    private String sendAdminRequest(AdminOperations job, String doAsUser) {
        ClientResponse clientResponse = new ResourceBuilder().path(job.path).addQueryParam(DO_AS_OPT, doAsUser)
            .call(job);
        return getResponseAsString(clientResponse);
    }

    private String sendRequestForReplicationMetrics(final MetadataOperations operation, final String schedEntityType,
                                                    final String schedEntityName, Integer numResults,
                                                    final String doAsUser) {
        WebResource resource = service.path(operation.path)
                .path(schedEntityName)
                .path(RelationshipType.REPLICATION_METRICS.getName())
                .path(FalconCLIConstants.LIST_OPT);

        if (StringUtils.isNotEmpty(schedEntityName)) {
            resource = resource.queryParam(FalconCLIConstants.TYPE_OPT, schedEntityType);
        }

        if (numResults != null) {
            resource = resource.queryParam(FalconCLIConstants.NUM_RESULTS_OPT, numResults.toString());
        }

        if (StringUtils.isNotEmpty(doAsUser)) {
            resource = resource.queryParam(FalconCLIConstants.DO_AS_OPT, doAsUser);
        }

        ClientResponse clientResponse = resource
                .header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                .accept(operation.mimeType).type(operation.mimeType)
                .method(operation.method, ClientResponse.class);

        printClientResponse(clientResponse);

        checkIfSuccessful(clientResponse);
        return clientResponse.getEntity(String.class);

    }

    private String sendMetadataDiscoveryRequest(final MetadataOperations operation,
                                                final String dimensionType,
                                                final String dimensionName,
                                                final String cluster,
                                                final String doAsUser) {
        WebResource resource;
        switch (operation) {
        case LIST:
            resource = service.path(operation.path)
                    .path(dimensionType)
                    .path(FalconCLIConstants.LIST_OPT);
            break;

        case RELATIONS:
            resource = service.path(operation.path)
                    .path(dimensionType)
                    .path(dimensionName)
                    .path(FalconCLIConstants.RELATIONS_OPT);
            break;

        default:
            throw new FalconCLIException("Invalid Metadata client Operation " + operation.toString());
        }

        if (!StringUtils.isEmpty(cluster)) {
            resource = resource.queryParam(FalconCLIConstants.CLUSTER_OPT, cluster);
        }

        if (StringUtils.isNotEmpty(doAsUser)) {
            resource = resource.queryParam(FalconCLIConstants.DO_AS_OPT, doAsUser);
        }

        ClientResponse clientResponse = resource
                .header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                .accept(operation.mimeType).type(operation.mimeType)
                .method(operation.method, ClientResponse.class);

        printClientResponse(clientResponse);

        checkIfSuccessful(clientResponse);
        return clientResponse.getEntity(String.class);
    }


    public String getVertex(String id, String doAsUser) {
        return sendMetadataLineageRequest(MetadataOperations.VERTICES, id, doAsUser);
    }

    public String getVertices(String key, String value, String doAsUser) {
        return sendMetadataLineageRequest(MetadataOperations.VERTICES, key, value, doAsUser);
    }

    public String getVertexEdges(String id, String direction, String doAsUser) {
        return sendMetadataLineageRequestForEdges(MetadataOperations.VERTICES, id, direction, doAsUser);
    }

    public String getEdge(String id, String doAsUser) {
        return sendMetadataLineageRequest(MetadataOperations.EDGES, id, doAsUser);
    }

    public APIResult enumerateExtensions()  {
        ClientResponse clientResponse = new ResourceBuilder()
                .path(ExtensionOperations.ENUMERATE.path)
                .call(ExtensionOperations.ENUMERATE);
        return getResponse(APIResult.class, clientResponse);
    }

    @Override
    public ExtensionJobList getExtensionJobs(String extensionName, String sortOrder, String doAsUser) {
        ClientResponse clientResponse = new ResourceBuilder()
                .path(ExtensionOperations.LIST.path, extensionName)
                .addQueryParam(DO_AS_OPT, doAsUser)
                .addQueryParam(SORT_ORDER, sortOrder)
                .call(ExtensionOperations.LIST);
        return getResponse(ExtensionJobList.class, clientResponse);
    }

    public APIResult unregisterExtension(final String extensionName) {
        ClientResponse clientResponse = new ResourceBuilder()
                .path(ExtensionOperations.UNREGISTER.path, extensionName)
                .call(ExtensionOperations.UNREGISTER);
        return getResponse(APIResult.class, clientResponse);
    }

    public APIResult getExtensionDetail(final String extensionName) {
        return getResponse(APIResult.class, getExtensionDetailResponse(extensionName));
    }

    public APIResult getExtensionJobDetails(final String jobName) {
        return getResponse(APIResult.class, getExtensionJobDetailsResponse(jobName));
    }

    private ClientResponse getExtensionJobDetailsResponse(final String jobName) {
        return new ResourceBuilder().path(ExtensionOperations.JOB_DETAILS.path, jobName)
                .call(ExtensionOperations.JOB_DETAILS);
    }

    private ClientResponse getExtensionDetailResponse(final String extensionName) {
        return  new ResourceBuilder().path(ExtensionOperations.DETAIL.path, extensionName)
                .call(ExtensionOperations.DETAIL);
    }

    public APIResult registerExtension(final String extensionName, final String packagePath, final String description) {
        ClientResponse clientResponse = new ResourceBuilder()
                .path(ExtensionOperations.REGISTER.path, extensionName).addQueryParam(PATH, packagePath)
                .addQueryParam(FalconCLIConstants.DESCRIPTION, description)
                .call(ExtensionOperations.REGISTER);
        return getResponse(APIResult.class, clientResponse);
    }

    public APIResult enableExtension(final String extensionName) {
        ClientResponse clientResponse = new ResourceBuilder()
                .path(ExtensionOperations.ENABLE.path, extensionName).call(ExtensionOperations.ENABLE);
        return getResponse(APIResult.class, clientResponse);
    }

    public APIResult disableExtension(final String extensionName) {
        ClientResponse clientResponse = new ResourceBuilder()
                .path(ExtensionOperations.DISABLE.path, extensionName).call(ExtensionOperations.DISABLE);
        return getResponse(APIResult.class, clientResponse);
    }

    public APIResult getExtensionDefinition(final String extensionName)  {
        ClientResponse clientResponse = new ResourceBuilder()
                .path(ExtensionOperations.DEFINITION.path, extensionName)
                .call(ExtensionOperations.DEFINITION);
        return getResponse(APIResult.class, clientResponse);
    }

    public APIResult getExtensionDescription(final String extensionName)  {
        ClientResponse clientResponse = new ResourceBuilder()
                .path(ExtensionOperations.DESCRIBE.path, extensionName)
                .call(ExtensionOperations.DESCRIBE);
        return getResponse(APIResult.class, clientResponse);
    }

    @Override
    public APIResult submitExtensionJob(final String extensionName, final String jobName, final String configPath,
                                        final String doAsUser) {
        FormDataMultiPart entitiesForm = getEntitiesForm(extensionName, jobName, configPath);
        ClientResponse clientResponse = new ResourceBuilder()
                .path(ExtensionOperations.SUBMIT.path, extensionName)
                .addQueryParam(DO_AS_OPT, doAsUser)
                .addQueryParam(JOB_NAME_OPT, jobName)
                .call(ExtensionOperations.SUBMIT, entitiesForm);
        return getResponse(APIResult.class, clientResponse);
    }

    private FormDataMultiPart getEntitiesForm(String extensionName, String jobName, String configPath) {
        InputStream configStream = getServletInputStream(configPath);
        List<Entity> entities = validateExtensionAndGetEntities(extensionName, jobName, configStream);
        FormDataMultiPart formDataMultiPart = new FormDataMultiPart();

        if (entities != null && !entities.isEmpty()) {
            for (Entity entity : entities) {
                if (EntityType.FEED.equals(entity.getEntityType())) {
                    formDataMultiPart.field(FEEDS, entity, MediaType.APPLICATION_XML_TYPE);
                } else if (EntityType.PROCESS.equals(entity.getEntityType())) {
                    formDataMultiPart.field(PROCESSES, entity, MediaType.APPLICATION_XML_TYPE);
                }
            }
        }
        formDataMultiPart.field(CONFIG, configStream, MediaType.APPLICATION_OCTET_STREAM_TYPE);
        try {
            formDataMultiPart.close();
        } catch (IOException e) {
            LOG.error("Submit failed. Failed to submit entities. Cause: ", e);
            throw new FalconCLIException("Submit failed. Failed to submit entities:" + e.getMessage(), e);
        }
        return formDataMultiPart;
    }

    private List<Entity> validateExtensionAndGetEntities(String extensionName, String jobName,
                                                         InputStream configStream) {
        JSONObject extensionDetailJson;
        if (StringUtils.isBlank(extensionName)) {
            extensionName = ExtensionHandler.getExtensionName(jobName, getExtensionJobDetailJson(jobName));
        }
        extensionDetailJson = getExtensionDetailJson(extensionName);
        String extensionType = ExtensionHandler.getExtensionType(extensionName, extensionDetailJson);
        String extensionBuildLocation = ExtensionHandler.getExtensionLocation(extensionName, extensionDetailJson);
        return getEntities(extensionName, jobName, configStream, extensionType,
                extensionBuildLocation);
    }

    private JSONObject getExtensionDetailJson(String extensionName) {
        ClientResponse clientResponse = getExtensionDetailResponse(extensionName);

        JSONObject extensionDetailJson;
        try {
            extensionDetailJson = new JSONObject(getResponse(APIResult.class, clientResponse).getMessage());
        } catch (JSONException e) {
            LOG.error("Failed to get details for the given extension. Cause: ", e);
            throw new FalconCLIException("Failed to get details for the given extension:" + e.getMessage(), e);
        }
        return extensionDetailJson;
    }

    private JSONObject getExtensionJobDetailJson(String jobName) {
        ClientResponse clientResponse = getExtensionJobDetailsResponse(jobName);
        JSONObject extensionJobDetailJson;
        try {
            extensionJobDetailJson = new JSONObject(getResponse(APIResult.class, clientResponse).getMessage());
        } catch (JSONException e) {
            LOG.error("Failed to get details for the given extension. Cause: ", e);
            throw new FalconCLIException("Failed to get details for the given extension:" + e.getMessage(), e);
        }
        return extensionJobDetailJson;
    }

    private List<Entity> getEntities(String extensionName, String jobName, InputStream configStream,
                                     String extensionType, String extensionBuildLocation) {
        List<Entity> entities = null;
        if (!extensionType.equals(ExtensionType.TRUSTED.toString())) {
            try {
                entities = ExtensionHandler.loadAndPrepare(extensionName, jobName, configStream,
                        extensionBuildLocation);
            } catch (Exception e) {
                LOG.error("Error in building the extension. Cause: ", e);
                OUT.get().println("Error in building the extension:" + ExceptionUtils.getFullStackTrace(e));
                throw new FalconCLIException("Error in building the extension:" + e.getMessage(), e);
            }
            if (entities == null || entities.isEmpty()) {
                throw new FalconCLIException("No entities got built for the given extension");
            }
        }
        return entities;
    }

    public APIResult submitAndScheduleExtensionJob(final String extensionName, final String jobName,
                                                   final String configPath, final String doAsUser)  {
        FormDataMultiPart entitiesForm = getEntitiesForm(extensionName, jobName, configPath);
        ClientResponse clientResponse = new ResourceBuilder()
                .path(ExtensionOperations.SUBMIT_AND_SCHEDULE.path, extensionName)
                .addQueryParam(JOB_NAME_OPT, jobName)
                .addQueryParam(DO_AS_OPT, doAsUser)
                .call(ExtensionOperations.SUBMIT_AND_SCHEDULE, entitiesForm);
        return getResponse(APIResult.class, clientResponse);
    }

    public APIResult updateExtensionJob(final String jobName, final String configPath, final String doAsUser) {
        FormDataMultiPart entitiesForm = getEntitiesForm(null, jobName, configPath);
        ClientResponse clientResponse = new ResourceBuilder()
                .path(ExtensionOperations.UPDATE.path, jobName)
                .addQueryParam(DO_AS_OPT, doAsUser)
                .call(ExtensionOperations.UPDATE, entitiesForm);
        return getResponse(APIResult.class, clientResponse);
    }

    public APIResult validateExtensionJob(final String extensionName, final String jobName,
                                          final String configPath, final String doAsUser) {
        String extensionType = ExtensionHandler.getExtensionType(extensionName, getExtensionDetailJson(extensionName));
        InputStream configStream = getServletInputStream(configPath);
        if (extensionType.equals(ExtensionType.TRUSTED.toString())) {
            ClientResponse clientResponse = new ResourceBuilder()
                    .path(ExtensionOperations.VALIDATE.path, extensionName)
                    .addQueryParam(DO_AS_OPT, doAsUser)
                    .call(ExtensionOperations.VALIDATE, configStream);
            return getResponse(APIResult.class, clientResponse);
        } else {
            validateExtensionAndGetEntities(extensionName, jobName, configStream);
            return new APIResult(APIResult.Status.SUCCEEDED, "Validated successfully");
        }
    }

    public APIResult scheduleExtensionJob(String jobName, final String coloExpr, final String doAsUser)  {
        ClientResponse clientResponse = new ResourceBuilder()
                .path(ExtensionOperations.SCHEDULE.path, jobName)
                .addQueryParam(COLO, coloExpr)
                .addQueryParam(DO_AS_OPT, doAsUser)
                .call(ExtensionOperations.SCHEDULE);
        return getResponse(APIResult.class, clientResponse);
    }

    public APIResult suspendExtensionJob(final String jobName, final String coloExpr, final String doAsUser)  {
        ClientResponse clientResponse = new ResourceBuilder()
                .path(ExtensionOperations.SUSPEND.path, jobName)
                .addQueryParam(COLO, coloExpr)
                .addQueryParam(DO_AS_OPT, doAsUser)
                .call(ExtensionOperations.SUSPEND);
        return getResponse(APIResult.class, clientResponse);
    }

    public APIResult resumeExtensionJob(final String jobName, final String coloExpr, final String doAsUser)  {
        ClientResponse clientResponse = new ResourceBuilder()
                .path(ExtensionOperations.RESUME.path, jobName)
                .addQueryParam(COLO, coloExpr)
                .addQueryParam(DO_AS_OPT, doAsUser)
                .call(ExtensionOperations.RESUME);
        return getResponse(APIResult.class, clientResponse);
    }

    public APIResult deleteExtensionJob(final String jobName, final String doAsUser)  {
        ClientResponse clientResponse = new ResourceBuilder()
                .path(ExtensionOperations.DELETE.path, jobName)
                .addQueryParam(DO_AS_OPT, doAsUser)
                .call(ExtensionOperations.DELETE);
        return getResponse(APIResult.class, clientResponse);
    }

    public ExtensionInstanceList listExtensionInstance(final String jobName, final String doAsUser, final String fields,
                                                       final String start, final String end, final String status,
                                                       final String orderBy, final String sortOrder,
                                                       final String offset, final String numResults) {
        ClientResponse clientResponse = new ResourceBuilder()
                .path(ExtensionOperations.INSTANCES.path, jobName)
                .addQueryParam(DO_AS_OPT, doAsUser)
                .addQueryParam(FIELDS, fields)
                .addQueryParam(START, start)
                .addQueryParam(END, end)
                .addQueryParam(INSTANCE_STATUS, status)
                .addQueryParam(ORDER_BY, orderBy)
                .addQueryParam(SORT_ORDER, sortOrder)
                .addQueryParam(OFFSET, offset)
                .addQueryParam(NUM_RESULTS, numResults)
                .call(ExtensionOperations.INSTANCES);
        return getResponse(ExtensionInstanceList.class, clientResponse);
    }

    private String sendExtensionRequest(final ExtensionOperations operation,
                                        final String extensionName)  {
        WebResource resource;
        switch (operation) {
        case ENUMERATE:
            resource = service.path(operation.path);
            break;

        case DESCRIBE:
        case DEFINITION:
            resource = service.path(operation.path).path(extensionName);
            break;

        default:
            throw new FalconCLIException("Invalid extension client Operation " + operation.toString());
        }

        ClientResponse clientResponse = resource
                .header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                .accept(operation.mimeType).type(operation.mimeType)
                .method(operation.method, ClientResponse.class);

        checkIfSuccessful(clientResponse);
        return clientResponse.getEntity(String.class);
    }

    private String sendMetadataLineageRequest(MetadataOperations job, String id,
                                              String doAsUser) {
        ClientResponse clientResponse = new ResourceBuilder().path(job.path, id).addQueryParam(DO_AS_OPT, doAsUser)
            .call(job);
        return getResponseAsString(clientResponse);
    }

    private String sendMetadataLineageRequest(MetadataOperations job, String key,
                                              String value, String doAsUser) {
        ClientResponse clientResponse = new ResourceBuilder().path(job.path).addQueryParam(DO_AS_OPT, doAsUser)
            .addQueryParam(KEY, key).addQueryParam(VALUE, value).call(job);
        return getResponseAsString(clientResponse);
    }

    private String sendMetadataLineageRequestForEdges(MetadataOperations job, String id,
                                                      String direction, String doAsUser) {
        ClientResponse clientResponse = new ResourceBuilder().path(job.path, id, direction)
            .addQueryParam(DO_AS_OPT, doAsUser).call(job);
        return getResponseAsString(clientResponse);
    }

    private void checkIfSuccessful(ClientResponse clientResponse) {
        Response.Status.Family statusFamily = clientResponse.getClientResponseStatus().getFamily();
        if (statusFamily != Response.Status.Family.SUCCESSFUL && statusFamily != Response.Status.Family.INFORMATIONAL) {
            throw FalconCLIException.fromReponse(clientResponse);
        }
    }

    private void printClientResponse(ClientResponse clientResponse) {
        if (getDebugMode()) {
            OUT.get().println(clientResponse.toString());
        }
    }
}
