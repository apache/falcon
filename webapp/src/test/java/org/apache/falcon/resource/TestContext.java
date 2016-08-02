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

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.urlconnection.HTTPSProperties;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.util.TrustManagerUtils;
import org.apache.falcon.FalconCLIConstants;
import org.apache.falcon.FalconException;
import org.apache.falcon.FalconRuntimException;
import org.apache.falcon.catalog.HiveCatalogService;
import org.apache.falcon.cli.FalconCLI;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.client.FalconClient;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.ClusterLocationType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.util.DeploymentUtil;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hive.hcatalog.api.HCatClient;
import org.testng.Assert;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.servlet.ServletInputStream;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.security.SecureRandom;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base test class for CLI, Entity and Process Instances.
 */
public class TestContext extends AbstractTestContext {

    public static final String DATASOURCE_TEMPLATE1 = "/datasource-template1.xml";
    public static final String DATASOURCE_TEMPLATE2 = "/datasource-template2.xml";
    public static final String DATASOURCE_TEMPLATE3 = "/datasource-template3.xml";
    public static final String DATASOURCE_TEMPLATE4 = "/datasource-template4.xml";
    public static final String DATASOURCE_TEMPLATE5 = "/datasource-template5.xml";
    public static final String CLUSTER_TEMPLATE = "/cluster-template.xml";
    public static final String CLUSTER_UPDATED_TEMPLATE = "/cluster-updated-template.xml";
    public static final String PIG_PROCESS_TEMPLATE = "/pig-process-template.xml";

    public static final String BASE_URL = "https://localhost:41443/falcon-webapp";
    public static final String REMOTE_USER = FalconClient.USER;

    private static final String AUTH_COOKIE_EQ = AuthenticatedURL.AUTH_COOKIE + "=";

    protected Unmarshaller unmarshaller;
    protected Marshaller marshaller;

    protected EmbeddedCluster cluster;
    protected WebResource service = null;
    private AuthenticatedURL.Token authenticationToken;

    protected String clusterName;
    protected String processName;
    protected String processEndTime;
    protected String inputFeedName;
    protected String outputFeedName;

    public static final Pattern VAR_PATTERN = Pattern.compile("##[A-Za-z0-9_.]*##");

    public TestContext() {
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(APIResult.class, Feed.class, Process.class, Cluster.class,
                    InstancesResult.class);
            unmarshaller = jaxbContext.createUnmarshaller();
            marshaller = jaxbContext.createMarshaller();
            configure();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This is only used for tests.
     *
     * @param metastoreUrl metastore url
     * @return client object
     * @throws FalconException
     */
    public static HCatClient getHCatClient(String metastoreUrl) throws FalconException {
        try {
            HiveConf hcatConf = HiveCatalogService.createHiveConf(new Configuration(false), metastoreUrl);
            return HCatClient.create(hcatConf);
        } catch (Exception e) {
            throw new FalconException("Exception creating HCatClient: " + e.getMessage(), e);
        }
    }

    public void configure() throws Exception {
        try {
            StartupProperties.get().setProperty(
                    "application.services",
                    StartupProperties.get().getProperty("application.services")
                            .replace("org.apache.falcon.service.ProcessSubscriberService", ""));
            String store = StartupProperties.get().getProperty("config.store.uri");
            StartupProperties.get().setProperty("config.store.uri", store + System.currentTimeMillis());
            SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(
                    null,
                    new TrustManager[]{TrustManagerUtils.getValidateServerCertificateTrustManager()},
                    new SecureRandom());
            DefaultClientConfig config = new DefaultClientConfig();
            config.getProperties().put(HTTPSProperties.PROPERTY_HTTPS_PROPERTIES,
                    new HTTPSProperties(new HostnameVerifier() {
                        @Override
                        public boolean verify(String hostname, SSLSession sslSession) {
                            return true;
                        }
                    }, sslContext));
            Client client = Client.create(config);
            this.service = client.resource(UriBuilder.fromUri(BASE_URL).build());
        } catch (Exception e) {
            throw new FalconRuntimException(e);
        }

        try {
            String baseUrl = BASE_URL;
            if (!baseUrl.endsWith("/")) {
                baseUrl += "/";
            }
            this.authenticationToken = FalconClient.getToken(baseUrl);
        } catch (FalconCLIException e) {
            throw new AuthenticationException(e);
        }

        ClientConfig config = new DefaultClientConfig();
        Client client = Client.create(config);
        client.setReadTimeout(500000);
        client.setConnectTimeout(500000);
        this.service = client.resource(UriBuilder.fromUri(BASE_URL).build());
    }

    public void setCluster(String cName) throws Exception {
        cluster = EmbeddedCluster.newCluster(cName, true);
        this.clusterName = cluster.getCluster().getName();
    }

    public EmbeddedCluster getCluster() {
        return cluster;
    }

    public WebResource getService() {
        return service;
    }

    public String getAuthenticationToken() {
        return AUTH_COOKIE_EQ + authenticationToken;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getProcessName() {
        return processName;
    }

    public String getProcessEndTime() {
        return processEndTime;
    }

    public String getInputFeedName() {
        return inputFeedName;
    }

    public String getOutputFeedName() {
        return outputFeedName;
    }

    public String getClusterFileTemplate() {
        return CLUSTER_TEMPLATE;
    }

    public void scheduleProcess(String processTemplate, Map<String, String> overlay) throws Exception {
        scheduleProcess(processTemplate, overlay, true, null, "", null);
    }

    public void scheduleProcess(String processTemplate, Map<String, String> overlay,
                                Boolean skipDryRun, final String doAsUSer, String properties) throws Exception {
        scheduleProcess(processTemplate, overlay, true, skipDryRun, doAsUSer, properties);
    }

    public void scheduleProcess(String processTemplate, Map<String, String> overlay, boolean succeed) throws Exception{
        scheduleProcess(processTemplate, overlay, succeed, null, "", null);
    }

    public void scheduleProcess(String processTemplate, Map<String, String> overlay, boolean succeed,
                                Boolean skipDryRun, final String doAsUser, String properties) throws Exception {
        ClientResponse response = submitToFalcon(CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        assertSuccessful(response);

        response = submitToFalcon(FEED_TEMPLATE1, overlay, EntityType.FEED);
        assertSuccessful(response);

        response = submitToFalcon(FEED_TEMPLATE2, overlay, EntityType.FEED);
        assertSuccessful(response);

        response = submitAndSchedule(processTemplate, overlay, EntityType.PROCESS, skipDryRun, doAsUser, properties);
        if (succeed) {
            assertSuccessful(response);
        } else {
            assertFailure(response);
        }
    }

    public void scheduleProcess() throws Exception {
        scheduleProcess(PROCESS_TEMPLATE, getUniqueOverlay());
    }

    /**
     * Converts a InputStream into ServletInputStream.
     *
     * @param fileName file name
     * @return ServletInputStream
     * @throws java.io.IOException
     */
    public ServletInputStream getServletInputStream(String fileName) throws IOException {
        return getServletInputStream(new FileInputStream(fileName));
    }

    public ServletInputStream getServletInputStream(final InputStream stream) {
        return new ServletInputStream() {

            @Override
            public int read() throws IOException {
                return stream.read();
            }
        };
    }

    public ExtensionJobList getExtensionJobs(String extensionName, String fields, String sortOrder, String offset,
                                             String resultsPerPage, String doAsUser) {
        WebResource resource = this.service.path("api/extension/list/" + extensionName);
        resource = addQueryParam(resource, "doAs", doAsUser);
        resource = addQueryParam(resource, "fields", fields);
        resource = addQueryParam(resource, "sortOrder", sortOrder);
        resource = addQueryParam(resource, "offset", offset);
        resource = addQueryParam(resource, "numResults", resultsPerPage);
        ClientResponse response = resource.header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                .accept(MediaType.APPLICATION_JSON).type(MediaType.TEXT_XML)
                .method(HttpMethod.GET, ClientResponse.class);
        return response.getEntity(ExtensionJobList.class);
    }

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    public ExtensionInstanceList getExtensionInstances(String jobName, String nominalStart, String nominalEnd,
                                                       String instanceStatus, String fields, String orderBy,
                                                       String sortOrder, String offset, String resultsPerPage,
                                                       String doAsUser) {
        WebResource resource = this.service.path("api/extension/instances/" + jobName);
        resource = addQueryParam(resource, "start", nominalStart);
        resource = addQueryParam(resource, "end", nominalEnd);
        resource = addQueryParam(resource, "instanceStatus", instanceStatus);
        resource = addQueryParam(resource, "doAs", doAsUser);
        resource = addQueryParam(resource, "fields", fields);
        resource = addQueryParam(resource, "orderBy", orderBy);
        resource = addQueryParam(resource, "sortOrder", sortOrder);
        resource = addQueryParam(resource, "offset", offset);
        resource = addQueryParam(resource, "numResults", resultsPerPage);
        ClientResponse response = resource.header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                .accept(MediaType.APPLICATION_JSON).type(MediaType.TEXT_XML)
                .method(HttpMethod.GET, ClientResponse.class);
        return response.getEntity(ExtensionInstanceList.class);
    }

    public void waitForInstancesToStart(String entityType, String entityName, long timeout) {
        long mustEnd = System.currentTimeMillis() + timeout;
        WebResource resource = this.service.path("api/instance/running/" + entityType + "/" + entityName);
        InstancesResult instancesResult;
        while (System.currentTimeMillis() < mustEnd) {
            ClientResponse response = resource.header("Cookie", AUTH_COOKIE_EQ + authenticationToken)
                    .accept(MediaType.APPLICATION_JSON).type(MediaType.TEXT_XML)
                    .method(HttpMethod.GET, ClientResponse.class);
            instancesResult = response.getEntity(InstancesResult.class);
            if (instancesResult.getInstances() != null && instancesResult.getInstances().length > 0) {
                break;
            }
        }
    }

    public ClientResponse submitAndSchedule(String template, Map<String, String> overlay, EntityType entityType)
        throws Exception {
        return submitAndSchedule(template, overlay, entityType, null, "", null);
    }

    public ClientResponse submitAndSchedule(String template, Map<String, String> overlay,
                                            EntityType entityType, Boolean skipDryRun,
                                            final String doAsUser, String properties) throws Exception {
        String tmpFile = overlayParametersOverTemplate(template, overlay);
        ServletInputStream rawlogStream = getServletInputStream(tmpFile);

        WebResource resource = this.service.path("api/entities/submitAndSchedule/" + entityType.name().toLowerCase());

        if (null != skipDryRun) {
            resource = resource.queryParam("skipDryRun", String.valueOf(skipDryRun));
        }

        if (StringUtils.isNotEmpty(doAsUser)) {
            resource = resource.queryParam(FalconCLIConstants.DO_AS_OPT, doAsUser);
        }

        if (StringUtils.isNotEmpty(properties)) {
            resource = resource.queryParam("properties", properties);
        }

        return resource.header("Cookie", getAuthenticationToken())
                .accept(MediaType.TEXT_XML)
                .type(MediaType.TEXT_XML)
                .post(ClientResponse.class, rawlogStream);
    }

    public ClientResponse validate(String template, Map<String, String> overlay, EntityType entityType)
        throws Exception {
        return validate(template, overlay, entityType, null);
    }

    public ClientResponse validate(String template, Map<String, String> overlay,
                                   EntityType entityType, Boolean skipDryRun)
        throws Exception {
        String tmpFile = overlayParametersOverTemplate(template, overlay);
        ServletInputStream rawlogStream = getServletInputStream(tmpFile);

        WebResource resource = service.path("api/entities/validate/" + entityType.name().toLowerCase());
        if (null != skipDryRun) {
            resource = resource.queryParam("skipDryRun", String.valueOf(skipDryRun));
        }
        return resource
            .header("Cookie", getAuthenticationToken())
            .accept(MediaType.TEXT_XML)
            .type(MediaType.TEXT_XML)
            .post(ClientResponse.class, rawlogStream);
    }

    public ClientResponse submitToFalcon(String template, Map<String, String> overlay, EntityType entityType)
        throws IOException {
        return submitToFalcon(template, overlay, entityType, "");
    }

    public ClientResponse submitToFalcon(String template, Map<String, String> overlay, EntityType entityType,
                                         final String doAsUser) throws IOException {
        String tmpFile = overlayParametersOverTemplate(template, overlay);
        if (entityType == EntityType.CLUSTER) {
            try {
                cluster = EmbeddedCluster.newCluster(overlay.get("cluster"), true);
                clusterName = cluster.getCluster().getName();
                deleteClusterLocations(cluster.getCluster(), cluster.getFileSystem());
                createClusterLocations(cluster.getCluster(), cluster.getFileSystem());
            } catch (Exception e) {
                throw new IOException("Unable to setup cluster info", e);
            }
        }
        return submitFileToFalcon(entityType, tmpFile, doAsUser);
    }

    public static void deleteClusterLocations(Cluster clusterEntity, FileSystem fs) throws IOException {
        String stagingLocation = ClusterHelper.getLocation(clusterEntity, ClusterLocationType.STAGING).getPath();
        Path stagingPath = new Path(stagingLocation);
        if (fs.exists(stagingPath)) {
            fs.delete(stagingPath, true);
        }

        String workingLocation = ClusterHelper.getLocation(clusterEntity, ClusterLocationType.WORKING).getPath();
        Path workingPath = new Path(workingLocation);
        if (fs.exists(workingPath)) {
            fs.delete(workingPath, true);
        }
    }

    public static void createClusterLocations(Cluster clusterEntity, FileSystem fs) throws IOException {
        createClusterLocations(clusterEntity, fs, true);
    }

    public static void createClusterLocations(Cluster clusterEntity, FileSystem fs, boolean withWorking)
        throws IOException {
        String stagingLocation = ClusterHelper.getLocation(clusterEntity, ClusterLocationType.STAGING).getPath();
        Path stagingPath = new Path(stagingLocation);
        if (fs.exists(stagingPath)) {
            fs.delete(stagingPath, true);
        }
        HadoopClientFactory.mkdirs(fs, stagingPath, HadoopClientFactory.ALL_PERMISSION);
        if (withWorking) {
            String workingLocation = ClusterHelper.getLocation(clusterEntity, ClusterLocationType.WORKING).getPath();
            Path workingPath = new Path(workingLocation);
            if (fs.exists(workingPath)) {
                fs.delete(workingPath, true);
            }
            HadoopClientFactory.mkdirs(fs, workingPath, HadoopClientFactory.READ_EXECUTE_PERMISSION);
        }
    }

    public ClientResponse submitFileToFalcon(EntityType entityType, String tmpFile) throws IOException {
        return submitFileToFalcon(entityType, tmpFile, "");
    }

    public ClientResponse submitFileToFalcon(EntityType entityType, String tmpFile,
                                             final String doAsUser) throws IOException {

        ServletInputStream rawlogStream = getServletInputStream(tmpFile);

        WebResource resource = this.service.path("api/entities/submit/" + entityType.name().toLowerCase());

        if (StringUtils.isNotEmpty(doAsUser)) {
            resource = resource.queryParam(FalconCLIConstants.DO_AS_OPT, doAsUser);
        }

        return resource.header("Cookie", getAuthenticationToken())
                .accept(MediaType.TEXT_XML)
                .type(MediaType.TEXT_XML)
                .post(ClientResponse.class, rawlogStream);
    }

    public ClientResponse deleteFromFalcon(String entityName, String entityType) throws IOException{
        return this.service.path("api/entities/delete/" + entityType + "/" + entityName.toLowerCase())
                .header("Cookie", getAuthenticationToken())
                .accept(MediaType.TEXT_XML)
                .delete(ClientResponse.class);
    }

    public void assertStatus(ClientResponse clientResponse, APIResult.Status status) {
        String response = clientResponse.getEntity(String.class);
        try {
            APIResult result = (APIResult) unmarshaller.unmarshal(new StringReader(response));
            Assert.assertEquals(result.getStatus(), status);
        } catch (JAXBException e) {
            Assert.fail("Response " + response + " is not valid");
        }
    }

    public void assertFailure(ClientResponse clientResponse) {
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
        assertStatus(clientResponse, APIResult.Status.FAILED);
    }

    public void assertSuccessful(ClientResponse clientResponse) {
        Assert.assertEquals(clientResponse.getStatus(), Response.Status.OK.getStatusCode());
        assertStatus(clientResponse, APIResult.Status.SUCCEEDED);
    }

    public static String overlayParametersOverTemplate(String template,
                                                       Map<String, String> overlay) throws IOException {
        return overlayParametersOverTemplate(getTempFile(), template, overlay);
    }

    public static String overlayParametersOverTemplate(File file, String template,
                                                       Map<String, String> overlay) throws IOException {
        OutputStream out = new FileOutputStream(file);

        InputStreamReader in;
        InputStream resourceAsStream = TestContext.class.getResourceAsStream(template);
        if (resourceAsStream == null) {
            in = new FileReader(template);
        } else {
            in = new InputStreamReader(resourceAsStream);
        }
        BufferedReader reader = new BufferedReader(in);
        String line;
        while ((line = reader.readLine()) != null) {
            Matcher matcher = VAR_PATTERN.matcher(line);
            while (matcher.find()) {
                String variable = line.substring(matcher.start(), matcher.end());
                line = line.replace(variable, overlay.get(variable.substring(2, variable.length() - 2)));
                matcher = VAR_PATTERN.matcher(line);
            }
            out.write(line.getBytes());
            out.write("\n".getBytes());
        }
        reader.close();
        out.close();
        return file.getAbsolutePath();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static File getTempFile(String path, String prefix, String suffix) throws IOException {
        File f = new File(path);
        if (!f.exists()) {
            f.mkdirs();
        }

        return File.createTempFile(prefix, suffix, f);
    }

    public Map<String, String> getUniqueOverlay() throws FalconException {
        Map<String, String> overlay = new HashMap<String, String>();
        long time = System.currentTimeMillis();
        clusterName = "cluster" + time;
        overlay.put("src.cluster.name", clusterName);
        overlay.put("cluster", clusterName);
        overlay.put("colo", DeploymentUtil.getCurrentColo());
        inputFeedName = "in" + time;
        overlay.put("inputFeedName", inputFeedName);
        //only feeds with future dates can be scheduled
        Date endDate = new Date(System.currentTimeMillis() + 15 * 60 * 1000);
        overlay.put("feedEndDate", SchemaHelper.formatDateUTC(endDate));
        processEndTime = SchemaHelper.formatDateUTC(endDate);
        overlay.put("processEndDate", processEndTime);
        outputFeedName = "out" + time;
        overlay.put("outputFeedName", outputFeedName);
        processName = "p" + time;
        overlay.put("processName", processName);
        overlay.put("user", System.getProperty("user.name"));
        overlay.put("workflow.path", "/falcon/test/workflow");
        overlay.put("workflow.lib.path", "/falcon/test/workflow/lib");
        return overlay;
    }

    public static void prepare() throws Exception {
        prepare(TestContext.CLUSTER_TEMPLATE, true);
    }

    public static void prepare(String clusterTemplate, boolean disableLineage) throws Exception {
        // setup a logged in user
        CurrentUser.authenticate(REMOTE_USER);

        if (disableLineage) {
            // disable recording lineage metadata
            String services = StartupProperties.get().getProperty("application.services");
            StartupProperties.get().setProperty("application.services",
                    services.replace("org.apache.falcon.metadata.MetadataMappingService", ""));
        }

        Map<String, String> overlay = new HashMap<String, String>();
        overlay.put("cluster", RandomStringUtils.randomAlphabetic(5));
        overlay.put("colo", DeploymentUtil.getCurrentColo());
        TestContext.overlayParametersOverTemplate(clusterTemplate, overlay);
        EmbeddedCluster cluster = EmbeddedCluster.newCluster(overlay.get("cluster"), true);

        cleanupStore();

        // setup dependent workflow and lipath in hdfs
        FileSystem fs = FileSystem.get(cluster.getConf());
        mkdir(fs, new Path("/falcon"), new FsPermission((short) 511));

        Path wfParent = new Path("/falcon/test");
        fs.delete(wfParent, true);
        Path wfPath = new Path(wfParent, "workflow");
        mkdir(fs, wfPath);
        mkdir(fs, new Path("/falcon/test/workflow/lib"));
        fs.copyFromLocalFile(false, true,
                new Path(TestContext.class.getResource("/fs-workflow.xml").getPath()),
                new Path(wfPath, "workflow.xml"));
        mkdir(fs, new Path(wfParent, "input/2012/04/20/00"));
        Path outPath = new Path(wfParent, "output");
        mkdir(fs, outPath, new FsPermission((short) 511));

        // init cluster locations
        initClusterLocations(cluster, fs);
    }

    private static void initClusterLocations(EmbeddedCluster cluster, FileSystem fs) throws Exception {
        String stagingPath = ClusterHelper.getLocation(cluster.getCluster(), ClusterLocationType.STAGING).getPath();
        mkdir(fs, new Path(stagingPath), HadoopClientFactory.ALL_PERMISSION);

        String workingPath = ClusterHelper.getLocation(cluster.getCluster(), ClusterLocationType.WORKING).getPath();
        mkdir(fs, new Path(workingPath), HadoopClientFactory.READ_EXECUTE_PERMISSION);
    }

    public static void cleanupStore() throws Exception {
        for (EntityType type : EntityType.values()) {
            for (String name : ConfigurationStore.get().getEntities(type)) {
                ConfigurationStore.get().remove(type, name);
            }
        }
    }

    public static void deleteEntitiesFromStore() throws Exception {
        for (EntityType type : EntityType.values()) {
            for (String name : ConfigurationStore.get().getEntities(type)) {
                executeWithURL("entity -delete -type " + type.name().toLowerCase() + " -name " + name);
            }
        }
    }

    public static int executeWithURL(String command) throws Exception {
        return new FalconCLI().run((command + " -url " + TestContext.BASE_URL).split("\\s+"));
    }

    private WebResource addQueryParam(WebResource resource, String key, String value) {
        if (StringUtils.isEmpty(value)) {
            return resource;
        }
        return resource.queryParam(key, value);
    }
}
