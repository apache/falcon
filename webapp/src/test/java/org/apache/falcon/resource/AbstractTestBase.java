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
import org.apache.commons.lang.RandomStringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.cluster.util.StandAloneCluster;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.util.EmbeddedServer;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.engine.OozieClientFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.oozie.client.*;
import org.apache.oozie.client.Job.Status;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;

import javax.servlet.ServletInputStream;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base test class for CLI, Entity and Process Instances.
 */
public class AbstractTestBase {
    protected static final String FEED_TEMPLATE1 = "/feed-template1.xml";
    protected static final String FEED_TEMPLATE2 = "/feed-template2.xml";

    protected String clusterFileTemplate = "/cluster-template.xml";

    protected static final String SAMPLE_PROCESS_XML = "/process-version-0.xml";
    protected static final String PROCESS_TEMPLATE = "/process-template.xml";

    protected static final String BASE_URL = "http://localhost:15000/";
    protected static final String REMOTE_USER = System.getProperty("user.name");

    protected EmbeddedServer server;

    protected Unmarshaller unmarshaller;
    protected Marshaller marshaller;

    protected EmbeddedCluster cluster;
    protected WebResource service = null;
    protected String clusterName;
    protected String processName;
    protected String outputFeedName;

    private static final Pattern VAR_PATTERN = Pattern.compile("##[A-Za-z0-9_]*##");

    protected void scheduleProcess(String processTemplate, Map<String, String> overlay) throws Exception {
        ClientResponse response = submitToFalcon(clusterFileTemplate, overlay, EntityType.CLUSTER);
        assertSuccessful(response);

        response = submitToFalcon(FEED_TEMPLATE1, overlay, EntityType.FEED);
        assertSuccessful(response);

        response = submitToFalcon(FEED_TEMPLATE2, overlay, EntityType.FEED);
        assertSuccessful(response);

        response = submitToFalcon(processTemplate, overlay, EntityType.PROCESS);
        assertSuccessful(response);
        ClientResponse clientRepsonse = this.service.path("api/entities/schedule/process/" + processName)
                .header("Remote-User", REMOTE_USER).accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML).post(
                        ClientResponse.class);
        assertSuccessful(clientRepsonse);
    }

    protected void scheduleProcess() throws Exception {
        scheduleProcess(PROCESS_TEMPLATE, getUniqueOverlay());
    }

    private List<WorkflowJob> getRunningJobs(String entityName) throws Exception {
        OozieClient ozClient = OozieClientFactory.get(
                (Cluster) ConfigurationStore.get().get(EntityType.CLUSTER, clusterName));
        StringBuilder builder = new StringBuilder();
        builder.append(OozieClient.FILTER_STATUS).append('=').append(Job.Status.RUNNING).append(';');
        builder.append(OozieClient.FILTER_NAME).append('=').append("FALCON_PROCESS_DEFAULT_").append(entityName);
        return ozClient.getJobsInfo(builder.toString());
    }

    protected void waitForWorkflowStart(String entityName) throws Exception {
        for (int i = 0; i < 10; i++) {
            List<WorkflowJob> jobs = getRunningJobs(entityName);
            if (jobs != null && !jobs.isEmpty()) {
                return;
            }

            System.out.println("Waiting for workflow to start");
            Thread.sleep(i * 1000);
        }
        throw new Exception("Workflow for " + entityName + " hasn't started in oozie");
    }

    protected void waitForProcessWFtoStart() throws Exception {
        waitForWorkflowStart(processName);
    }

    protected void waitForOutputFeedWFtoStart() throws Exception {
        waitForWorkflowStart(outputFeedName);
    }

    protected void waitForBundleStart(Status status) throws Exception {
        OozieClient ozClient = OozieClientFactory.get(clusterName);
        List<BundleJob> bundles = getBundles();
        if (bundles.isEmpty()) {
            return;
        }

        String bundleId = bundles.get(0).getId();
        for (int i = 0; i < 15; i++) {
            Thread.sleep(i * 1000);
            BundleJob bundle = ozClient.getBundleJobInfo(bundleId);
            if (bundle.getStatus() == status) {
                if (status == Status.FAILED) {
                    return;
                }

                boolean done = false;
                for (CoordinatorJob coord : bundle.getCoordinators()) {
                    if (coord.getStatus() == status) {
                        done = true;
                    }
                }
                if (done) {
                    return;
                }
            }
            System.out.println("Waiting for bundle " + bundleId + " in " + status + " state");
        }
        throw new Exception("Bundle " + bundleId + " is not " + status + " in oozie");
    }

    public AbstractTestBase() {
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(APIResult.class, Feed.class, Process.class, Cluster.class,
                    InstancesResult.class);
            unmarshaller = jaxbContext.createUnmarshaller();
            marshaller = jaxbContext.createMarshaller();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public void configure() throws Exception {
        StartupProperties.get().setProperty(
                "application.services",
                StartupProperties.get().getProperty("application.services")
                        .replace("org.apache.falcon.service.ProcessSubscriberService", ""));
        String store = StartupProperties.get().getProperty("config.store.uri");
        StartupProperties.get().setProperty("config.store.uri", store + System.currentTimeMillis());
        if (new File("webapp/src/main/webapp").exists()) {
            this.server = new EmbeddedServer(15000, "webapp/src/main/webapp");
        } else if (new File("src/main/webapp").exists()) {
            this.server = new EmbeddedServer(15000, "src/main/webapp");
        } else {
            throw new RuntimeException("Cannot run jersey tests");
        }
        ClientConfig config = new DefaultClientConfig();
        Client client = Client.create(config);
        this.service = client.resource(UriBuilder.fromUri(BASE_URL).build());
        this.server.start();

        if (System.getProperty("falcon.test.hadoop.embedded", "true").equals("true")) {
            clusterFileTemplate = "target/cluster-template.xml";
            this.cluster = EmbeddedCluster.newCluster("##cluster##", true);
            Cluster clusterEntity = this.cluster.getCluster();
            FileOutputStream out = new FileOutputStream(clusterFileTemplate);
            marshaller.marshal(clusterEntity, out);
            out.close();
        } else {
            Map<String, String> overlay = new HashMap<String, String>();
            overlay.put("cluster", RandomStringUtils.randomAlphabetic(5));
            String file = overlayParametersOverTemplate(clusterFileTemplate, overlay);
            this.cluster = StandAloneCluster.newCluster(file);
            clusterName = cluster.getCluster().getName();
        }

        cleanupStore();

        // setup dependent workflow and lipath in hdfs
        FileSystem fs = FileSystem.get(this.cluster.getConf());
        fs.mkdirs(new Path("/falcon"), new FsPermission((short) 511));

        Path wfParent = new Path("/falcon/test");
        fs.delete(wfParent, true);
        Path wfPath = new Path(wfParent, "workflow");
        fs.mkdirs(wfPath);
        fs.copyFromLocalFile(false, true, new Path(this.getClass().getResource("/fs-workflow.xml").getPath()),
                new Path(wfPath,
                        "workflow.xml"));
        fs.mkdirs(new Path(wfParent, "input/2012/04/20/00"));
        Path outPath = new Path(wfParent, "output");
        fs.mkdirs(outPath);
        fs.setPermission(outPath, new FsPermission((short) 511));
    }

    /**
     * Converts a InputStream into ServletInputStream.
     *
     * @param fileName
     * @return ServletInputStream
     * @throws java.io.IOException
     */
    protected ServletInputStream getServletInputStream(String fileName) throws IOException {
        return getServletInputStream(new FileInputStream(fileName));
    }

    protected ServletInputStream getServletInputStream(final InputStream stream) {
        return new ServletInputStream() {

            @Override
            public int read() throws IOException {
                return stream.read();
            }
        };
    }

    public void tearDown() throws Exception {
        this.cluster.shutdown();
        server.stop();
    }

    public void cleanupStore() throws Exception {
        for (EntityType type : EntityType.values()) {
            for (String name : ConfigurationStore.get().getEntities(type)) {
                ConfigurationStore.get().remove(type, name);
            }
        }
    }

    protected ClientResponse submitAndSchedule(String template, Map<String, String> overlay, EntityType entityType)
        throws Exception {
        String tmpFile = overlayParametersOverTemplate(template, overlay);
        ServletInputStream rawlogStream = getServletInputStream(tmpFile);

        return this.service.path("api/entities/submitAndSchedule/" + entityType.name().toLowerCase())
                .header("Remote-User", "testuser").accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
                .post(ClientResponse.class, rawlogStream);
    }

    protected ClientResponse submitToFalcon(String template, Map<String, String> overlay, EntityType entityType)
        throws IOException {
        String tmpFile = overlayParametersOverTemplate(template, overlay);
        return submitFileToFalcon(entityType, tmpFile);
    }

    private ClientResponse submitFileToFalcon(EntityType entityType, String tmpFile) throws IOException {

        ServletInputStream rawlogStream = getServletInputStream(tmpFile);

        return this.service.path("api/entities/submit/" + entityType.name().toLowerCase()).header("Remote-User",
                "testuser")
                .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML).post(ClientResponse.class, rawlogStream);
    }

    protected void assertRequestId(ClientResponse clientRepsonse) {
        String response = clientRepsonse.getEntity(String.class);
        try {
            APIResult result = (APIResult) unmarshaller.unmarshal(new StringReader(response));
            Assert.assertNotNull(result.getRequestId());
        } catch (JAXBException e) {
            Assert.fail("Reponse " + response + " is not valid");
        }
    }

    protected void assertStatus(ClientResponse clientRepsonse, APIResult.Status status) {
        String response = clientRepsonse.getEntity(String.class);
        try {
            APIResult result = (APIResult) unmarshaller.unmarshal(new StringReader(response));
            Assert.assertEquals(result.getStatus(), status);
        } catch (JAXBException e) {
            Assert.fail("Reponse " + response + " is not valid");
        }
    }

    protected void assertFailure(ClientResponse clientRepsonse) {
        Assert.assertEquals(clientRepsonse.getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
        assertStatus(clientRepsonse, APIResult.Status.FAILED);
    }

    protected void assertSuccessful(ClientResponse clientRepsonse) {
        Assert.assertEquals(clientRepsonse.getStatus(), Response.Status.OK.getStatusCode());
        assertStatus(clientRepsonse, APIResult.Status.SUCCEEDED);
    }

    protected String overlayParametersOverTemplate(String template, Map<String, String> overlay) throws IOException {
        File tmpFile = getTempFile();
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
        return tmpFile.getAbsolutePath();
    }

    protected File getTempFile() throws IOException {
        File target = new File("webapp/target");
        if (!target.exists()) {
            target = new File("target");
        }

        return File.createTempFile("test", ".xml", target);
    }

    protected List<BundleJob> getBundles() throws Exception {
        List<BundleJob> bundles = new ArrayList<BundleJob>();
        if (clusterName == null) {
            return bundles;
        }

        OozieClient ozClient = OozieClientFactory.get(cluster.getCluster());
        return ozClient.getBundleJobsInfo("name=FALCON_PROCESS_" + processName, 0, 10);
    }

    @AfterClass
    public void cleanup() throws Exception {
        tearDown();
        cleanupStore();
    }

    @AfterMethod
    public boolean killOozieJobs() throws Exception {
        if (clusterName == null) {
            return true;
        }

        OozieClient ozClient = OozieClientFactory.get(cluster.getCluster());
        List<BundleJob> bundles = getBundles();
        if (bundles != null) {
            for (BundleJob bundle : bundles) {
                ozClient.kill(bundle.getId());
            }
        }
        return false;
    }

    protected Map<String, String> getUniqueOverlay() throws FalconException {
        Map<String, String> overlay = new HashMap<String, String>();
        long time = System.currentTimeMillis();
        clusterName = "cluster" + time;
        overlay.put("cluster", clusterName);
        overlay.put("inputFeedName", "in" + time);
        //only feeds with future dates can be scheduled
        Date endDate = new Date(System.currentTimeMillis() + 15 * 60 * 1000);
        overlay.put("feedEndDate", SchemaHelper.formatDateUTC(endDate));
        overlay.put("outputFeedName", "out" + time);
        processName = "p" + time;
        overlay.put("processName", processName);
        outputFeedName = "out" + time;
        return overlay;
    }
}
