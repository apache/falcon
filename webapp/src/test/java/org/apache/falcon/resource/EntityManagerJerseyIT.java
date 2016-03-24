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

import com.sun.jersey.api.client.ClientResponse;
import org.apache.commons.io.FileUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Property;
import org.apache.falcon.entity.v0.process.Validity;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.unit.FalconUnit;
import org.apache.falcon.util.BuildProperties;
import org.apache.falcon.util.DeploymentProperties;
import org.apache.falcon.util.FalconTestUtil;
import org.apache.falcon.util.OozieTestUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.Job.Status;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

/**
 * Test class for Entity REST APIs.
 *
 * Tests should be enabled only in local environments as they need running instance of the web server.
 */
@Test(groups = {"exhaustive"})
public class EntityManagerJerseyIT extends AbstractSchedulerManagerJerseyIT {

    private static final String START_INSTANCE = "2012-04-20T00:00Z";
    private static final String SLEEP_WORKFLOW = "sleepWorkflow.xml";

    @BeforeClass
    @Override
    public void setup() throws Exception {
        String version = System.getProperty("project.version");
        String buildDir = System.getProperty("project.build.directory");
        System.setProperty("falcon.libext", buildDir + "/../../unit/target/falcon-unit-" + version + ".jar");
        super.setup();
    }

    @AfterMethod
    @Override
    public void cleanUpActionXml() throws IOException, FalconException {
        //Needed since oozie writes action xml to current directory.
        FileUtils.deleteQuietly(new File("action.xml"));
        FileUtils.deleteQuietly(new File(".action.xml.crc"));
        contexts.remove();
    }

    private ThreadLocal<UnitTestContext> contexts = new ThreadLocal<UnitTestContext>();

    private UnitTestContext newContext() throws FalconException, IOException {
        contexts.set(new UnitTestContext());
        return contexts.get();
    }

    static void assertLibs(FileSystem fs, Path path) throws IOException {
        FileStatus[] libs = fs.listStatus(path);
        Assert.assertNotNull(libs);
    }

    private Entity getDefinition(EntityType type, String name) throws Exception {
        Entity entity = falconUnitClient.getDefinition(type.name(), name, null);
        return entity;
    }

    private void updateEndtime(Process process) {
        Validity processValidity = process.getClusters().getClusters().get(0).getValidity();
        processValidity.setEnd(new Date(new Date().getTime() + 2 * 24 * 60 * 60 * 1000));
    }

    @Test
    public void testLibExtensions() throws Exception {
        UnitTestContext context = newContext();
        submitCluster(context);
        FileSystem fs = FalconUnit.getFileSystem();
        assertLibs(fs, new Path("/projects/falcon/working/libext/FEED/retention"));
        assertLibs(fs, new Path("/projects/falcon/working/libext/PROCESS"));

        String tmpFileName = TestContext.overlayParametersOverTemplate(UnitTestContext.FEED_TEMPLATE1, context.overlay);
        Feed feed = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(new File(tmpFileName));
        Location location = new Location();
        location.setPath("fsext://global:00/falcon/test/input/${YEAR}/${MONTH}/${DAY}/${HOUR}");
        location.setType(LocationType.DATA);
        Cluster cluster = feed.getClusters().getClusters().get(0);
        cluster.setLocations(new Locations());
        feed.getClusters().getClusters().get(0).getLocations().getLocations().add(location);

        File tmpFile = UnitTestContext.getTempFile();
        EntityType.FEED.getMarshaller().marshal(feed, tmpFile);

        APIResult result = falconUnitClient.submitAndSchedule(EntityType.FEED.name(), tmpFile.getAbsolutePath(), true,
                null, null);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    }

    @Test
    public void testUpdateCheckUser() throws Exception {
        UnitTestContext context = newContext();
        String tmpFileName = TestContext.overlayParametersOverTemplate(UnitTestContext.PROCESS_TEMPLATE,
                context.overlay);
        Process process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(new File(tmpFileName));
        updateEndtime(process);
        File tmpFile = UnitTestContext.getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);
        submitCluster(context);
        context.prepare();
        submitFeeds(context.overlay);
        submitProcess(tmpFile.getAbsolutePath(), context.overlay);
        scheduleProcess(context.getProcessName(), context.getClusterName(), getAbsolutePath(SLEEP_WORKFLOW));
        waitForStatus(EntityType.PROCESS.name(), context.getProcessName(), START_INSTANCE,
                InstancesResult.WorkflowStatus.RUNNING);

        List<BundleJob> bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 1);
        Assert.assertEquals(bundles.get(0).getUser(), TestContext.REMOTE_USER);

        Feed feed = (Feed) getDefinition(EntityType.FEED, context.outputFeedName);

        //change output feed path and update feed as another user
        feed.getLocations().getLocations().get(0).setPath("/falcon/test/output2/${YEAR}/${MONTH}/${DAY}");
        tmpFile = TestContext.getTempFile();
        feed.getEntityType().getMarshaller().marshal(feed, tmpFile);
        APIResult result = falconUnitClient.update(EntityType.FEED.name(), feed.getName(),
                tmpFile.getAbsolutePath(), true, null);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

        bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 2);
        Assert.assertEquals(bundles.get(0).getUser(), TestContext.REMOTE_USER);
        Assert.assertEquals(bundles.get(1).getUser(), TestContext.REMOTE_USER);
    }

    public void testOptionalInput() throws Exception {
        UnitTestContext context = newContext();
        String tmpFileName = TestContext.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, context.overlay);
        Process process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(new File(tmpFileName));

        Input in1 = process.getInputs().getInputs().get(0);
        Input in2 = new Input();
        in2.setFeed(in1.getFeed());
        in2.setName("input2");
        in2.setOptional(true);
        in2.setPartition(in1.getPartition());
        in2.setStart("now(-1,0)");
        in2.setEnd("now(0,0)");
        process.getInputs().getInputs().add(in2);

        File tmpFile = TestContext.getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);
        schedule(context);
        waitForStatus(EntityType.PROCESS.name(), context.getProcessName(), START_INSTANCE,
                InstancesResult.WorkflowStatus.SUCCEEDED);
    }

    public void testDryRun() throws Exception {
        //Schedule of invalid process should fail because of dryRun, and should pass when dryrun is skipped
        UnitTestContext context = newContext();
        String tmpFileName = TestContext.overlayParametersOverTemplate(UnitTestContext.PROCESS_TEMPLATE,
                context.overlay);
        Process process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(new File(tmpFileName));
        Property prop = new Property();
        prop.setName("newProp");
        prop.setValue("${instanceTim()}");  //invalid property
        process.getProperties().getProperties().add(prop);
        File tmpFile = TestContext.getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);

        try {
            falconUnitClient.validate(EntityType.PROCESS.name(), tmpFile.getAbsolutePath(),
                    true, null);
            Assert.fail("Exception should be Thrown");
        } catch (Exception e) {
            //ignore
        }
        schedule(context);

        //Fix the process and then submitAndSchedule should succeed
        Iterator<Property> itr = process.getProperties().getProperties().iterator();
        while (itr.hasNext()) {
            Property myProp = itr.next();
            if (myProp.getName().equals("newProp")) {
                itr.remove();
            }
        }
        tmpFile = TestContext.getTempFile();
        process.setName("process" + System.currentTimeMillis());
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);
        APIResult result = falconUnitClient.submitAndSchedule(EntityType.PROCESS.name(),
                tmpFile.getAbsolutePath(), true, null, null);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

        // update where dryrun is disabled should succeed.
        tmpFile = TestContext.getTempFile();
        process.getEntityType().getMarshaller().marshal(process, tmpFile);
        result = falconUnitClient.update(EntityType.PROCESS.name(), process.getName(),
                tmpFile.getAbsolutePath(), true, null);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    }

    @Test
    public void testUpdateSuspendedEntity() throws Exception {
        UnitTestContext context = newContext();
        schedule(context);
        waitForStatus(EntityType.PROCESS.name(), context.processName, START_INSTANCE,
                InstancesResult.WorkflowStatus.RUNNING);

        //Suspend entity
        Process process = (Process) getDefinition(EntityType.PROCESS, context.processName);
        APIResult result = falconUnitClient.suspend(EntityType.PROCESS, process.getName(), context.colo,
                null);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
        result = falconUnitClient.getStatus(EntityType.PROCESS, context.processName, context.clusterName,
                null, false);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertEquals(result.getMessage(), "SUSPENDED");

        process.getProperties().getProperties().get(0).setName("newprop");
        Date endTime = getEndTime();
        process.getClusters().getClusters().get(0).getValidity().setEnd(endTime);
        File tmpFile = TestContext.getTempFile();
        process.getEntityType().getMarshaller().marshal(process, tmpFile);
        result = falconUnitClient.update(EntityType.PROCESS.name(), context.getProcessName(),
                tmpFile.getAbsolutePath(), true, null);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

        //Since the process endtime = update effective time, it shouldn't create new bundle
        List<BundleJob> bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 1);

        //Since the entity was suspended before update, it should still be suspended
        Assert.assertEquals(bundles.get(0).getStatus(), Status.SUSPENDED);
    }

    @Test
    public void testProcessInputUpdate() throws Exception {
        UnitTestContext context = newContext();

        schedule(context);
        waitForStatus(EntityType.PROCESS.name(), context.processName, START_INSTANCE,
                InstancesResult.WorkflowStatus.RUNNING);
        List<BundleJob> bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 1);
        OozieClient ozClient = OozieTestUtils.getOozieClient(context);
        String bundle = bundles.get(0).getId();
        String coordId = ozClient.getBundleJobInfo(bundle).getCoordinators().get(0).getId();

        Process process = (Process) getDefinition(EntityType.PROCESS, context.processName);

        String feed3 = "f3" + System.currentTimeMillis();
        Map<String, String> overlay = new HashMap<String, String>();
        overlay.put("inputFeedName", feed3);
        overlay.put("cluster", context.clusterName);
        overlay.put("user", System.getProperty("user.name"));
        submitFeed(UnitTestContext.FEED_TEMPLATE1, overlay);


        Input input = new Input();
        input.setFeed(feed3);
        input.setName("inputData2");
        input.setStart("today(20,0)");
        input.setEnd("today(20,20)");
        process.getInputs().getInputs().add(input);

        updateEndtime(process);
        Date endTime = getEndTime();
        File tmpFile = TestContext.getTempFile();
        process.getEntityType().getMarshaller().marshal(process, tmpFile);
        APIResult result = falconUnitClient.update(EntityType.PROCESS.name(), context.getProcessName(),
                tmpFile.getAbsolutePath(), true, null);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

        //Assert that update creates new bundle and old coord is running
        bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 2);
        CoordinatorJob coord = ozClient.getCoordJobInfo(coordId);
        Assert.assertEquals(coord.getStatus(), Status.RUNNING);
        Assert.assertEquals(coord.getEndTime(), endTime);

        //Assert on new bundle/coord
        String newBundle = null;
        for (BundleJob myBundle : bundles) {
            if (!myBundle.getId().equals(bundle)) {
                newBundle = myBundle.getId();
                break;
            }
        }

        assert newBundle != null;
        OozieTestUtils.waitForBundleStart(context, newBundle, Job.Status.RUNNING, Status.PREP);
        coord = ozClient.getCoordJobInfo(ozClient.getBundleJobInfo(newBundle).getCoordinators().get(0).getId());
        Assert.assertTrue(coord.getStatus() == Status.RUNNING || coord.getStatus() == Status.PREP);
        Assert.assertEquals(coord.getStartTime(), endTime);
    }

    @Test
    public void testProcessEndtimeUpdate() throws Exception {
        UnitTestContext context = newContext();
        schedule(context);
        OozieTestUtils.waitForBundleStart(context, Job.Status.RUNNING);

        Process process = (Process) getDefinition(EntityType.PROCESS, context.processName);

        updateEndtime(process);
        File tmpFile = TestContext.getTempFile();
        process.getEntityType().getMarshaller().marshal(process, tmpFile);
        APIResult result = falconUnitClient.update(EntityType.PROCESS.name(), context.getProcessName(),
                tmpFile.getAbsolutePath(), true, null);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

        //Assert that update does not create new bundle
        List<BundleJob> bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 1);
    }

    @Test
    public void testTouchEntity() throws Exception {
        UnitTestContext context = newContext();
        schedule(context);
        OozieTestUtils.waitForBundleStart(context, Job.Status.RUNNING);
        List<BundleJob> bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 1);
        OozieClient ozClient = OozieTestUtils.getOozieClient(context);
        String bundle = bundles.get(0).getId();
        String coordId = ozClient.getBundleJobInfo(bundle).getCoordinators().get(0).getId();

        //Update end time of process required for touch
        Process process = (Process) getDefinition(EntityType.PROCESS, context.processName);
        updateEndtime(process);
        File tmpFile = TestContext.getTempFile();
        process.getEntityType().getMarshaller().marshal(process, tmpFile);
        APIResult result = falconUnitClient.update(EntityType.PROCESS.name(), context.getProcessName(),
                tmpFile.getAbsolutePath(), true, null);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
        bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 1);

        result = falconUnitClient.touch(EntityType.PROCESS.name(), context.getProcessName(), context.colo, true, null);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
        OozieTestUtils.waitForBundleStart(context, Status.PREP, Status.RUNNING);

        //Assert that touch creates new bundle and old coord is running
        bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 2);
        CoordinatorJob coord = ozClient.getCoordJobInfo(coordId);
        Assert.assertTrue(coord.getStatus() == Status.RUNNING || coord.getStatus() == Status.SUCCEEDED);

        //Assert on new bundle/coord
        String newBundle = null;
        for (BundleJob myBundle : bundles) {
            if (!myBundle.getId().equals(bundle)) {
                newBundle = myBundle.getId();
                break;
            }
        }

        assert newBundle != null;
        coord = ozClient.getCoordJobInfo(ozClient.getBundleJobInfo(newBundle).getCoordinators().get(0).getId());
        Assert.assertTrue(coord.getStatus() == Status.RUNNING || coord.getStatus() == Status.PREP);
    }

    public void testStatus() throws Exception {
        UnitTestContext context = newContext();

        submitCluster(context);

        context.prepare();
        submitFeed(UnitTestContext.FEED_TEMPLATE1, context.overlay);

        APIResult result = falconUnitClient.getStatus(EntityType.FEED, context.overlay.get("inputFeedName"),
                context.colo, null, false);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertEquals(result.getMessage(), "SUBMITTED");
    }

    public void testIdempotentSubmit() throws Exception {
        UnitTestContext context = newContext();

        submitCluster(context);

        submitCluster(context);
    }

    public void testNotFoundStatus() throws FalconException, IOException, FalconCLIException {
        String feed1 = "f1" + System.currentTimeMillis();
        try {
            falconUnitClient.getStatus(EntityType.FEED, feed1, null, null, false);
            Assert.fail("Exception should be Thrown");
        } catch (Exception e) {
            //ignore
        }
    }

    public void testVersion() throws FalconException, IOException, FalconCLIException {
        String json = falconUnitClient.getVersion(null);
        String buildVersion = BuildProperties.get().getProperty("build.version");
        String deployMode = DeploymentProperties.get().getProperty("deploy.mode");
        Assert.assertTrue(Pattern.matches(
                        ".*\\{\\s*\"key\"\\s*:\\s*\"Version\"\\s*,\\s*\"value\"\\s*:\\s*\""
                                + buildVersion + "\"\\s*}.*", json),
                "No build.version found in /api/admin/version");
        Assert.assertTrue(Pattern.matches(
                        ".*\\{\\s*\"key\"\\s*:\\s*\"Mode\"\\s*,\\s*\"value\"\\s*:\\s*\""
                                + deployMode + "\"\\s*}.*", json),
                "No deploy.mode found in /api/admin/version");
    }

    public void testValidate() throws FalconException, IOException {
        UnitTestContext context = newContext();
        try {
            falconUnitClient.validate(EntityType.PROCESS.name(),
                    UnitTestContext.class.getResource(UnitTestContext.SAMPLE_PROCESS_XML).getPath(), true, null);
            Assert.fail("Exception should be Thrown");
        } catch (Exception e) {
            //ignore
        }
    }

    public void testClusterValidate() throws Exception {
        UnitTestContext context = newContext();

        String tmpFileName = TestContext.overlayParametersOverTemplate(TestContext.CLUSTER_TEMPLATE, context.overlay);
        File tmpFile = new File(tmpFileName);
        fs.mkdirs(new Path(STAGING_PATH), HadoopClientFactory.ALL_PERMISSION);
        fs.mkdirs(new Path(WORKING_PATH), HadoopClientFactory.READ_EXECUTE_PERMISSION);
        APIResult result = falconUnitClient.validate(EntityType.CLUSTER.name(), tmpFile.getAbsolutePath(),
                true, null);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    }

    ClientResponse suspend(TestContext context, Entity entity) {
        return suspend(context, entity.getEntityType(), entity.getName());
    }

    private ClientResponse suspend(TestContext context, EntityType entityType, String name) {
        return context.service
            .path("api/entities/suspend/" + entityType.name().toLowerCase() + "/" + name)
            .header("Cookie", context.getAuthenticationToken())
            .accept(MediaType.TEXT_XML).type(MediaType.TEXT_XML)
            .post(ClientResponse.class);
    }

    public void testClusterSubmitScheduleSuspendResumeDelete() throws Exception {
        UnitTestContext context = newContext();

        submitCluster(context);

        try {
            falconUnitClient.schedule(EntityType.CLUSTER, context.clusterName, null, true, null, null);
            Assert.fail("Exception should be Thrown");
        } catch (Exception e) {
            //ignore
        }

        try {
            falconUnitClient.suspend(EntityType.CLUSTER, context.clusterName, context.colo, null);
            Assert.fail("Exception should be Thrown");
        } catch (Exception e) {
            //ignore
        }

        try {
            falconUnitClient.resume(EntityType.CLUSTER, context.clusterName, context.colo, null);
            Assert.fail("Exception should be Thrown");
        } catch (Exception e) {
            //ignore
        }

        APIResult result = falconUnitClient.delete(EntityType.CLUSTER, context.getClusterName(), null);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    }

    public void testSubmit() throws Exception {
        UnitTestContext context = newContext();
        ClientResponse response;

        submitCluster(context);

        submitFeed(UnitTestContext.FEED_TEMPLATE1, context.overlay);

        submitFeed(UnitTestContext.FEED_TEMPLATE2, context.overlay);

        submitProcess(UnitTestContext.PROCESS_TEMPLATE, context.overlay);
    }

    @Test
    public void testDuplicateSubmitCommands() throws Exception {
        UnitTestContext context = newContext();

        submitCluster(context);

        ExecutorService service = Executors.newSingleThreadExecutor();
        ExecutorService duplicateService = Executors.newSingleThreadExecutor();

        Future<APIResult> future = service.submit(new SubmitCommand(context, context.overlay));
        Future<APIResult> duplicateFuture = duplicateService.submit(new SubmitCommand(context, context.overlay));

        // since there are duplicate threads for submits, there is no guarantee which request will succeed.
        try {
            APIResult response = future.get();
            APIResult duplicateSubmitThreadResponse = duplicateFuture.get();
            Assert.fail("Exception should be Thrown");
        } catch (Exception e) {
            //ignore
        }
    }

    @Test
    public void testDuplicateDeleteCommands() throws Exception {
        UnitTestContext context = newContext();
        Map<String, String> overlay = context.overlay;
        submitCluster(context);
        submitFeed(UnitTestContext.FEED_TEMPLATE1, overlay);

        ExecutorService service = Executors.newFixedThreadPool(2);

        Future<APIResult> future = service.submit(new DeleteCommand(context, overlay.get("inputFeedName"),
                "feed"));
        Future<APIResult> duplicateFuture = service.submit(new DeleteCommand(context,
                overlay.get("inputFeedName"), "feed"));

        // since there are duplicate threads for submits, there is no guarantee which request will succeed.
        try {
            APIResult response = future.get();
            APIResult duplicateSubmitThreadResponse = duplicateFuture.get();
            Assert.fail("Exception should be Thrown");
        } catch (Exception e) {
            //ignore
        }
    }

    public void testProcesssScheduleAndDelete() throws Exception {
        scheduleAndDeleteProcess(false);
    }

    public void testProcesssScheduleAndDeleteWithDoAs() throws Exception {
        scheduleAndDeleteProcess(true);
    }

    private void scheduleAndDeleteProcess(boolean withDoAs) throws Exception {
        UnitTestContext context = newContext();
        submitCluster(context);
        context.prepare();
        submitFeeds(context.overlay);
        ClientResponse clientResponse;
        String tmpFileName = TestContext.overlayParametersOverTemplate(TestContext.PROCESS_TEMPLATE, context.overlay);
        Process process = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(new File(tmpFileName));
        updateEndtime(process);
        File tmpFile = TestContext.getTempFile();
        EntityType.PROCESS.getMarshaller().marshal(process, tmpFile);
        submitProcess(tmpFile.getAbsolutePath(), context.overlay);
        if (withDoAs) {
            falconUnitClient.schedule(EntityType.PROCESS, context.getProcessName(), context.getClusterName(), false,
                    FalconTestUtil.TEST_USER_2, null);
        } else {
            falconUnitClient.schedule(EntityType.PROCESS, context.getProcessName(), context.getClusterName(), false, "",
                    "key1:value1");
        }
        OozieTestUtils.waitForBundleStart(context, Status.RUNNING);

        APIResult result;
        if (withDoAs) {
            result = falconUnitClient.delete(EntityType.PROCESS, context.getProcessName(), FalconTestUtil.TEST_USER_2);
        } else {
            result = falconUnitClient.delete(EntityType.PROCESS, context.getProcessName(), null);
        }
        assertStatus(result);
    }

    public void testGetEntityDefinition() throws Exception {
        UnitTestContext context = newContext();

        submitCluster(context);

        context.prepare();
        APIResult result = submitFeed(UnitTestContext.FEED_TEMPLATE1, context.overlay);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

        Feed feed = (Feed) falconUnitClient.getDefinition(EntityType.FEED.name(),
                context.overlay.get("inputFeedName"), null);
        Assert.assertEquals(feed.getName(), context.overlay.get("inputFeedName"));
    }

    public void testInvalidGetEntityDefinition() throws FalconException, IOException, FalconCLIException {
        try {
            falconUnitClient.getDefinition(EntityType.PROCESS.name(), "sample1", null);
            Assert.fail("Exception should be Thrown");
        } catch (Exception e) {
            //ignore
        }
    }

    public void testScheduleSuspendResume() throws Exception {
        UnitTestContext context = newContext();

        schedule(context);
        waitForStatus(EntityType.PROCESS.name(), context.processName, START_INSTANCE,
                InstancesResult.WorkflowStatus.RUNNING);

        APIResult result = falconUnitClient.suspend(EntityType.PROCESS, context.getProcessName(),
                context.colo, null);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
        result = falconUnitClient.getStatus(EntityType.PROCESS, context.processName, context.clusterName,
                null, false);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertEquals(result.getMessage(), "SUSPENDED");

        result = falconUnitClient.resume(EntityType.PROCESS, context.getProcessName(),
                context.colo, null);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
        result = falconUnitClient.getStatus(EntityType.PROCESS, context.processName, context.clusterName,
                null, false);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
        Assert.assertEquals(result.getMessage(), "RUNNING");
    }

    public void testFeedSchedule() throws Exception {
        UnitTestContext context = newContext();

        submitCluster(context);

        context.prepare();
        APIResult result = submitFeed(UnitTestContext.FEED_TEMPLATE1, context.overlay);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

        createTestData();
        result = falconUnitClient.schedule(EntityType.FEED, context.overlay.get("inputFeedName"), null, true, null,
                null);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    }

    public void testDeleteDataSet() throws Exception {
        UnitTestContext context = newContext();

        submitCluster(context);

        context.prepare();
        APIResult result = submitFeed(UnitTestContext.FEED_TEMPLATE1, context.overlay);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

        result = falconUnitClient.delete(EntityType.FEED, context.overlay.get("inputFeedName"), null);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    }

    public void testDelete() throws Exception {
        UnitTestContext context = newContext();

        submitCluster(context);

        context.prepare();
        APIResult result = submitFeed(UnitTestContext.FEED_TEMPLATE1, context.overlay);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

        try {
            falconUnitClient.delete(EntityType.CLUSTER, context.getClusterName(), null);
            Assert.fail("Exception should be Thrown");
        } catch (Exception e) {
            //ignore
        }

        result = submitFeed(UnitTestContext.FEED_TEMPLATE2, context.overlay);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

        submitProcess(UnitTestContext.PROCESS_TEMPLATE, context.overlay);

        //Delete a referred feed
        try {
            falconUnitClient.delete(EntityType.FEED, context.overlay.get("inputFeedName"), null);
            Assert.fail("Exception should be Thrown");
        } catch (Exception e) {
            //ignore
        }

        //Delete a submitted process
        result = falconUnitClient.delete(EntityType.PROCESS, context.getProcessName(), null);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

        submitProcess(UnitTestContext.PROCESS_TEMPLATE, context.overlay);

        result = falconUnitClient.schedule(EntityType.PROCESS, context.getProcessName(), null, true, null,
                null);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

        //Delete a scheduled process
        result = falconUnitClient.delete(EntityType.PROCESS, context.getProcessName(), null);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    }

    @Test
    public void testGetEntityList() throws Exception {
        EntityList result = falconUnitClient.getEntityList(EntityType.PROCESS.name(), "", "", null, null,
                null, null, null, new Integer(0), new Integer(1), null);
        Assert.assertNotNull(result);
        for (EntityList.EntityElement entityElement : result.getElements()) {
            Assert.assertNull(entityElement.status); // status is null
        }

        result = falconUnitClient.getEntityList(EntityType.CLUSTER.name(), "", "", null, null,
                null, null, null, new Integer(0), new Integer(1), null);
        Assert.assertNotNull(result);
        for (EntityList.EntityElement entityElement : result.getElements()) {
            Assert.assertNull(entityElement.status); // status is null
        }

        result = falconUnitClient.getEntityList(EntityType.FEED.name() + "," + EntityType.PROCESS.name(),
                "", "", null, null, null, null, null, new Integer(0), new Integer(1), null);
        Assert.assertNotNull(result);
        for (EntityList.EntityElement entityElement : result.getElements()) {
            Assert.assertNull(entityElement.status); // status is null
        }

        result = falconUnitClient.getEntityList(null, "", "", null, null, null, null, null, new Integer(0),
                new Integer(1), null);
        Assert.assertNotNull(result);
        for (EntityList.EntityElement entityElement : result.getElements()) {
            Assert.assertNull(entityElement.status); // status is null
        }
    }

    @Test
    public void testDuplicateUpdateCommands() throws Exception {
        UnitTestContext context = newContext();
        schedule(context);
        OozieTestUtils.waitForBundleStart(context, Job.Status.RUNNING);
        List<BundleJob> bundles = OozieTestUtils.getBundles(context);
        Assert.assertEquals(bundles.size(), 1);

        Process process = (Process) getDefinition(EntityType.PROCESS, context.processName);

        String feed3 = "f3" + System.currentTimeMillis();
        Map<String, String> overlay = new HashMap<String, String>();
        overlay.put("inputFeedName", feed3);
        overlay.put("cluster", context.clusterName);
        overlay.put("user", System.getProperty("user.name"));
        APIResult result = submitFeed(UnitTestContext.FEED_TEMPLATE1, context.overlay);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);

        Input input = new Input();
        input.setFeed(feed3);
        input.setName("inputData2");
        input.setStart("today(20,0)");
        input.setEnd("today(20,20)");
        process.getInputs().getInputs().add(input);

        updateEndtime(process);
        Date endTime = getEndTime();
        ExecutorService service =  Executors.newSingleThreadExecutor();
        ExecutorService duplicateService = Executors.newSingleThreadExecutor();

        Future<APIResult> future = service.submit(new UpdateCommand(context, process, endTime));
        Future<APIResult> duplicateFuture = duplicateService.submit(new UpdateCommand(context, process, endTime));

        // since there are duplicate threads for updates, there is no guarantee which request will succeed
        try {
            future.get();
            duplicateFuture.get();
            Assert.fail("Exception should be Thrown");
        } catch (Exception e) {
            //ignore
        }
    }

    public Date getEndTime() {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.add(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    class UpdateCommand implements Callable<APIResult> {
        private UnitTestContext context;
        private Process process;
        private Date endTime;

        public UnitTestContext getContext() {
            return context;
        }
        public Process getProcess() {
            return process;
        }

        public UpdateCommand(UnitTestContext context, Process process, Date endTime) {
            this.context = context;
            this.process = process;
            this.endTime = endTime;
        }

        @Override
        public APIResult call() throws Exception {
            File tmpFile = TestContext.getTempFile();
            process.getEntityType().getMarshaller().marshal(process, tmpFile);
            return falconUnitClient.update(EntityType.PROCESS.name(), context.getProcessName(),
                    tmpFile.getAbsolutePath(), true, null);
        }
    }

    class SubmitCommand implements Callable<APIResult> {
        private Map<String, String> overlay;
        private UnitTestContext context;

        public UnitTestContext getContext() {
            return context;
        }

        public Map<String, String> getOverlay() {
            return overlay;
        }

        public SubmitCommand(UnitTestContext context, Map<String, String> overlay) {
            this.context = context;
            this.overlay = overlay;
        }

        @Override
        public APIResult call() throws Exception {
            return submitFeed(UnitTestContext.FEED_TEMPLATE1, context.overlay);
        }
    }

    class DeleteCommand implements Callable<APIResult> {
        private UnitTestContext context;
        private String entityName;
        private String entityType;

        public UnitTestContext getContext() {
            return context;
        }

        public DeleteCommand(UnitTestContext context, String entityName, String entityType) {
            this.context = context;
            this.entityName = entityName;
            this.entityType = entityType;
        }

        @Override
        public APIResult call() throws Exception {
            return falconUnitClient.delete(EntityType.valueOf(entityType), entityName, null);
        }
    }
}
