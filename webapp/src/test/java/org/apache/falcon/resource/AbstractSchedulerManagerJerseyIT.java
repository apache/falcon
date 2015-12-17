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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.state.AbstractSchedulerTestBase;
import org.apache.falcon.state.store.service.FalconJPAService;
import org.apache.falcon.unit.FalconUnitTestBase;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Base class for tests using Native Scheduler.
 */
public class AbstractSchedulerManagerJerseyIT extends FalconUnitTestBase {

    private static final String SLEEP_WORKFLOW = "sleepWorkflow.xml";
    private static final String LOCAL_MODE = "local";
    private static final String IT_RUN_MODE = "it.run.mode";

    public static final String PROCESS_TEMPLATE = "/local-process-noinputs-template.xml";
    public static final String PROCESS_NAME = "processName";
    protected static final String START_INSTANCE = "2012-04-20T00:00Z";
    private static FalconJPAService falconJPAService = FalconJPAService.get();
    private static final String DB_BASE_DIR = "target/test-data/falcondb";
    protected static String dbLocation = DB_BASE_DIR + File.separator + "data.db";
    protected static String url = "jdbc:derby:"+ dbLocation +";create=true";
    protected static final String DB_SQL_FILE = DB_BASE_DIR + File.separator + "out.sql";
    protected LocalFileSystem localFS = new LocalFileSystem();



    @BeforeClass
    public void setup() throws Exception {
        Configuration localConf = new Configuration();
        localFS.initialize(LocalFileSystem.getDefaultUri(localConf), localConf);
        cleanupDB();
        localFS.mkdirs(new Path(DB_BASE_DIR));
        falconJPAService.init();
        createDB();
        super.setup();
    }

    protected void updateStartUpProps() {
        StartupProperties.get().setProperty("workflow.engine.impl",
                "org.apache.falcon.workflow.engine.FalconWorkflowEngine");
        StartupProperties.get().setProperty("dag.engine.impl",
                "org.apache.falcon.workflow.engine.OozieDAGEngine");
        String[] listeners = StartupProperties.get().getProperty("configstore.listeners").split(",");
        List<String> configListeners = new ArrayList<>(Arrays.asList(listeners));
        configListeners.remove("org.apache.falcon.service.SharedLibraryHostingService");
        configListeners.add("org.apache.falcon.state.store.jdbc.JDBCStateStore");
        StartupProperties.get().setProperty("configstore.listeners", StringUtils.join(configListeners, ","));
        StartupProperties.get().getProperty("falcon.state.store.impl",
                "org.apache.falcon.state.store.jdbc.JDBCStateStore");
    }

    protected void submitProcess(Map<String, String> overlay) throws IOException, FalconCLIException {
        String tmpFile = TestContext.overlayParametersOverTemplate(PROCESS_TEMPLATE, overlay);
        APIResult result = submit(EntityType.PROCESS, tmpFile);
        assertStatus(result);
    }

    protected void scheduleProcess(String processName, String cluster,
                                   String startTime, int noOfInstances) throws FalconCLIException {
        APIResult result = falconUnitClient.schedule(EntityType.PROCESS, processName, startTime, noOfInstances,
                cluster, true, null);
        assertStatus(result);
    }

    protected void setupProcessExecution(UnitTestContext context,
                                         Map<String, String> overlay, int numInstances) throws Exception {
        String colo = overlay.get(COLO);
        String cluster = overlay.get(CLUSTER);
        submitCluster(colo, cluster, null);
        context.prepare();
        submitProcess(overlay);

        String processName = overlay.get(PROCESS_NAME);
        scheduleProcess(processName, cluster, START_INSTANCE, numInstances);
    }

    private void createDB() throws Exception {
        AbstractSchedulerTestBase abstractSchedulerTestBase = new AbstractSchedulerTestBase();
        StartupProperties.get().setProperty(FalconJPAService.URL, url);
        abstractSchedulerTestBase.createDB(DB_SQL_FILE);
    }

    @AfterClass
    public void cleanup() throws Exception {
        super.cleanup();
        cleanupDB();
    }

    private void cleanupDB() throws IOException {
        localFS.delete(new Path(DB_BASE_DIR), true);
    }

    protected void submitCluster(UnitTestContext context) throws IOException, FalconCLIException {
        String mode = System.getProperty(IT_RUN_MODE);
        if (StringUtils.isNotEmpty(mode) && mode.toLowerCase().equals(LOCAL_MODE)) {
            submitCluster(context.colo, context.clusterName, null);
        } else {
            fs.mkdirs(new Path(STAGING_PATH), HadoopClientFactory.ALL_PERMISSION);
            fs.mkdirs(new Path(WORKING_PATH), HadoopClientFactory.READ_EXECUTE_PERMISSION);
            String tmpFile = TestContext.overlayParametersOverTemplate(TestContext.CLUSTER_TEMPLATE,
                    context.overlay);
            submit(EntityType.CLUSTER, tmpFile);
        }
    }

    protected APIResult submitFeed(String template, Map<String, String> overlay) throws IOException,
            FalconCLIException {
        String tmpFile = TestContext.overlayParametersOverTemplate(template, overlay);
        APIResult result = falconUnitClient.submit(EntityType.FEED.name(), tmpFile, null);
        return result;
    }

    protected void submitFeeds(Map<String, String> overlay) throws IOException, FalconCLIException {
        String tmpFile = TestContext.overlayParametersOverTemplate(UnitTestContext.FEED_TEMPLATE1, overlay);
        APIResult result = falconUnitClient.submit(EntityType.FEED.name(), tmpFile, null);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
        tmpFile = TestContext.overlayParametersOverTemplate(UnitTestContext.FEED_TEMPLATE2, overlay);
        result = falconUnitClient.submit(EntityType.FEED.name(), tmpFile, null);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    }

    protected void submitProcess(String template, Map<String, String> overlay) throws Exception {
        String tmpFile = TestContext.overlayParametersOverTemplate(template, overlay);
        APIResult result = falconUnitClient.submit(EntityType.PROCESS.name(), tmpFile, null);
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    }

    protected void scheduleProcess(UnitTestContext context) throws FalconCLIException, IOException, FalconException {
        String scheduleTime = START_INSTANCE;
        APIResult result = scheduleProcess(context.getProcessName(), scheduleTime, 1, context.getClusterName(),
                getAbsolutePath(SLEEP_WORKFLOW), true, "");
        Assert.assertEquals(result.getStatus(), APIResult.Status.SUCCEEDED);
    }

    protected void schedule(UnitTestContext context) throws Exception {
        submitCluster(context);
        context.prepare();
        submitFeeds(context.overlay);
        submitProcess(UnitTestContext.PROCESS_TEMPLATE, context.overlay);
        scheduleProcess(context);
    }

    protected List<Path> createTestData() throws Exception {
        List<Path> list = new ArrayList<Path>();
        fs.mkdirs(new Path("/user/guest"));
        fs.setOwner(new Path("/user/guest"), TestContext.REMOTE_USER, "users");

        DateFormat formatter = new SimpleDateFormat("yyyy/MM/dd/HH/mm");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date date = new Date(System.currentTimeMillis() + 3 * 3600000);
        Path path = new Path("/examples/input-data/rawLogs/" + formatter.format(date) + "/file");
        fs.create(path).close();
        date = new Date(date.getTime() - 3600000);
        path = new Path("/examples/input-data/rawLogs/" + formatter.format(date) + "/file");
        fs.create(path).close();
        date = new Date(date.getTime() - 3600000);
        path = new Path("/examples/input-data/rawLogs/" + formatter.format(date) + "/file");
        fs.create(path).close();
        date = new Date(date.getTime() - 3600000);
        path = new Path("/examples/input-data/rawLogs/" + formatter.format(date) + "/file");
        list.add(path);
        fs.create(path).close();
        date = new Date(date.getTime() - 3600000);
        path = new Path("/examples/input-data/rawLogs/" + formatter.format(date) + "/file");
        list.add(path);
        fs.create(path).close();
        date = new Date(date.getTime() - 3600000);
        path = new Path("/examples/input-data/rawLogs/" + formatter.format(date) + "/file");
        list.add(path);
        fs.create(path).close();
        date = new Date(date.getTime() - 3600000);
        path = new Path("/examples/input-data/rawLogs/" + formatter.format(date) + "/file");
        list.add(path);
        fs.create(path).close();
        date = new Date(date.getTime() - 3600000);
        path = new Path("/examples/input-data/rawLogs/" + formatter.format(date) + "/file");
        list.add(path);
        fs.create(path).close();
        date = new Date(date.getTime() - 3600000);
        path = new Path("/examples/input-data/rawLogs/" + formatter.format(date) + "/file");
        list.add(path);
        fs.create(path).close();
        date = new Date(date.getTime() - 3600000);
        path = new Path("/examples/input-data/rawLogs/" + formatter.format(date) + "/file");
        list.add(path);
        fs.create(path).close();
        new FsShell(new Configuration()).run(new String[] {
            "-chown", "-R", "guest:users", "/examples/input-data/rawLogs", });
        return list;
    }

}
