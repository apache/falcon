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
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.state.AbstractSchedulerTestBase;
import org.apache.falcon.state.store.service.FalconJPAService;
import org.apache.falcon.unit.FalconUnitTestBase;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Base class for tests using Native Scheduler.
 */
public class AbstractSchedulerManagerJerseyIT extends FalconUnitTestBase {

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
        updateStartUpProps();
        falconJPAService.init();
        createDB();
        super.setup();
    }

    private void updateStartUpProps() {
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
}
