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
package org.apache.falcon.service;

import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.AbstractTestBase;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.jdbc.BacklogMetricStore;
import org.apache.falcon.metrics.MetricNotificationService;
import org.apache.falcon.tools.FalconStateStoreDBCLI;
import org.apache.falcon.util.StateStoreProperties;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Test cases for Backlog Metric Store.
 */
public class BacklogMetricEmitterServiceTest extends AbstractTestBase{
    private static final String DB_BASE_DIR = "target/test-data/backlogmetricdb";
    protected static String dbLocation = DB_BASE_DIR + File.separator + "data.db";
    protected static String url = "jdbc:derby:"+ dbLocation +";create=true";
    protected static final String DB_SQL_FILE = DB_BASE_DIR + File.separator + "out.sql";
    protected LocalFileSystem fs = new LocalFileSystem();

    private static BacklogMetricStore backlogMetricStore;
    private static FalconJPAService falconJPAService = FalconJPAService.get();
    private static BacklogMetricEmitterService backlogMetricEmitterService;
    private MetricNotificationService mockMetricNotificationService;

    protected int execDBCLICommands(String[] args) {
        return new FalconStateStoreDBCLI().run(args);
    }

    public void createDB(String file) {
        File sqlFile = new File(file);
        String[] argsCreate = { "create", "-sqlfile", sqlFile.getAbsolutePath(), "-run" };
        int result = execDBCLICommands(argsCreate);
        Assert.assertEquals(0, result);
        Assert.assertTrue(sqlFile.exists());

    }

    @AfterClass
    public void cleanup() throws IOException {
        cleanupDB();
    }

    private void cleanupDB() throws IOException {
        fs.delete(new Path(DB_BASE_DIR), true);
    }

    @BeforeClass
    public void setup() throws Exception{
        StateStoreProperties.get().setProperty(FalconJPAService.URL, url);
        Configuration localConf = new Configuration();
        fs.initialize(LocalFileSystem.getDefaultUri(localConf), localConf);
        fs.mkdirs(new Path(DB_BASE_DIR));
        createDB(DB_SQL_FILE);
        falconJPAService.init();
        this.dfsCluster = EmbeddedCluster.newCluster("testCluster");
        this.conf = dfsCluster.getConf();
        backlogMetricStore = new BacklogMetricStore();
        mockMetricNotificationService = Mockito.mock(MetricNotificationService.class);
        Mockito.when(mockMetricNotificationService.getName()).thenReturn("MetricNotificationService");
        Services.get().register(mockMetricNotificationService);
        Services.get().register(BacklogMetricEmitterService.get());
        backlogMetricEmitterService = BacklogMetricEmitterService.get();

    }


    @Test
    public void testBacklogEmitter() throws Exception {
        backlogMetricEmitterService.init();
        storeEntity(EntityType.PROCESS, "entity1");
        backlogMetricEmitterService.highSLAMissed("entity1",  "cluster1", EntityType.PROCESS,
                BacklogMetricEmitterService.DATE_FORMAT.get().parse("2016-06-30T00-00Z"));
        Thread.sleep(10);
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Long> valueCaptor = ArgumentCaptor.forClass(Long.class);
        Mockito.verify(mockMetricNotificationService, Mockito.atLeastOnce()).publish(captor.capture(),
                valueCaptor.capture());
        Assert.assertEquals(captor.getValue(), "falcon.cluster1.testPipeline.EXECUTION.entity1.backlogInMins");
        WorkflowExecutionContext workflowExecutionContext = getWorkflowExecutionContext();
        backlogMetricEmitterService.onSuccess(workflowExecutionContext);
        Thread.sleep(100);
        Mockito.reset(mockMetricNotificationService);
        Mockito.verify(mockMetricNotificationService, Mockito.times(0)).publish(Mockito.any(String.class),
                Mockito.any(Long.class));

    }

    private WorkflowExecutionContext getWorkflowExecutionContext() {
        Map<WorkflowExecutionArgs, String> args = new HashMap<>();
        args.put(WorkflowExecutionArgs.ENTITY_TYPE, "process");
        args.put(WorkflowExecutionArgs.CLUSTER_NAME, "cluster1");
        args.put(WorkflowExecutionArgs.ENTITY_NAME, "entity1");
        args.put(WorkflowExecutionArgs.NOMINAL_TIME, "2016-06-30-00-00");
        args.put(WorkflowExecutionArgs.OPERATION, "GENERATE");
        WorkflowExecutionContext workflowExecutionContext = new WorkflowExecutionContext(args);
        return workflowExecutionContext;

    }
}
