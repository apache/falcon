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
package org.apache.ivory.workflow.engine;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Properties;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.ivory.cluster.util.EmbeddedCluster;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.transaction.TransactionManager;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.WorkflowJob.Status;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class OozieWorkflowEngineActionTest {
    private static Cluster cluster;
    private static final String WF_PATH = "/ivory/test/workflow";
    private static final String COORD_PATH = "/ivory/test/coordinator";
    private static EmbeddedCluster embCluster;
    private static OozieWorkflowEngine workflowEgine = new OozieWorkflowEngine();
    
    @BeforeClass
    public void setUp() throws Exception {
        embCluster = EmbeddedCluster.newCluster(RandomStringUtils.randomAlphabetic(5), false);
        cluster = embCluster.getCluster();
        ConfigurationStore.get().publish(EntityType.CLUSTER, cluster);

        // copy workflow
        Path hdfsUrl = new Path(ClusterHelper.getHdfsUrl(cluster));
        Path workflowPath = new Path(WF_PATH);
        FileSystem fs = hdfsUrl.getFileSystem(new Configuration());
        FsPermission perm = new FsPermission((short) 511);
        fs.mkdirs(workflowPath, perm);
        fs.copyFromLocalFile(new Path(this.getClass().getResource("/fs-workflow.xml").getPath()), new Path(workflowPath,
                "workflow.xml"));

        // copy coord
        Path coordPath = new Path(COORD_PATH);
        fs.mkdirs(coordPath);
        fs.copyFromLocalFile(new Path(this.getClass().getResource("/fs-coord.xml").getPath()), new Path(coordPath, "coordinator.xml"));
    }

    @AfterClass
    public void tearDown() {
        embCluster.shutdown();
    }
    
    private String submitWorkflow() throws Exception {
        Path hdfsUrl = new Path(ClusterHelper.getHdfsUrl(cluster));
        Properties props = new Properties();
        props.put("nameNode", hdfsUrl.toString());
        props.put(OozieClient.USER_NAME, "guest");
        props.put(OozieClient.APP_PATH, "${nameNode}" + WF_PATH);

        return workflowEgine.run(cluster.getName(), props);
    }

    private String submitCoord() throws Exception {
        Path hdfsUrl = new Path(ClusterHelper.getHdfsUrl(cluster));
        Properties props = new Properties();
        props.put("nameNode", hdfsUrl.toString());
        props.put(OozieClient.USER_NAME, "guest");
        props.put(OozieClient.COORDINATOR_APP_PATH, "${nameNode}" + COORD_PATH);

        return workflowEgine.run(cluster.getName(), props);
    }

    @Test
    public void testRunRollback() throws Exception {
        TransactionManager.startTransaction();
        String jobId = submitWorkflow();
        TransactionManager.rollback();

        assertStatus(jobId, WorkflowJob.Status.KILLED);
    }

    @Test
    public void testRunCommit() throws Exception {
        TransactionManager.startTransaction();
        String jobId = submitWorkflow();
        TransactionManager.commit();

        assertStatus(jobId, WorkflowJob.Status.RUNNING);
    }

    @Test
    public void testSuspendRollback() throws Exception {
        String jobId = submitWorkflow();

        TransactionManager.startTransaction();
        workflowEgine.suspend(cluster.getName(), jobId);
        TransactionManager.rollback();

        assertStatus(jobId, WorkflowJob.Status.RUNNING);
    }

    @Test
    public void testSuspendCommit() throws Exception {
        String jobId = submitWorkflow();

        TransactionManager.startTransaction();
        workflowEgine.suspend(cluster.getName(), jobId);
        TransactionManager.commit();

        assertStatus(jobId, WorkflowJob.Status.SUSPENDED);
    }

    @Test
    public void testResumeRollback() throws Exception {
        String jobId = submitWorkflow();
        workflowEgine.suspend(cluster.getName(), jobId);

        TransactionManager.startTransaction();
        workflowEgine.resume(cluster.getName(), jobId);
        TransactionManager.rollback();

        assertStatus(jobId, WorkflowJob.Status.SUSPENDED);
    }

    @Test
    public void testResumeCommit() throws Exception {
        String jobId = submitWorkflow();
        workflowEgine.suspend(cluster.getName(), jobId);

        TransactionManager.startTransaction();
        workflowEgine.resume(cluster.getName(), jobId);
        TransactionManager.commit();

        assertStatus(jobId, WorkflowJob.Status.RUNNING);
    }

    @Test
    public void testChangeRollback() throws Exception {
        String jobId = submitCoord();

        TransactionManager.startTransaction();
        workflowEgine.change(cluster.getName(), jobId, OozieClient.CHANGE_VALUE_CONCURRENCY + "=4");
        TransactionManager.rollback();

        assertCoordConcurrency(jobId, 2);
    }

    @Test
    public void testChangeCommit() throws Exception {
        String jobId = submitCoord();

        TransactionManager.startTransaction();
        workflowEgine.change(cluster.getName(), jobId, OozieClient.CHANGE_VALUE_CONCURRENCY + "=4");
        TransactionManager.commit();

        assertCoordConcurrency(jobId, 4);
    }

    @Test
    public void testKillRollback() throws Exception {
        String jobId = submitWorkflow();

        TransactionManager.startTransaction();
        workflowEgine.kill(cluster.getName(), jobId);
        TransactionManager.rollback();

        assertStatus(jobId, WorkflowJob.Status.RUNNING);
    }

    @Test
    public void testKillCommit() throws Exception {
        String jobId = submitWorkflow();

        TransactionManager.startTransaction();
        workflowEgine.kill(cluster.getName(), jobId);
        TransactionManager.commit();

        assertStatus(jobId, WorkflowJob.Status.KILLED);
    }

    protected void assertCoordConcurrency(String jobId, int concurrency) throws Exception {
        OozieClient client = OozieClientFactory.get(cluster);
        CoordinatorJob coord = client.getCoordJobInfo(jobId);
        assertEquals(concurrency, coord.getConcurrency());
    }

    private void assertStatus(String jobId, Status expected) throws Exception {
        OozieClient client = OozieClientFactory.get(cluster);
        WorkflowJob jobInfo = client.getJobInfo(jobId);
        assertEquals(expected, jobInfo.getStatus());
    }
}
