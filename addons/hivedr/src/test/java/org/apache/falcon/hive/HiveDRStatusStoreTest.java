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
package org.apache.falcon.hive;

import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.hadoop.JailedFileSystem;
import org.apache.falcon.hive.exception.HiveReplicationException;
import org.apache.falcon.hive.util.DRStatusStore;
import org.apache.falcon.hive.util.HiveDRStatusStore;
import org.apache.falcon.hive.util.ReplicationStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Unit tests for HiveDRStatusStore.
 */
@Test
public class HiveDRStatusStoreTest {
    private HiveDRStatusStore drStatusStore;
    private FileSystem fileSystem = new JailedFileSystem();

    public HiveDRStatusStoreTest() throws Exception {
        EmbeddedCluster cluster =  EmbeddedCluster.newCluster("hiveReplTest");
        Path storePath = new Path(DRStatusStore.BASE_DEFAULT_STORE_PATH);

        fileSystem.initialize(LocalFileSystem.getDefaultUri(cluster.getConf()), cluster.getConf());
        if (fileSystem.exists(storePath)) {
            fileSystem.delete(storePath, true);
        }
        FileSystem.mkdirs(fileSystem, storePath, DRStatusStore.DEFAULT_STORE_PERMISSION);
        drStatusStore = new HiveDRStatusStore(fileSystem, fileSystem.getFileStatus(storePath).getGroup());
    }

    @BeforeClass
    public  void updateReplicationStatusTest() throws Exception {
        ReplicationStatus dbStatus = new ReplicationStatus("source", "target", "jobname",
                "Default1", null, ReplicationStatus.Status.SUCCESS, 20L);
        ReplicationStatus table1 = new ReplicationStatus("source", "target", "jobname",
                "Default1", "table1", ReplicationStatus.Status.SUCCESS, 20L);
        ReplicationStatus table2 = new ReplicationStatus("source", "target", "jobname",
                "default1", "Table2", ReplicationStatus.Status.INIT, -1L);
        ReplicationStatus table3 = new ReplicationStatus("source", "target", "jobname",
                "Default1", "Table3", ReplicationStatus.Status.FAILURE, 15L);
        ReplicationStatus table4 = new ReplicationStatus("source", "target", "jobname",
                "default1", "table4", ReplicationStatus.Status.FAILURE, 18L);
        ArrayList<ReplicationStatus> replicationStatusList = new ArrayList<ReplicationStatus>();
        replicationStatusList.add(table1);
        replicationStatusList.add(table2);
        replicationStatusList.add(table3);
        replicationStatusList.add(table4);
        replicationStatusList.add(dbStatus);
        drStatusStore.updateReplicationStatus("jobname", replicationStatusList);
    }

    @Test(expectedExceptions = IOException.class,
            expectedExceptionsMessageRegExp = ".*does not have correct ownership/permissions.*")
    public void testDrStatusStoreWithFakeUser() throws IOException {
        new HiveDRStatusStore(fileSystem, "fakeGroup");
    }

    public  void updateReplicationStatusNewTablesTest() throws Exception {
        ReplicationStatus dbStatus = new ReplicationStatus("source", "target", "jobname2",
                "default2", null, ReplicationStatus.Status.SUCCESS, 20L);
        ReplicationStatus table1 = new ReplicationStatus("source", "target", "jobname2",
                "Default2", "table1", ReplicationStatus.Status.SUCCESS, 20L);
        ReplicationStatus table2 = new ReplicationStatus("source", "target", "jobname2",
                "default2", "Table2", ReplicationStatus.Status.INIT, -1L);
        ReplicationStatus table3 = new ReplicationStatus("source", "target", "jobname2",
                "default2", "table3", ReplicationStatus.Status.FAILURE, 15L);
        ReplicationStatus table4 = new ReplicationStatus("source", "target", "jobname2",
                "Default2", "Table4", ReplicationStatus.Status.FAILURE, 18L);
        ArrayList<ReplicationStatus> replicationStatusList = new ArrayList<ReplicationStatus>();
        replicationStatusList.add(table1);
        replicationStatusList.add(table2);
        replicationStatusList.add(table3);
        replicationStatusList.add(table4);
        replicationStatusList.add(dbStatus);

        drStatusStore.updateReplicationStatus("jobname2", replicationStatusList);
        ReplicationStatus status = drStatusStore.getReplicationStatus("source", "target", "jobname2", "default2");
        Assert.assertEquals(status.getEventId(), 15);
        Assert.assertEquals(status.getStatus(), ReplicationStatus.Status.FAILURE);
        Assert.assertEquals(status.getJobName(), "jobname2");
        Assert.assertEquals(status.getTable(), null);
        Assert.assertEquals(status.getSourceUri(), "source");

        Iterator<ReplicationStatus> iter = drStatusStore.getTableReplicationStatusesInDb("source", "target",
                "jobname2", "default2");
        int size = 0;
        while(iter.hasNext()) {
            iter.next();
            size++;
        }
        Assert.assertEquals(4, size);

        table3 = new ReplicationStatus("source", "target", "jobname2",
                "default2", "table3", ReplicationStatus.Status.SUCCESS, 25L);
        table4 = new ReplicationStatus("source", "target", "jobname2",
                "Default2", "table4", ReplicationStatus.Status.SUCCESS, 22L);
        ReplicationStatus table5 = new ReplicationStatus("source", "target", "jobname2",
                "default2", "Table5", ReplicationStatus.Status.SUCCESS, 18L);
        ReplicationStatus db1table1 = new ReplicationStatus("source", "target", "jobname2",
                "Default1", "Table1", ReplicationStatus.Status.SUCCESS, 18L);
        replicationStatusList = new ArrayList<ReplicationStatus>();
        replicationStatusList.add(table5);
        replicationStatusList.add(table3);
        replicationStatusList.add(table4);
        replicationStatusList.add(db1table1);

        drStatusStore.updateReplicationStatus("jobname2", replicationStatusList);
        status = drStatusStore.getReplicationStatus("source", "target", "jobname2", "default1");
        Assert.assertEquals(status.getEventId(), 18);
        Assert.assertEquals(status.getStatus(), ReplicationStatus.Status.SUCCESS);

        status = drStatusStore.getReplicationStatus("source", "target", "jobname2", "default2");
        Assert.assertEquals(status.getEventId(), 25);
        Assert.assertEquals(status.getStatus(), ReplicationStatus.Status.SUCCESS);

        iter = drStatusStore.getTableReplicationStatusesInDb("source", "target",
                "jobname2", "default2");
        size = 0;
        while(iter.hasNext()) {
            iter.next();
            size++;
        }
        Assert.assertEquals(5, size);
    }

    public void getReplicationStatusDBTest() throws HiveReplicationException {
        ReplicationStatus status = drStatusStore.getReplicationStatus("source", "target", "jobname", "Default1");
        Assert.assertEquals(status.getEventId(), 15);
        Assert.assertEquals(status.getStatus(), ReplicationStatus.Status.FAILURE);
        Assert.assertEquals(status.getJobName(), "jobname");
        Assert.assertEquals(status.getTable(), null);
        Assert.assertEquals(status.getSourceUri(), "source");
    }

    public void checkReplicationConflictTest() throws HiveReplicationException {

        try {
            //same source, same job, same DB, null table : pass
            drStatusStore.checkForReplicationConflict("source", "jobname", "default1", null);

            //same source, same job, same DB, same table : pass
            drStatusStore.checkForReplicationConflict("source", "jobname", "default1", "table1");

            //same source, same job, different DB, null table : pass
            drStatusStore.checkForReplicationConflict("source", "jobname", "diffDB", null);

            //same source, same job, different DB, different table : pass
            drStatusStore.checkForReplicationConflict("source", "jobname", "diffDB", "diffTable");

            // same source, different job, same DB, diff table : pass
            drStatusStore.checkForReplicationConflict("source", "diffJob", "default1", "diffTable");
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

        try {
            // different source, same job, same DB, null table : fail
            drStatusStore.checkForReplicationConflict("diffSource", "jobname", "default1", null);
            Assert.fail();
        } catch (HiveReplicationException e) {
            Assert.assertEquals(e.getMessage(),
                    "Two different sources are attempting to replicate to same db default1."
                            + " New Source = diffSource, Existing Source = source");
        }

        try {
            // same source, different job, same DB, null table : fail
            drStatusStore.checkForReplicationConflict("source", "diffJob", "default1", null);
            Assert.fail();
        } catch (HiveReplicationException e) {
            Assert.assertEquals(e.getMessage(),
                    "Two different jobs are attempting to replicate to same db default1."
                            + " New Job = diffJob, Existing Job = jobname");
        }

        try {
            // same source, different job, same DB, same table : fail
            drStatusStore.checkForReplicationConflict("source", "diffJob", "default1", "table1");
            Assert.fail();
        } catch (HiveReplicationException e) {
            Assert.assertEquals(e.getMessage(),
                    "Two different jobs are trying to replicate to same table table1."
                            + " New job = diffJob, Existing job = jobname");
        }


    }

    public void deleteReplicationStatusTest() throws Exception {
        ReplicationStatus dbStatus = new ReplicationStatus("source", "target", "deleteJob",
                "deleteDB", null, ReplicationStatus.Status.SUCCESS, 20L);
        ReplicationStatus table1 = new ReplicationStatus("source", "target", "deleteJob",
                "deleteDB", "Table1", ReplicationStatus.Status.SUCCESS, 20L);
        ArrayList<ReplicationStatus> replicationStatusList = new ArrayList<ReplicationStatus>();
        replicationStatusList.add(table1);
        replicationStatusList.add(dbStatus);
        drStatusStore.updateReplicationStatus("deleteJob", replicationStatusList);

        ReplicationStatus status = drStatusStore.getReplicationStatus("source", "target", "deleteJob", "deleteDB");
        Path statusPath = drStatusStore.getStatusDirPath(status.getDatabase(), status.getJobName());
        Assert.assertEquals(fileSystem.exists(statusPath), true);

        drStatusStore.deleteReplicationStatus("deleteJob", "deleteDB");
        Assert.assertEquals(fileSystem.exists(statusPath), false);
    }

    public void  getReplicationStatusTableTest() throws HiveReplicationException {
        ReplicationStatus status = drStatusStore.getReplicationStatus("source", "target",
                "jobname", "default1", "table1");
        Assert.assertEquals(status.getEventId(), 20);
        Assert.assertEquals(status.getStatus(), ReplicationStatus.Status.SUCCESS);
        Assert.assertEquals(status.getTable(), "table1");

        status = drStatusStore.getReplicationStatus("source", "target",
                "jobname", "Default1", "Table2");
        Assert.assertEquals(status.getEventId(), -1);
        Assert.assertEquals(status.getStatus(), ReplicationStatus.Status.INIT);
        Assert.assertEquals(status.getTable(), "table2");

        status = drStatusStore.getReplicationStatus("source", "target",
                "jobname", "default1", "Table3");
        Assert.assertEquals(status.getEventId(), 15);
        Assert.assertEquals(status.getStatus(), ReplicationStatus.Status.FAILURE);
        Assert.assertEquals(status.getTable(), "table3");

        status = drStatusStore.getReplicationStatus("source", "target",
                "jobname", "default1", "table4");
        Assert.assertEquals(status.getEventId(), 18);
        Assert.assertEquals(status.getStatus(), ReplicationStatus.Status.FAILURE);
        Assert.assertEquals(status.getTable(), "table4");
    }

    public void getTableReplicationStatusesInDbTest() throws HiveReplicationException {
        Iterator<ReplicationStatus> iter = drStatusStore.getTableReplicationStatusesInDb("source", "target",
                "jobname", "Default1");
        int size = 0;
        while(iter.hasNext()) {
            size++;
            ReplicationStatus status = iter.next();
            if (status.getTable().equals("table3")) {
                Assert.assertEquals(status.getEventId(), 15);
                Assert.assertEquals(status.getStatus(), ReplicationStatus.Status.FAILURE);
                Assert.assertEquals(status.getTable(), "table3");
            }
        }
        Assert.assertEquals(4, size);
    }

    public void fileRotationTest() throws Exception {
        // initialize replication status store for db default3.
        // This should init with eventId = -1 and status = INIT
        ReplicationStatus status = drStatusStore.getReplicationStatus("source", "target",
                "jobname3", "default3");
        Assert.assertEquals(status.getEventId(), -1);
        Assert.assertEquals(status.getStatus(), ReplicationStatus.Status.INIT);

        // update status 5 times resulting in 6 files : latest.json + five rotated files
        ReplicationStatus dbStatus = new ReplicationStatus("source", "target", "jobname3",
                "Default3", null, ReplicationStatus.Status.SUCCESS, 20L);
        ReplicationStatus table1 = new ReplicationStatus("source", "target", "jobname3",
                "default3", "Table1", ReplicationStatus.Status.SUCCESS, 20L);
        ArrayList<ReplicationStatus> replicationStatusList = new ArrayList<ReplicationStatus>();
        replicationStatusList.add(table1);
        replicationStatusList.add(dbStatus);

        for(int i=0; i<5; i++) {
            Thread.sleep(2000);
            drStatusStore.updateReplicationStatus("jobname3", replicationStatusList);
        }

        status = drStatusStore.getReplicationStatus("source", "target", "jobname3", "default3");
        Path statusPath = drStatusStore.getStatusDirPath(status.getDatabase(), status.getJobName());
        RemoteIterator<LocatedFileStatus> iter = fileSystem.listFiles(statusPath, false);
        Assert.assertEquals(getRemoteIterSize(iter), 6);

        drStatusStore.rotateStatusFiles(statusPath, 3, 10000000);
        iter = fileSystem.listFiles(statusPath, false);
        Assert.assertEquals(getRemoteIterSize(iter), 6);

        drStatusStore.rotateStatusFiles(statusPath, 3, 6000);
        iter = fileSystem.listFiles(statusPath, false);
        Assert.assertEquals(getRemoteIterSize(iter), 3);
    }

    public void wrongJobNameTest() throws Exception {
        ReplicationStatus dbStatus = new ReplicationStatus("source", "target", "jobname3",
                "Default3", null, ReplicationStatus.Status.SUCCESS, 20L);
        ArrayList<ReplicationStatus> replicationStatusList = new ArrayList<ReplicationStatus>();
        replicationStatusList.add(dbStatus);

        try {
            drStatusStore.updateReplicationStatus("jobname2", replicationStatusList);
            Assert.fail();
        } catch (HiveReplicationException e) {
            // Expected exception due to jobname mismatch
        }
    }

    @AfterClass
    public void cleanUp() throws IOException {
        fileSystem.delete(new Path(DRStatusStore.BASE_DEFAULT_STORE_PATH), true);
    }

    private int getRemoteIterSize(RemoteIterator<LocatedFileStatus> iter) throws IOException {
        int size = 0;
        while(iter.hasNext()) {
            iter.next();
            size++;
        }
        return size;
    }


}
