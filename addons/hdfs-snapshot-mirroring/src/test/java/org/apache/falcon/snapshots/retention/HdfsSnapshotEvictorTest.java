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

package org.apache.falcon.snapshots.retention;

import org.apache.falcon.FalconException;
import org.apache.falcon.cluster.util.MiniHdfsClusterUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Files;

/**
 * Hdfs snapshot evictor unit tests.
 */
public class HdfsSnapshotEvictorTest extends HdfsSnapshotEvictor {

    private static final int NUM_FILES = 7;
    private MiniDFSCluster miniDFSCluster;
    private DistributedFileSystem miniDfs;
    private File baseDir;
    private Path evictionDir = new Path("/apps/falcon/snapshot-eviction/");
    private FsPermission fsPermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);

    @BeforeClass
    public void init() throws Exception {
        baseDir = Files.createTempDirectory("test_snapshot-eviction_hdfs").toFile().getAbsoluteFile();
        miniDFSCluster = MiniHdfsClusterUtil.initMiniDfs(MiniHdfsClusterUtil.SNAPSHOT_EVICTION_TEST_PORT, baseDir);
        miniDfs = miniDFSCluster.getFileSystem();
        miniDfs.mkdirs(evictionDir, fsPermission);
        miniDfs.allowSnapshot(evictionDir);
        createSnapshotsForEviction();
    }

    private void createSnapshotsForEviction() throws Exception {
        for (int i = 0; i < NUM_FILES; i++) {
            miniDfs.createSnapshot(evictionDir, String.valueOf(i));
            Thread.sleep(10000);
        }
    }

    @Test
    public void evictionTest() throws Exception {
        Path snapshotDir = new Path(evictionDir, ".snapshot");
        FileStatus[] fileStatuses = miniDfs.listStatus(snapshotDir);
        Assert.assertEquals(fileStatuses.length, NUM_FILES);

        evictSnapshots(miniDfs, evictionDir.toString(), "minutes(1)", NUM_FILES + 1);
        fileStatuses = miniDfs.listStatus(snapshotDir);
        Assert.assertEquals(fileStatuses.length, NUM_FILES);

        evictSnapshots(miniDfs, evictionDir.toString(), "minutes(1)", NUM_FILES - 1);
        fileStatuses = miniDfs.listStatus(snapshotDir);
        Assert.assertEquals(fileStatuses.length, NUM_FILES - 1);

        evictSnapshots(miniDfs, evictionDir.toString(), "minutes(1)", 2);
        fileStatuses = miniDfs.listStatus(snapshotDir);
        Assert.assertTrue(fileStatuses.length >= 5);
    }

    @Test(expectedExceptions = FalconException.class,
            expectedExceptionsMessageRegExp = "Unable to evict snapshots from dir /apps/falcon/non-snapshot-eviction")
    public void evictionTestCannotSnapshot() throws Exception {
        Path nonSnapshotDir = new Path("/apps/falcon/non-snapshot-eviction/");
        miniDfs.mkdirs(nonSnapshotDir, fsPermission);
        evictSnapshots(miniDfs, nonSnapshotDir.toString(), "minutes(1)", NUM_FILES);
    }

    @AfterClass
    public void cleanup() throws Exception {
        MiniHdfsClusterUtil.cleanupDfs(miniDFSCluster, baseDir);

    }
}
