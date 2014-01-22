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
package org.apache.falcon.cleanup;

import org.apache.falcon.FalconException;
import org.apache.falcon.Tag;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.AbstractTestBase;
import org.apache.falcon.entity.CatalogStorage;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;

/**
 * Test for log cleanup service.
 */
public class LogCleanupServiceTest extends AbstractTestBase {

    private FileSystem fs;
    private FileSystem tfs;
    private EmbeddedCluster targetDfsCluster;
    private Path sourceStagingPath1;
    private Path sourceStagingPath2;
    private Path targetStagingPath1;
    private Path targetStagingPath2;

    private final Path instanceLogPath = new Path("/projects/falcon/staging/falcon/workflows/process/"
        + "sample" + "/logs/job-2010-01-01-01-00/000");
    private final Path instanceLogPath1 = new Path("/projects/falcon/staging/falcon/workflows/process/"
        + "sample" + "/logs/job-2010-01-01-01-00/001");
    private final Path instanceLogPath2 = new Path("/projects/falcon/staging/falcon/workflows/process/"
        + "sample" + "/logs/job-2010-01-01-02-00/001");
    private final Path instanceLogPath3 = new Path("/projects/falcon/staging/falcon/workflows/process/"
        + "sample2" + "/logs/job-2010-01-01-01-00/000");
    private final Path instanceLogPath4 = new Path("/projects/falcon/staging/falcon/workflows/process/"
        + "sample" + "/logs/latedata/2010-01-01-01-00");
    private final Path feedInstanceLogPath = new Path("/projects/falcon/staging/falcon/workflows/feed/"
        + "impressionFeed" + "/logs/job-2010-01-01-01-00/testCluster/000");
    private final Path feedInstanceLogPath1 = new Path("/projects/falcon/staging/falcon/workflows/feed/"
        + "impressionFeed2" + "/logs/job-2010-01-01-01-00/testCluster/000");


    @AfterClass
    public void tearDown() {
        this.dfsCluster.shutdown();
        this.targetDfsCluster.shutdown();
    }

    @Override
    @BeforeClass
    public void setup() throws Exception {
        this.dfsCluster = EmbeddedCluster.newCluster("testCluster");
        conf = dfsCluster.getConf();
        fs = dfsCluster.getFileSystem();
        fs.delete(new Path("/"), true);

        storeEntity(EntityType.CLUSTER, "testCluster");
        System.setProperty("test.build.data", "target/tdfs/data" + System.currentTimeMillis());
        this.targetDfsCluster = EmbeddedCluster.newCluster("backupCluster");
        conf = targetDfsCluster.getConf();

        storeEntity(EntityType.CLUSTER, "backupCluster");
        storeEntity(EntityType.FEED, "impressionFeed");
        storeEntity(EntityType.FEED, "clicksFeed");
        storeEntity(EntityType.FEED, "imp-click-join1");
        storeEntity(EntityType.FEED, "imp-click-join2");
        storeEntity(EntityType.PROCESS, "sample");
        Process process = ConfigurationStore.get().get(EntityType.PROCESS, "sample");
        Process otherProcess = (Process) process.copy();
        otherProcess.setName("sample2");
        otherProcess.setFrequency(new Frequency("days(1)"));
        ConfigurationStore.get().remove(EntityType.PROCESS,
                otherProcess.getName());
        ConfigurationStore.get().publish(EntityType.PROCESS, otherProcess);

        fs.mkdirs(instanceLogPath);
        fs.mkdirs(instanceLogPath1);
        fs.mkdirs(instanceLogPath2);
        fs.mkdirs(instanceLogPath3);
        fs.mkdirs(instanceLogPath4);

        // fs.setTimes wont work on dirs
        fs.createNewFile(new Path(instanceLogPath, "oozie.log"));
        fs.createNewFile(new Path(instanceLogPath, "pigAction_SUCCEEDED.log"));

        tfs = targetDfsCluster.getFileSystem();
        tfs.delete(new Path("/"), true);
        fs.mkdirs(feedInstanceLogPath);
        fs.mkdirs(feedInstanceLogPath1);
        tfs.mkdirs(feedInstanceLogPath);
        tfs.mkdirs(feedInstanceLogPath1);
        fs.createNewFile(new Path(feedInstanceLogPath, "oozie.log"));
        tfs.createNewFile(new Path(feedInstanceLogPath, "oozie.log"));

        // table feed staging dir setup
        initializeStagingDirs();
        createStageData(sourceStagingPath1, targetStagingPath1, 0);
        createStageData(sourceStagingPath2, targetStagingPath2, 10000);
        Thread.sleep(1000);
    }

    private void initializeStagingDirs() throws Exception {
        final InputStream inputStream = getClass().getResourceAsStream("/config/feed/hive-table-feed.xml");
        Feed tableFeed = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(inputStream);
        getStore().publish(EntityType.FEED, tableFeed);

        final Cluster srcCluster = dfsCluster.getCluster();
        final CatalogStorage sourceStorage = (CatalogStorage) FeedHelper.createStorage(srcCluster, tableFeed);
        String sourceStagingDir = FeedHelper.getStagingDir(srcCluster, tableFeed, sourceStorage, Tag.REPLICATION);

        sourceStagingPath1 = new Path(sourceStagingDir + "/ds=2012092400/" + System.currentTimeMillis());
        sourceStagingPath2 = new Path(sourceStagingDir + "/ds=2012092500/" + System.currentTimeMillis());

        final Cluster targetCluster = targetDfsCluster.getCluster();
        final CatalogStorage targetStorage = (CatalogStorage) FeedHelper.createStorage(targetCluster, tableFeed);
        String targetStagingDir = FeedHelper.getStagingDir(targetCluster, tableFeed, targetStorage, Tag.REPLICATION);

        targetStagingPath1 = new Path(targetStagingDir + "/ds=2012092400/" + System.currentTimeMillis());
        targetStagingPath2 = new Path(targetStagingDir + "/ds=2012092500/" + System.currentTimeMillis());
    }

    private void createStageData(Path sourcePath, Path targetPath, int offset) throws Exception {
        fs.mkdirs(sourcePath);
        Path metaSource = new Path(sourcePath, "_metadata.xml");
        Path dataSource = new Path(sourcePath, "data.txt");
        fs.createNewFile(metaSource);
        fs.createNewFile(dataSource);
        FileStatus status = fs.getFileStatus(metaSource);
        fs.setTimes(metaSource, status.getModificationTime() + offset, status.getAccessTime());
        status = fs.getFileStatus(dataSource);
        fs.setTimes(dataSource, status.getModificationTime() + offset, status.getAccessTime());

        tfs.mkdirs(targetPath);
        Path metaTarget = new Path(targetPath, "_metadata.xml");
        Path dataTarget = new Path(targetPath, "data.txt");
        tfs.createNewFile(metaTarget);
        tfs.createNewFile(dataTarget);
        status = tfs.getFileStatus(metaTarget);
        tfs.setTimes(metaTarget, status.getModificationTime() + offset, status.getAccessTime());
        status = tfs.getFileStatus(dataTarget);
        tfs.setTimes(dataTarget, status.getModificationTime() + offset, status.getAccessTime());
    }

    @Test
    public void testProcessLogs() throws IOException, FalconException, InterruptedException {

        AbstractCleanupHandler processCleanupHandler = new ProcessCleanupHandler();
        processCleanupHandler.cleanup();

        Assert.assertFalse(fs.exists(instanceLogPath));
        Assert.assertFalse(fs.exists(instanceLogPath1));
        Assert.assertFalse(fs.exists(instanceLogPath2));
        Assert.assertTrue(fs.exists(instanceLogPath3));
    }

    @Test (enabled = false)
    public void testFeedLogs() throws IOException, FalconException, InterruptedException {

        AbstractCleanupHandler feedCleanupHandler = new FeedCleanupHandler();
        feedCleanupHandler.cleanup();

        Assert.assertFalse(fs.exists(feedInstanceLogPath));
        Assert.assertFalse(tfs.exists(feedInstanceLogPath));
        Assert.assertTrue(fs.exists(feedInstanceLogPath1));
        Assert.assertTrue(tfs.exists(feedInstanceLogPath1));

        // source table replication staging dirs
        Assert.assertFalse(fs.exists(new Path(sourceStagingPath1, "_metadata.xml")));
        Assert.assertFalse(fs.exists(new Path(sourceStagingPath1, "data.txt")));

        Assert.assertTrue(fs.exists(new Path(sourceStagingPath2, "_metadata.xml")));
        Assert.assertTrue(fs.exists(new Path(sourceStagingPath2, "data.txt")));

        // target table replication staging dirs
        Assert.assertFalse(tfs.exists(new Path(targetStagingPath1, "_metadata.xml")));
        Assert.assertFalse(tfs.exists(new Path(targetStagingPath1, "data.txt")));

        Assert.assertTrue(tfs.exists(new Path(targetStagingPath2, "_metadata.xml")));
        Assert.assertTrue(tfs.exists(new Path(targetStagingPath2, "data.txt")));
    }
}
