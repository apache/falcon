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
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.AbstractTestBase;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Process;
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
        Thread.sleep(1000);
    }

    private void initializeStagingDirs() throws Exception {
        final InputStream inputStream = getClass().getResourceAsStream("/config/feed/hive-table-feed.xml");
        Feed tableFeed = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(inputStream);
        getStore().publish(EntityType.FEED, tableFeed);
    }

    @Test
    public void testProcessLogs() throws IOException, FalconException, InterruptedException {

        Assert.assertTrue(fs.exists(instanceLogPath));
        Assert.assertTrue(fs.exists(instanceLogPath1));
        Assert.assertTrue(fs.exists(instanceLogPath2));
        Assert.assertTrue(fs.exists(instanceLogPath3));

        AbstractCleanupHandler processCleanupHandler = new ProcessCleanupHandler();
        processCleanupHandler.cleanup();

        Assert.assertFalse(fs.exists(instanceLogPath));
        Assert.assertFalse(fs.exists(instanceLogPath1));
        Assert.assertFalse(fs.exists(instanceLogPath2));
        Assert.assertTrue(fs.exists(instanceLogPath3));
    }

    @Test
    public void testFeedLogs() throws IOException, FalconException, InterruptedException {

        Assert.assertTrue(fs.exists(feedInstanceLogPath));
        Assert.assertTrue(tfs.exists(feedInstanceLogPath));
        Assert.assertTrue(fs.exists(feedInstanceLogPath1));
        Assert.assertTrue(tfs.exists(feedInstanceLogPath1));

        AbstractCleanupHandler feedCleanupHandler = new FeedCleanupHandler();
        feedCleanupHandler.cleanup();

        Assert.assertFalse(fs.exists(feedInstanceLogPath));
        Assert.assertFalse(tfs.exists(feedInstanceLogPath));
        Assert.assertTrue(fs.exists(feedInstanceLogPath1));
        Assert.assertTrue(tfs.exists(feedInstanceLogPath1));
    }
}
