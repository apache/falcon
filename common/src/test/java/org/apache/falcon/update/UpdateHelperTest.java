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

package org.apache.falcon.update;

import org.apache.falcon.FalconException;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.AbstractTestBase;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.parser.EntityParserFactory;
import org.apache.falcon.entity.parser.FeedEntityParser;
import org.apache.falcon.entity.parser.ProcessEntityParser;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.entity.v0.feed.Partition;
import org.apache.falcon.entity.v0.feed.Properties;
import org.apache.falcon.entity.v0.feed.Property;
import org.apache.falcon.entity.v0.process.PolicyType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;

/**
 * Test for Update helper methods.
 */
public class UpdateHelperTest extends AbstractTestBase {
    private final FeedEntityParser parser = (FeedEntityParser)EntityParserFactory.getParser(EntityType.FEED);
    private final ProcessEntityParser processParser =
        (ProcessEntityParser)EntityParserFactory.getParser(EntityType.PROCESS);

    @BeforeClass
    public void init() throws Exception {
        this.dfsCluster = EmbeddedCluster.newCluster("testCluster");
        this.conf = dfsCluster.getConf();
        setup();
    }

    @AfterClass
    public void tearDown() {
        this.dfsCluster.shutdown();
    }

    @BeforeMethod
    public void setUp() throws Exception {
        storeEntity(EntityType.CLUSTER, "testCluster");
        storeEntity(EntityType.CLUSTER, "backupCluster");
        storeEntity(EntityType.FEED, "clicksFeed");
        storeEntity(EntityType.FEED, "impressionFeed");
        storeEntity(EntityType.FEED, "imp-click-join1");
        storeEntity(EntityType.FEED, "imp-click-join2");
    }

    private void prepare(Process process) throws IOException, FalconException {
        FileSystem fs = dfsCluster.getFileSystem();
        Cluster clusterEntity = ConfigurationStore.get().get(EntityType.CLUSTER, "testCluster");
        Path staging = EntityUtil.getNewStagingPath(clusterEntity, process);
        fs.mkdirs(staging);
        fs.create(new Path(staging, "workflow.xml")).close();
        fs.create(new Path(staging, "checksums")).close();
    }

    @Test
    public void testCopyUserWorkflow() throws Exception {
        FileSystem fs = dfsCluster.getFileSystem();
        Path basePath = new Path("/tmp/basepath");
        fs.mkdirs(basePath);
        Path wfdir = new Path(basePath, "workflow");
        fs.mkdirs(wfdir);
        Path wf = new Path(wfdir, "workflow.xml");
        Path lib = new Path(wfdir, "common.jar");
        fs.create(wf).close();
        fs.create(lib).close();
        Path dest = new Path("/tmp/dest");
        UpdateHelper.checksumAndCopy(fs, wfdir, dest);
        Assert.assertTrue(fs.exists(new Path(dest, "workflow.xml")) && fs.isFile(new Path(dest, "workflow.xml")));
        Assert.assertTrue(fs.exists(new Path(dest, "common.jar")) && fs.isFile(new Path(dest, "common.jar")));

        fs.delete(dest, true);
        UpdateHelper.checksumAndCopy(fs, wf, dest);
        Assert.assertTrue(fs.exists(new Path(dest, "workflow.xml")) && fs.isFile(new Path(dest, "workflow.xml")));
    }

    @Test
    public void testIsWorkflowUpdated() throws IOException, FalconException {
        FileSystem fs = dfsCluster.getFileSystem();
        Process process = processParser.parseAndValidate(this.getClass().getResourceAsStream(PROCESS_XML));
        String cluster = "testCluster";
        Cluster clusterEntity = ConfigurationStore.get().get(EntityType.CLUSTER, cluster);
        Path staging = EntityUtil.getNewStagingPath(clusterEntity, process);
        fs.mkdirs(staging);
        fs.create(new Path(staging, "workflow.xml")).close();

        //Update if there is no checksum file
        Assert.assertTrue(UpdateHelper.isWorkflowUpdated(cluster, process, staging));

        //No update if there is no new file
        fs.create(new Path(staging, "checksums")).close();
        Assert.assertFalse(UpdateHelper.isWorkflowUpdated(cluster, process, staging));

        //Update if there is new lib
        Path libpath = new Path("/falcon/test/lib");
        process.getWorkflow().setLib(libpath.toString());
        fs.mkdirs(libpath);
        Path lib = new Path(libpath, "new.jar");
        fs.create(lib).close();
        Assert.assertTrue(UpdateHelper.isWorkflowUpdated(cluster, process, staging));

        //Don't Update if the lib is not updated
        fs.delete(new Path(staging, "checksums"), true);
        FSDataOutputStream stream = fs.create(new Path(staging, "checksums"));
        stream.write((dfsCluster.getConf().get("fs.default.name") + lib.toString() + "="
                + fs.getFileChecksum(lib).toString() + "\n").getBytes());
        stream.close();
        Assert.assertFalse(UpdateHelper.isWorkflowUpdated(cluster, process, staging));

        //Update if the lib is updated
        fs.delete(lib, true);
        stream = fs.create(lib);
        stream.writeChars("some new jar");
        stream.close();
        Assert.assertTrue(UpdateHelper.isWorkflowUpdated(cluster, process, staging));

        //Update if the lib is deleted
        fs.delete(lib, true);
        Assert.assertTrue(UpdateHelper.isWorkflowUpdated(cluster, process, staging));
    }

    @Test
    public void testIsEntityUpdated() throws Exception {
        Feed oldFeed = parser.parseAndValidate(this.getClass().getResourceAsStream(FEED_XML));
        String cluster = "testCluster";
        Feed newFeed = (Feed) oldFeed.copy();
        Cluster clusterEntity = ConfigurationStore.get().get(EntityType.CLUSTER, cluster);

        Path feedPath = EntityUtil.getNewStagingPath(clusterEntity, oldFeed);
        Assert.assertFalse(UpdateHelper.isEntityUpdated(oldFeed, newFeed, cluster, feedPath));

        newFeed.setGroups("newgroups");
        Assert.assertFalse(UpdateHelper.isEntityUpdated(oldFeed, newFeed, cluster, feedPath));
        newFeed.getLateArrival().setCutOff(Frequency.fromString("hours(8)"));
        Assert.assertFalse(UpdateHelper.isEntityUpdated(oldFeed, newFeed, cluster, feedPath));
        newFeed.setFrequency(Frequency.fromString("days(1)"));
        Assert.assertTrue(UpdateHelper.isEntityUpdated(oldFeed, newFeed, cluster, feedPath));

        Process oldProcess = processParser.parseAndValidate(this.getClass().getResourceAsStream(PROCESS_XML));
        prepare(oldProcess);
        Process newProcess = (Process) oldProcess.copy();
        Path procPath = EntityUtil.getNewStagingPath(clusterEntity, oldProcess);

        newProcess.getRetry().setPolicy(PolicyType.FINAL);
        Assert.assertFalse(UpdateHelper.isEntityUpdated(oldProcess, newProcess, cluster, procPath));
        newProcess.getLateProcess().getLateInputs().remove(1);
        Assert.assertFalse(UpdateHelper.isEntityUpdated(oldProcess, newProcess, cluster, procPath));
        newProcess.getLateProcess().setPolicy(PolicyType.PERIODIC);
        Assert.assertFalse(UpdateHelper.isEntityUpdated(oldProcess, newProcess, cluster, procPath));
        newProcess.setFrequency(Frequency.fromString("days(1)"));
        Assert.assertTrue(UpdateHelper.isEntityUpdated(oldProcess, newProcess, cluster, procPath));

        //Adding new cluster shouldn't cause update in the old cluster
        newProcess = (Process) oldProcess.copy();
        org.apache.falcon.entity.v0.process.Cluster procCluster = new org.apache.falcon.entity.v0.process.Cluster();
        procCluster.setName("newcluster");
        procCluster.setValidity(newProcess.getClusters().getClusters().get(0).getValidity());
        newProcess.getClusters().getClusters().add(procCluster);
        Assert.assertFalse(UpdateHelper.isEntityUpdated(oldProcess, newProcess, cluster, procPath));

        //In the case of incomplete update, where new entity is scheduled but still not updated in config store,
        //another update call shouldn't cause update in workflow engine
        newProcess.setFrequency(Frequency.fromString("days(1)"));
        procPath = EntityUtil.getNewStagingPath(clusterEntity, newProcess);
        Assert.assertFalse(UpdateHelper.isEntityUpdated(oldProcess, newProcess, cluster, procPath));
    }

    @Test
    public void testShouldUpdateAffectedEntities() throws Exception {
        Feed oldFeed = parser.parseAndValidate(this.getClass().getResourceAsStream(FEED_XML));

        Feed newFeed = (Feed) oldFeed.copy();
        Process process = processParser.parseAndValidate(this.getClass().getResourceAsStream(PROCESS_XML));
        prepare(process);
        String cluster = process.getClusters().getClusters().get(0).getName();

        Assert.assertFalse(UpdateHelper.shouldUpdate(oldFeed, newFeed, process, cluster));

        newFeed.getLateArrival().setCutOff(Frequency.fromString("hours(1)"));
        Assert.assertFalse(UpdateHelper.shouldUpdate(oldFeed, newFeed, process, cluster));

        newFeed.getLateArrival().setCutOff(oldFeed.getLateArrival().getCutOff());
        getLocation(newFeed, LocationType.DATA, cluster).setPath("/test");
        Assert.assertTrue(UpdateHelper.shouldUpdate(oldFeed, newFeed, process, cluster));

        getLocation(newFeed, LocationType.DATA, cluster).setPath(
                getLocation(oldFeed, LocationType.DATA, cluster).getPath());
        newFeed.setFrequency(Frequency.fromString("months(1)"));
        Assert.assertTrue(UpdateHelper.shouldUpdate(oldFeed, newFeed, process, cluster));

        newFeed.setFrequency(oldFeed.getFrequency());
        Partition partition = new Partition();
        partition.setName("1");
        newFeed.getPartitions().getPartitions().add(partition);
        Assert.assertFalse(UpdateHelper.shouldUpdate(oldFeed, newFeed, process, cluster));

        Property property = new Property();
        property.setName("1");
        property.setValue("1");
        newFeed.setProperties(new Properties());
        newFeed.getProperties().getProperties().add(property);
        Assert.assertFalse(UpdateHelper.shouldUpdate(oldFeed, newFeed, process, cluster));

        newFeed.getProperties().getProperties().remove(0);
        Assert.assertFalse(UpdateHelper.shouldUpdate(oldFeed, newFeed, process, cluster));

        //Change in start time should trigger process update as instance time changes
        FeedHelper.getCluster(newFeed, process.getClusters().getClusters().get(0).getName()).getValidity().setStart(
                SchemaHelper.parseDateUTC("2012-11-01T00:00Z"));
        Assert.assertTrue(UpdateHelper.shouldUpdate(oldFeed, newFeed, process, cluster));

        FeedHelper.getCluster(newFeed, process.getClusters().getClusters().get(0).getName()).getValidity().
                setStart(FeedHelper.getCluster(oldFeed,
                        process.getClusters().getClusters().get(0).getName()).getValidity().getStart());

        //Change location to table should trigger process update
        newFeed.setLocations(null);
        CatalogTable table = new CatalogTable();
        table.setUri("catalog:default:clicks-blah#ds=${YEAR}-${MONTH}-${DAY}-${HOUR}");
        newFeed.setTable(table);
        Assert.assertFalse(UpdateHelper.shouldUpdate(oldFeed, newFeed, process, cluster));
    }

    @Test
    public void testIsEntityUpdatedTable() throws Exception {
        InputStream inputStream = getClass().getResourceAsStream("/config/feed/hive-table-feed.xml");
        Feed oldTableFeed = (Feed) EntityType.FEED.getUnmarshaller().unmarshal(inputStream);
        getStore().publish(EntityType.FEED, oldTableFeed);

        String cluster = "testCluster";
        Cluster clusterEntity = ConfigurationStore.get().get(EntityType.CLUSTER, cluster);
        Path feedPath = EntityUtil.getNewStagingPath(clusterEntity, oldTableFeed);
        Feed newTableFeed = (Feed) oldTableFeed.copy();
        Assert.assertFalse(UpdateHelper.isEntityUpdated(oldTableFeed, newTableFeed, cluster, feedPath));

        newTableFeed.setGroups("newgroups");
        Assert.assertFalse(UpdateHelper.isEntityUpdated(oldTableFeed, newTableFeed, cluster, feedPath));
        newTableFeed.setFrequency(Frequency.fromString("days(1)"));
        Assert.assertTrue(UpdateHelper.isEntityUpdated(oldTableFeed, newTableFeed, cluster, feedPath));

        final CatalogTable table = new CatalogTable();
        table.setUri("catalog:default:clicks-blah#ds=${YEAR}-${MONTH}-${DAY}-${HOUR}");
        newTableFeed.setTable(table);
        Assert.assertTrue(UpdateHelper.isEntityUpdated(oldTableFeed, newTableFeed, cluster, feedPath));

        inputStream = getClass().getResourceAsStream("/config/process/process-table.xml");
        Process oldProcess = (Process) EntityType.PROCESS.getUnmarshaller().unmarshal(inputStream);
        FileSystem fs = dfsCluster.getFileSystem();
        Path staging = EntityUtil.getNewStagingPath(clusterEntity, oldProcess);
        fs.mkdirs(staging);
        fs.create(new Path(staging, "workflow.xml")).close();
        fs.create(new Path(staging, "checksums")).close();
        Process newProcess = (Process) oldProcess.copy();
        Path procPath = EntityUtil.getNewStagingPath(clusterEntity, oldProcess);

        newProcess.getRetry().setPolicy(PolicyType.FINAL);
        Assert.assertFalse(UpdateHelper.isEntityUpdated(oldProcess, newProcess, cluster, procPath));
        newProcess.setFrequency(Frequency.fromString("days(1)"));
        Assert.assertTrue(UpdateHelper.isEntityUpdated(oldProcess, newProcess, cluster, procPath));
    }

    @Test
    public void testIsEntityACLUpdated() throws Exception {
        Feed oldFeed = parser.parseAndValidate(this.getClass().getResourceAsStream(FEED_XML));
        String cluster = "testCluster";
        Feed newFeed = (Feed) oldFeed.copy();
        Cluster clusterEntity = ConfigurationStore.get().get(EntityType.CLUSTER, cluster);

        Path feedPath = EntityUtil.getNewStagingPath(clusterEntity, oldFeed);
        Assert.assertFalse(UpdateHelper.isEntityUpdated(oldFeed, newFeed, cluster, feedPath));

        Assert.assertFalse(UpdateHelper.isEntityUpdated(oldFeed, newFeed, cluster, feedPath));
        newFeed.getACL().setOwner("new-user");
        newFeed.getACL().setGroup("new-group");
        Assert.assertNotEquals(oldFeed.getACL().getOwner(), newFeed.getACL().getOwner());
        Assert.assertNotEquals(oldFeed.getACL().getGroup(), newFeed.getACL().getGroup());
        Assert.assertTrue(UpdateHelper.isEntityUpdated(oldFeed, newFeed, cluster, feedPath));

        Process oldProcess = processParser.parseAndValidate(this.getClass().getResourceAsStream(PROCESS_XML));
        prepare(oldProcess);
        Process newProcess = (Process) oldProcess.copy();
        Path procPath = EntityUtil.getNewStagingPath(clusterEntity, oldProcess);

        Assert.assertFalse(UpdateHelper.isEntityUpdated(oldProcess, newProcess, cluster, procPath));
        org.apache.falcon.entity.v0.process.ACL processACL =
                new org.apache.falcon.entity.v0.process.ACL();
        processACL.setOwner("owner");
        processACL.setOwner("group");
        newProcess.setACL(processACL);
        Assert.assertTrue(UpdateHelper.isEntityUpdated(oldProcess, newProcess, cluster, procPath));
    }

    private static Location getLocation(Feed feed, LocationType type, String cluster) {
        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(feed, cluster);
        if (feedCluster.getLocations() != null) {
            return getLocation(feedCluster.getLocations(), type);
        }
        return getLocation(feed.getLocations(), type);
    }

    private static Location getLocation(Locations locations, LocationType type) {
        for (Location loc : locations.getLocations()) {
            if (loc.getType() == type) {
                return loc;
            }
        }
        Location loc = new Location();
        loc.setPath("/tmp");
        loc.setType(type);
        return loc;
    }
}
