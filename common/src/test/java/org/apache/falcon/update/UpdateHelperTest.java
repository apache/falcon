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
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.parser.EntityParserFactory;
import org.apache.falcon.entity.parser.FeedEntityParser;
import org.apache.falcon.entity.parser.ProcessEntityParser;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.datasource.Datasource;
import org.apache.falcon.entity.v0.datasource.Credential;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.ACL;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.ClusterLocationType;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.entity.v0.feed.Partition;
import org.apache.falcon.entity.v0.feed.Properties;
import org.apache.falcon.entity.v0.feed.Property;
import org.apache.falcon.entity.v0.process.LateProcess;
import org.apache.falcon.entity.v0.process.PolicyType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
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
        storeEntity(EntityType.DATASOURCE, "datasource1");
        storeEntity(EntityType.DATASOURCE, "datasource2");
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
    public void testIsEntityUpdated() throws Exception {
        Feed oldFeed = parser.parseAndValidate(this.getClass().getResourceAsStream(FEED_XML));
        String cluster = "testCluster";
        Feed newFeed = (Feed) oldFeed.copy();
        Cluster clusterEntity = ConfigurationStore.get().get(EntityType.CLUSTER, cluster);

        Path feedPath = EntityUtil.getNewStagingPath(clusterEntity, oldFeed);
        Assert.assertFalse(UpdateHelper.isEntityUpdated(oldFeed, newFeed, cluster, feedPath));

        //Add tags and ensure isEntityUpdated returns false
        newFeed.setTags("category=test");
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

        //change pipelines and ensure it doesn't cause an update
        oldProcess.setPipelines("test");
        newProcess.setPipelines("newTest");
        newProcess.setTags("category=test");
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

    @Test
    public void testIsEntityLateProcessUpdated() throws Exception {
        String cluster = "testCluster";
        Cluster clusterEntity = ConfigurationStore.get().get(EntityType.CLUSTER, cluster);
        Process oldProcess = processParser.parseAndValidate(this.getClass().getResourceAsStream(PROCESS_XML));
        prepare(oldProcess);
        Path procPath = EntityUtil.getNewStagingPath(clusterEntity, oldProcess);

        // The Process should not be updated when late processing is updated.
        // As the definition does not affect the Oozie workflow.
        Process newProcess = (Process) oldProcess.copy();
        newProcess.getLateProcess().setPolicy(PolicyType.FINAL);
        Assert.assertFalse(UpdateHelper.isEntityUpdated(oldProcess, newProcess, cluster, procPath));

        LateProcess lateProcess = newProcess.getLateProcess();
        newProcess.setLateProcess(null);

        // The Process should be updated when late processing is removed.
        // Pre-processing needs to be removed from the workflow
        Assert.assertTrue(UpdateHelper.isEntityUpdated(oldProcess, newProcess, cluster, procPath));

        Process newerProcess = (Process) newProcess.copy();
        newerProcess.setLateProcess(lateProcess);

        // The Process should be updated when late processing is added.
        // Pre-processing needs to be added to the workflow
        Assert.assertTrue(UpdateHelper.isEntityUpdated(newProcess, newerProcess, cluster, procPath));
    }

    @Test
    public void testIsClusterEntityUpdated() throws Exception {
        Unmarshaller unmarshaller = EntityType.CLUSTER.getUnmarshaller();

        String cluster = "testCluster";
        Cluster clusterEntity = ConfigurationStore.get().get(EntityType.CLUSTER, cluster);
        Cluster newClusterEntity = (Cluster) unmarshaller.unmarshal(this.getClass().getResource(CLUSTER_XML));
        newClusterEntity.setName(cluster);
        Assert.assertNotNull(newClusterEntity);

        // Tags, ACL, description update should not update bundle/workflow for dependent entities
        ACL acl = new ACL();
        acl.setOwner("Test");
        acl.setGroup("testGroup");
        acl.setPermission("*");
        newClusterEntity.setACL(acl);
        newClusterEntity.setDescription("New Description");
        newClusterEntity.setTags("test=val,test2=val2");
        Assert.assertFalse(UpdateHelper.isClusterEntityUpdated(clusterEntity, newClusterEntity));

        // Changing colo should trigger update
        newClusterEntity.setColo("NewColoValue");
        Assert.assertTrue(UpdateHelper.isClusterEntityUpdated(clusterEntity, newClusterEntity));


        // Updating an interface should trigger update bundle/workflow for dependent entities
        Interface writeInterface = ClusterHelper.getInterface(newClusterEntity, Interfacetype.WRITE);
        newClusterEntity.getInterfaces().getInterfaces().remove(writeInterface);
        Assert.assertNotNull(writeInterface);
        writeInterface.setEndpoint("hdfs://test.host.name:8020");
        writeInterface.setType(Interfacetype.WRITE);
        writeInterface.setVersion("2.2.0");
        newClusterEntity.getInterfaces().getInterfaces().add(writeInterface);
        Assert.assertTrue(UpdateHelper.isClusterEntityUpdated(clusterEntity, newClusterEntity));

        // Updating a property should trigger update bundle/workflow for dependent entities
        newClusterEntity = (Cluster) unmarshaller.unmarshal(this.getClass().getResource(CLUSTER_XML));
        newClusterEntity.setName(cluster);
        Assert.assertNotNull(newClusterEntity);
        org.apache.falcon.entity.v0.cluster.Property property = new org.apache.falcon.entity.v0.cluster.Property();
        property.setName("testName");
        property.setValue("testValue");
        newClusterEntity.getProperties().getProperties().add(property);
        Assert.assertTrue(UpdateHelper.isClusterEntityUpdated(clusterEntity, newClusterEntity));

        // Updating a location should trigger update bundle/workflow for dependent entities
        newClusterEntity = (Cluster) unmarshaller.unmarshal(this.getClass().getResource(CLUSTER_XML));
        newClusterEntity.setName(cluster);
        Assert.assertNotNull(newClusterEntity);
        org.apache.falcon.entity.v0.cluster.Location stagingLocation =
                ClusterHelper.getLocation(newClusterEntity, ClusterLocationType.STAGING);
        Assert.assertNotNull(stagingLocation);
        newClusterEntity.getInterfaces().getInterfaces().remove(stagingLocation);
        stagingLocation.setPath("/test/path/here");
        newClusterEntity.getLocations().getLocations().add(stagingLocation);
        Assert.assertTrue(UpdateHelper.isClusterEntityUpdated(clusterEntity, newClusterEntity));
    }

    @Test
    public void testIsDatasourceEntityUpdated() throws Exception {
        Unmarshaller unmarshaller = EntityType.DATASOURCE.getUnmarshaller();

        String datasource = "datasource1";
        Datasource datasourceEntity = ConfigurationStore.get().get(EntityType.DATASOURCE, datasource);
        Datasource newDatasourceEntity = getNewDatasource(unmarshaller, datasource);
        Assert.assertNotNull(newDatasourceEntity);

        // Tags, ACL, description, colo update should not update bundle/workflow for dependent entities
        org.apache.falcon.entity.v0.datasource.ACL acl = new org.apache.falcon.entity.v0.datasource.ACL();
        acl.setOwner("Test");
        acl.setGroup("testGroup");
        acl.setPermission("*");
        newDatasourceEntity.setACL(acl);
        newDatasourceEntity.setDescription("New Description");
        newDatasourceEntity.setTags("test=val,test2=val2");
        newDatasourceEntity.setColo("newColo2");
        Assert.assertFalse(UpdateHelper.isDatasourceEntityUpdated(datasourceEntity, newDatasourceEntity));

        // Changing read or write endpoint should trigger rewrite
        newDatasourceEntity.getInterfaces().getInterfaces().get(0).setEndpoint("jdbc:hsqldb:localhost2/db1");
        Assert.assertTrue(UpdateHelper.isDatasourceEntityUpdated(datasourceEntity, newDatasourceEntity));

        // change credential type or value should trigger
        newDatasourceEntity = getNewDatasource(unmarshaller, datasource);
        Credential cred = newDatasourceEntity.getInterfaces().getInterfaces().get(0).getCredential();
        cred.setPasswordText("blah");
        Assert.assertTrue(UpdateHelper.isDatasourceEntityUpdated(datasourceEntity, newDatasourceEntity));
    }

    private Datasource getNewDatasource(Unmarshaller unmarshaller, String datasource) throws JAXBException {
        Datasource newDatasourceEntity = (Datasource) unmarshaller.unmarshal(this.getClass()
                .getResource(DATASOURCE_XML));
        newDatasourceEntity.setName(datasource);
        return newDatasourceEntity;
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
