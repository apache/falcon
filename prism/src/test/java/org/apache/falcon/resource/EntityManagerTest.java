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
import org.apache.falcon.FalconWebException;
import org.apache.falcon.entity.store.FeedLocationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.entity.v0.process.ACL;
import org.apache.falcon.entity.v0.process.Clusters;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Validity;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.util.StartupProperties;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

import static org.mockito.Mockito.when;

/**
 * Unit testing class for AbstractEntityManager class for testing APIs/methods in it.
 */
public class EntityManagerTest extends AbstractEntityManager {

    @Mock
    private HttpServletRequest mockHttpServletRequest;
    private static final String SAMPLE_PROCESS_XML = "/process-version-0.xml";

    private static final String SAMPLE_INVALID_PROCESS_XML = "/process-invalid.xml";
    private static final long DAY_IN_MILLIS = 86400000L;

    @BeforeTest
    public void init() throws Exception {
        MockitoAnnotations.initMocks(this);
        configStore.unregisterListener(FeedLocationStore.get());
        configStore.registerListener(FeedLocationStore.get());
    }

    @SuppressWarnings("unused")
    @DataProvider(name = "validXMLServletStreamProvider")
    private Object[][] servletStreamProvider() {
        ServletInputStream validProcessXML = getServletInputStream(SAMPLE_PROCESS_XML);
        return new Object[][]{{validProcessXML},
        };

    }

    /**
     * Run this testcase for different types of VALID entity xmls like process, feed, dataEndPoint.
     *
     * @param stream entity stream
     * @throws IOException
     */
    @Test(dataProvider = "validXMLServletStreamProvider")
    public void testValidateForValidEntityXML(ServletInputStream stream) throws IOException {

        when(mockHttpServletRequest.getInputStream()).thenReturn(stream);
    }

    @Test
    public void testValidateForInvalidEntityXML() throws IOException {
        ServletInputStream invalidProcessXML = getServletInputStream(SAMPLE_INVALID_PROCESS_XML);
        when(mockHttpServletRequest.getInputStream()).thenReturn(
                invalidProcessXML);

        try {
            validate(mockHttpServletRequest,
                    EntityType.PROCESS.name());
            Assert.fail("Invalid entity type was accepted by the system");
        } catch (FalconWebException ignore) {
            // ignore
        }
    }

    @Test
    public void testValidateForInvalidEntityType() throws IOException {
        ServletInputStream invalidProcessXML = getServletInputStream(SAMPLE_PROCESS_XML);
        when(mockHttpServletRequest.getInputStream()).thenReturn(
                invalidProcessXML);

        try {
            validate(mockHttpServletRequest,
                    "InvalidEntityType");
            Assert.fail("Invalid entity type was accepted by the system");
        } catch (FalconWebException ignore) {
            // ignore
        }
    }

    @Test
    public void testGetEntityListBadUser() throws Exception {
        CurrentUser.authenticate("fakeUser");
        try {
            Entity process1 = buildProcess("processFakeUser", "fakeUser", "", "");
            configStore.publish(EntityType.PROCESS, process1);
            Assert.fail();
        } catch (Throwable ignore) {
            // do nothing
        }

        /*
         * Only one entity should be returned when the auth is enabled.
         */
        try {
            getEntityList("process", "", "", "", "", "", 0, 10, "");
            Assert.fail();
        } catch (Throwable ignore) {
            // do nothing
        }

        // reset values
        StartupProperties.get().setProperty("falcon.security.authorization.enabled", "false");
        CurrentUser.authenticate(System.getProperty("user.name"));
    }

    @Test
    public void testGetEntityList() throws Exception {

        Entity process2 = buildProcess("processAuthUser", System.getProperty("user.name"), "", "");
        configStore.publish(EntityType.PROCESS, process2);

        EntityList entityList = this.getEntityList("process", "", "", "", "", "asc", 0, 10, "");
        Assert.assertNotNull(entityList.getElements());
        Assert.assertEquals(entityList.getElements().length, 1);

        /*
         * Both entities should be returned when the user is SuperUser.
         */
        StartupProperties.get().setProperty("falcon.security.authorization.enabled", "true");
        CurrentUser.authenticate(System.getProperty("user.name"));
        entityList = this.getEntityList("process", "", "", "", "", "desc", 0, 10, "");
        Assert.assertNotNull(entityList.getElements());
        Assert.assertEquals(entityList.getElements().length, 1);

        // reset values
        StartupProperties.get().setProperty("falcon.security.authorization.enabled", "false");
        CurrentUser.authenticate(System.getProperty("user.name"));
    }


    @Test
    public void testCapOnNumberOfResults() {
        Assert.assertNotEquals(getRequiredNumberOfResults(10000, 0, 10000), 10000);
        Assert.assertEquals(getRequiredNumberOfResults(10000, 0, 10000), MAX_RESULTS);
    }

    @Test
    public void testGetEntityListPagination() throws Exception {
        String user = System.getProperty("user.name");

        Entity process1 = buildProcess("process1", user,
                "consumer=consumer@xyz.com, owner=producer@xyz.com",
                "testPipeline,dataReplicationPipeline");
        configStore.publish(EntityType.PROCESS, process1);

        Entity process2 = buildProcess("process2", user,
                "consumer=consumer@xyz.com, owner=producer@xyz.com",
                "testPipeline,dataReplicationPipeline");
        configStore.publish(EntityType.PROCESS, process2);

        Entity process3 = buildProcess("process3", user, "", "testPipeline");
        configStore.publish(EntityType.PROCESS, process3);

        Entity process4 = buildProcess("process4", user, "owner=producer@xyz.com", "");
        configStore.publish(EntityType.PROCESS, process4);

        EntityList entityList = this.getEntityList("process", "tags", "PIPELINES:dataReplicationPipeline",
                "", "name", "desc", 1, 1, "");
        Assert.assertNotNull(entityList.getElements());
        Assert.assertEquals(entityList.getElements().length, 1);
        Assert.assertEquals(entityList.getElements()[0].name, "process1");
        Assert.assertEquals(entityList.getElements()[0].tag.size(), 2);
        Assert.assertEquals(entityList.getElements()[0].tag.get(0), "consumer=consumer@xyz.com");
        Assert.assertEquals(entityList.getElements()[0].status, null);


        entityList = this.getEntityList("process", "pipelines", "",
                "consumer=consumer@xyz.com, owner=producer@xyz.com", "name", "", 0, 2, "");
        Assert.assertNotNull(entityList.getElements());
        Assert.assertEquals(entityList.getElements().length, 2);
        Assert.assertEquals(entityList.getElements()[1].name, "process2");
        Assert.assertEquals(entityList.getElements()[1].pipeline.size(), 2);
        Assert.assertEquals(entityList.getElements()[1].pipeline.get(0), "testPipeline");
        Assert.assertEquals(entityList.getElements()[0].tag, null);

        entityList = this.getEntityList("process", "pipelines", "",
                "consumer=consumer@xyz.com, owner=producer@xyz.com", "name", "", 10, 2, "");
        Assert.assertEquals(entityList.getElements().length, 0);

        entityList = this.getEntityList("process", "pipelines", "",
                "owner=producer@xyz.com", "name", "", 1, 2, "");
        Assert.assertEquals(entityList.getElements().length, 2);

        // Test negative value for numResults, should throw an exception.
        try {
            this.getEntityList("process", "pipelines", "",
                    "consumer=consumer@xyz.com, owner=producer@xyz.com", "name", "", 10, -1, "");
            Assert.assertTrue(false);
        } catch (Throwable e) {
            Assert.assertTrue(true);
        }

        // Test invalid entry for sortOrder
        try {
            this.getEntityList("process", "pipelines", "",
                    "consumer=consumer@xyz.com, owner=producer@xyz.com", "name", "invalid", 10, 2, "");
            Assert.assertTrue(false);
        } catch (Throwable e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testSearch() throws FalconException {
        Assert.assertTrue(fuzzySearch("My-Hourly-Summary", "mhs"));
        Assert.assertTrue(fuzzySearch("New-My-Hourly-Summary", "MHs"));
        Assert.assertFalse(fuzzySearch("My-Hourly-Summary", "moh"));
    }

    @Test
    public void testGetEntityListWithPattern() throws FalconException {
        String user = System.getProperty("user.name");

        Entity process1 = buildProcess("New-My-Hourly-Summary", user,
                "consumer=consumer@xyz.com, owner=producer@xyz.com",
                "testPipeline,dataReplicationPipeline");
        configStore.publish(EntityType.PROCESS, process1);

        Entity process2 = buildProcess("Random-Summary-Generator", user,
                "consumer=consumer@xyz.com, owner=producer@xyz.com",
                "testPipeline,dataReplicationPipeline");
        configStore.publish(EntityType.PROCESS, process2);

        Entity process3 = buildProcess("My-Hourly-Summary", user, "", "testPipeline");
        configStore.publish(EntityType.PROCESS, process3);

        Entity process4 = buildProcess("sample-process4", user, "owner=producer@xyz.com", "");
        configStore.publish(EntityType.PROCESS, process4);

        EntityList entityList = this.getEntityList("process", "tags", "PIPELINES:dataReplicationPipeline",
                "", "name", "desc", 0, 10, "mhs");
        Assert.assertNotNull(entityList.getElements());
        Assert.assertEquals(entityList.getElements().length, 1);

    }

    @Test
    public void testReverseLookup() throws Exception {
        Feed f = buildFeed("sampleFeed");
        configStore.publish(EntityType.FEED, f);
        Assert.assertNotNull(reverseLookup("feed", "/falcon/test/input/2014/12/10/23"));
    }

    private Location createLocation(LocationType type, String path){
        Location location = new Location();
        location.setPath(path);
        location.setType(type);
        return location;
    }

    private Feed buildFeed(String name) {
        org.apache.falcon.entity.v0.feed.ACL acl = new org.apache.falcon.entity.v0.feed.ACL();
        acl.setOwner("user");
        acl.setGroup("hdfs");
        acl.setPermission("*");

        Feed feed = new Feed();
        feed.setName(name);
        feed.setACL(acl);

        feed.setClusters(createBlankClusters());
        Locations locations = new Locations();
        feed.setLocations(locations);

        feed.getLocations().getLocations().add(createLocation(LocationType.DATA,
                "/falcon/test/input/${YEAR}/${MONTH}/${DAY}/${HOUR}"));
        return feed;
    }

    private org.apache.falcon.entity.v0.feed.Clusters createBlankClusters() {
        org.apache.falcon.entity.v0.feed.Clusters clusters = new org.apache.falcon.entity.v0.feed.Clusters();

        Cluster cluster = new Cluster();
        cluster.setName("blankCluster1");
        clusters.getClusters().add(cluster);

        Cluster cluster2 = new Cluster();
        cluster2.setName("blankCluster2");
        clusters.getClusters().add(cluster2);

        return clusters;
    }

    private Entity buildProcess(String name, String username, String tags, String pipelines) {
        ACL acl = new ACL();
        acl.setOwner(username);
        acl.setGroup("hdfs");
        acl.setPermission("*");

        Process process = new Process();
        process.setName(name);
        process.setACL(acl);
        if (!StringUtils.isEmpty(pipelines)) {
            process.setPipelines(pipelines);
        }
        if (!StringUtils.isEmpty(tags)) {
            process.setTags(tags);
        }
        process.setClusters(buildClusters("cluster" + name));
        return process;
    }

    private Clusters buildClusters(String name) {
        Validity validity = new Validity();
        long startMilliSecs = new Date().getTime() - (2 * DAY_IN_MILLIS);
        validity.setStart(new Date(startMilliSecs));
        validity.setEnd(new Date());
        org.apache.falcon.entity.v0.process.Cluster cluster = new org.apache.falcon.entity.v0.process.Cluster();
        cluster.setName(name);
        cluster.setValidity(validity);

        Clusters clusters =  new Clusters();
        clusters.getClusters().add(cluster);
        return clusters;
    }


    /**
     * Converts a InputStream into ServletInputStream.
     *
     * @param resourceName resource name
     * @return ServletInputStream
     */
    private ServletInputStream getServletInputStream(String resourceName) {
        final InputStream stream = this.getClass().getResourceAsStream(resourceName);
        return new ServletInputStream() {

            @Override
            public int read() throws IOException {
                return stream.read();
            }
        };
    }
}
