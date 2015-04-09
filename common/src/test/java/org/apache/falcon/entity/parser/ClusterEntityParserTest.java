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

package org.apache.falcon.entity.parser;

import org.apache.falcon.FalconException;
import org.apache.falcon.catalog.CatalogServiceFactory;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.AbstractTestBase;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.ClusterLocationType;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.cluster.Location;
import org.apache.falcon.entity.v0.cluster.Locations;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;


/**
 * Test for validating cluster entity parsing.
 */
public class ClusterEntityParserTest extends AbstractTestBase {

    private final ClusterEntityParser parser = (ClusterEntityParser) EntityParserFactory.getParser(EntityType.CLUSTER);

    @Test
    public void testParse() throws IOException, FalconException, JAXBException {

        InputStream stream = this.getClass().getResourceAsStream(CLUSTER_XML);

        Cluster cluster = parser.parse(stream);
        ClusterHelper.getInterface(cluster, Interfacetype.WRITE)
                .setEndpoint(conf.get(HadoopClientFactory.FS_DEFAULT_NAME_KEY));

        Assert.assertNotNull(cluster);
        Assert.assertEquals(cluster.getName(), "testCluster");

        Interface execute = ClusterHelper.getInterface(cluster, Interfacetype.EXECUTE);

        Assert.assertEquals(execute.getEndpoint(), "localhost:8021");
        Assert.assertEquals(execute.getVersion(), "0.20.2");

        Interface readonly = ClusterHelper.getInterface(cluster, Interfacetype.READONLY);
        Assert.assertEquals(readonly.getEndpoint(), "hftp://localhost:50010");
        Assert.assertEquals(readonly.getVersion(), "0.20.2");

        Interface write = ClusterHelper.getInterface(cluster, Interfacetype.WRITE);
        //assertEquals(write.getEndpoint(), conf.get("fs.defaultFS"));
        Assert.assertEquals(write.getVersion(), "0.20.2");

        Interface workflow = ClusterHelper.getInterface(cluster, Interfacetype.WORKFLOW);
        Assert.assertEquals(workflow.getEndpoint(), "http://localhost:11000/oozie/");
        Assert.assertEquals(workflow.getVersion(), "4.0");

        Assert.assertEquals(ClusterHelper.getLocation(cluster, ClusterLocationType.STAGING).getPath(),
                "/projects/falcon/staging");

        StringWriter stringWriter = new StringWriter();
        Marshaller marshaller = EntityType.CLUSTER.getMarshaller();
        marshaller.marshal(cluster, stringWriter);
        System.out.println(stringWriter.toString());

        Interface catalog = ClusterHelper.getInterface(cluster, Interfacetype.REGISTRY);
        Assert.assertEquals(catalog.getEndpoint(), "http://localhost:48080/templeton/v1");
        Assert.assertEquals(catalog.getVersion(), "0.11.0");

        Assert.assertEquals(ClusterHelper.getLocation(cluster, ClusterLocationType.STAGING).getPath(),
                "/projects/falcon/staging");
    }

    @Test
    public void testParseClusterWithoutRegistry() throws IOException, FalconException, JAXBException {

        StartupProperties.get().setProperty(CatalogServiceFactory.CATALOG_SERVICE, "thrift://localhost:9083");
        Assert.assertTrue(CatalogServiceFactory.isEnabled());

        InputStream stream = this.getClass().getResourceAsStream("/config/cluster/cluster-no-registry.xml");
        Cluster cluster = parser.parse(stream);

        Interface catalog = ClusterHelper.getInterface(cluster, Interfacetype.REGISTRY);
        Assert.assertNull(catalog);

        StartupProperties.get().remove(CatalogServiceFactory.CATALOG_SERVICE);
        Assert.assertFalse(CatalogServiceFactory.isEnabled());

        catalog = ClusterHelper.getInterface(cluster, Interfacetype.REGISTRY);
        Assert.assertNull(catalog);
    }

    @Test
    public void testParseClusterWithBadRegistry() throws Exception {
        // disable catalog service
        StartupProperties.get().remove(CatalogServiceFactory.CATALOG_SERVICE);
        Assert.assertFalse(CatalogServiceFactory.isEnabled());

        InputStream stream = this.getClass().getResourceAsStream("/config/cluster/cluster-bad-registry.xml");
        Cluster cluster = parser.parse(stream);

        Interface catalog = ClusterHelper.getInterface(cluster, Interfacetype.REGISTRY);
        Assert.assertEquals(catalog.getEndpoint(), "Hcat");
        Assert.assertEquals(catalog.getVersion(), "0.1");
    }

    /**
     * A positive test for validating tags key value pair regex: key=value, key=value.
     * @throws FalconException
     */
    @Test
    public void testClusterTags() throws FalconException {
        InputStream stream = this.getClass().getResourceAsStream(CLUSTER_XML);
        Cluster cluster = parser.parse(stream);

        final String tags = cluster.getTags();
        Assert.assertEquals("consumer=consumer@xyz.com, owner=producer@xyz.com, department=forecasting", tags);

        final String[] keys = {"consumer", "owner", "department", };
        final String[] values = {"consumer@xyz.com", "producer@xyz.com", "forecasting", };

        final String[] pairs = tags.split(",");
        Assert.assertEquals(3, pairs.length);
        for (int i = 0; i < pairs.length; i++) {
            String pair = pairs[i].trim();
            String[] parts = pair.split("=");
            Assert.assertEquals(keys[i], parts[0]);
            Assert.assertEquals(values[i], parts[1]);
        }
    }

    @Test
    public void testValidateACLWithNoACLAndAuthorizationEnabled() throws Exception {
        StartupProperties.get().setProperty("falcon.security.authorization.enabled", "true");
        Assert.assertTrue(Boolean.valueOf(
                StartupProperties.get().getProperty("falcon.security.authorization.enabled")));

        try {
            InputStream stream = this.getClass().getResourceAsStream(CLUSTER_XML);

            // need a new parser since it caches authorization enabled flag
            ClusterEntityParser clusterEntityParser =
                    (ClusterEntityParser) EntityParserFactory.getParser(EntityType.CLUSTER);
            Cluster cluster = clusterEntityParser.parse(stream);
            Assert.assertNotNull(cluster);
            Assert.assertNull(cluster.getACL());
        } finally {
            StartupProperties.get().setProperty("falcon.security.authorization.enabled", "false");
        }
    }

    @Test
    public void testValidateACLAuthorizationEnabled() throws Exception {
        StartupProperties.get().setProperty("falcon.security.authorization.enabled", "true");
        Assert.assertTrue(Boolean.valueOf(
                StartupProperties.get().getProperty("falcon.security.authorization.enabled")));

        try {
            InputStream stream = this.getClass().getResourceAsStream("/config/cluster/cluster-no-registry.xml");

            // need a new parser since it caches authorization enabled flag
            ClusterEntityParser clusterEntityParser =
                    (ClusterEntityParser) EntityParserFactory.getParser(EntityType.CLUSTER);
            Cluster cluster = clusterEntityParser.parse(stream);
            Assert.assertNotNull(cluster);
            Assert.assertNotNull(cluster.getACL());
            Assert.assertNotNull(cluster.getACL().getOwner());
            Assert.assertNotNull(cluster.getACL().getGroup());
        } finally {
            StartupProperties.get().setProperty("falcon.security.authorization.enabled", "false");
        }
    }

    /**
     * A lightweight unit test for a cluster where location type staging is missing.
     * Extensive tests are found in ClusterEntityValidationIT.
     *
     * @throws ValidationException
     */
    @Test(expectedExceptions = ValidationException.class) public void testClusterWithoutStaging() throws Exception {
        ClusterEntityParser clusterEntityParser = Mockito
                .spy((ClusterEntityParser) EntityParserFactory.getParser(EntityType.CLUSTER));
        Cluster cluster = (Cluster) this.dfsCluster.getCluster().copy();
        Mockito.doNothing().when(clusterEntityParser).validateWorkflowInterface(cluster);
        Mockito.doNothing().when(clusterEntityParser).validateMessagingInterface(cluster);
        Mockito.doNothing().when(clusterEntityParser).validateRegistryInterface(cluster);
        Location location = new Location();
        location.setName(ClusterLocationType.WORKING);
        location.setPath("/apps/non/existent/path");
        Locations locations = new Locations();
        locations.getLocations().add(location);
        cluster.setLocations(locations);
        clusterEntityParser.validate(cluster);
        Assert.fail("Should have thrown a validation exception");
    }

    /**
     * A lightweight unit test for a cluster where location paths are invalid.
     * Extensive tests are found in ClusterEntityValidationIT.
     *
     * @throws ValidationException
     */
    @Test(expectedExceptions = ValidationException.class)
    public void testClusterWithInvalidLocationsPaths() throws Exception {
        ClusterEntityParser clusterEntityParser = Mockito
                .spy((ClusterEntityParser) EntityParserFactory.getParser(EntityType.CLUSTER));
        Cluster cluster = (Cluster)this.dfsCluster.getCluster().copy();
        Location location = new Location();
        location.setName(ClusterLocationType.STAGING);
        location.setPath("/apps/non/existent/path");
        Locations locations = new Locations();
        locations.getLocations().add(location);
        cluster.setLocations(locations);
        Mockito.doNothing().when(clusterEntityParser).validateWorkflowInterface(cluster);
        Mockito.doNothing().when(clusterEntityParser).validateMessagingInterface(cluster);
        Mockito.doNothing().when(clusterEntityParser).validateRegistryInterface(cluster);
        try {
            clusterEntityParser.validate(cluster);
        } catch (ValidationException e) {
            String errorMessage =
                    "Location " + location.getPath() + " for cluster " + cluster.getName() + " must exist.";
            Assert.assertEquals(e.getMessage(), errorMessage);
            throw e;
        }
        Assert.fail("Should have thrown a validation exception");
    }

    /**
     * A lightweight unit test for a cluster where location paths are same.
     * Extensive tests are found in ClusterEntityValidationIT.
     *
     * @throws ValidationException
     */
    @Test(expectedExceptions = ValidationException.class)
    public void testClusterWithSameWorkingAndStaging() throws Exception {
        ClusterEntityParser clusterEntityParser = Mockito
                .spy((ClusterEntityParser) EntityParserFactory.getParser(EntityType.CLUSTER));
        Cluster cluster = (Cluster) this.dfsCluster.getCluster().copy();
        cluster.getLocations().getLocations().get(1).setPath("/projects/falcon/staging");
        Mockito.doNothing().when(clusterEntityParser).validateWorkflowInterface(cluster);
        Mockito.doNothing().when(clusterEntityParser).validateMessagingInterface(cluster);
        Mockito.doNothing().when(clusterEntityParser).validateRegistryInterface(cluster);
        clusterEntityParser.validate(cluster);
        Assert.fail("Should have thrown a validation exception");
    }

    /**
     * A lightweight unit test for a cluster where location type working is missing.
     * It should automatically get generated
     * Extensive tests are found in ClusterEntityValidationIT.
     */
    @Test public void testClusterWithOnlyStaging() throws Exception {
        ClusterEntityParser clusterEntityParser = Mockito
                .spy((ClusterEntityParser) EntityParserFactory.getParser(EntityType.CLUSTER));
        Cluster cluster = (Cluster) this.dfsCluster.getCluster().copy();
        cluster.getLocations().getLocations().remove(1);
        Mockito.doNothing().when(clusterEntityParser).validateWorkflowInterface(cluster);
        Mockito.doNothing().when(clusterEntityParser).validateMessagingInterface(cluster);
        Mockito.doNothing().when(clusterEntityParser).validateRegistryInterface(cluster);
        this.dfsCluster.getFileSystem().mkdirs(new Path(ClusterHelper.getLocation(cluster,
                        ClusterLocationType.STAGING).getPath()), HadoopClientFactory.READ_EXECUTE_PERMISSION);
        clusterEntityParser.validate(cluster);
        String workingDirPath = cluster.getLocations().getLocations().get(0).getPath() + "/working";
        Assert.assertEquals(ClusterHelper.getLocation(cluster, ClusterLocationType.WORKING).getPath(), workingDirPath);
        FileStatus workingDirStatus = this.dfsCluster.getFileSystem().getFileLinkStatus(new Path(workingDirPath));
        Assert.assertTrue(workingDirStatus.isDirectory());
        Assert.assertEquals(workingDirStatus.getPermission(), HadoopClientFactory.READ_EXECUTE_PERMISSION);
    }

    /**
     * A lightweight unit test for a cluster where location working is not there and staging
     * has a subdir which will be used by cluster as working.
     * Checking for wrong perms of this subdir
     * Extensive tests are found in ClusterEntityValidationIT.
     *
     * @throws ValidationException
     */
    @Test(expectedExceptions = ValidationException.class)
    public void testClusterWithSubdirInStaging() throws Exception {
        ClusterEntityParser clusterEntityParser = Mockito
                .spy((ClusterEntityParser) EntityParserFactory.getParser(EntityType.CLUSTER));
        Cluster cluster = (Cluster) this.dfsCluster.getCluster().copy();
        cluster.getLocations().getLocations().get(1).setPath("/projects/falcon/staging");
        cluster.getLocations().getLocations().remove(1);
        HadoopClientFactory.mkdirs(this.dfsCluster.getFileSystem(),
                new Path(ClusterHelper.getLocation(cluster, ClusterLocationType.WORKING).getPath()),
                HadoopClientFactory.ALL_PERMISSION);
        Mockito.doNothing().when(clusterEntityParser).validateWorkflowInterface(cluster);
        Mockito.doNothing().when(clusterEntityParser).validateMessagingInterface(cluster);
        Mockito.doNothing().when(clusterEntityParser).validateRegistryInterface(cluster);
        clusterEntityParser.validate(cluster);
        Assert.fail("Should have thrown a validation exception");
    }

    @BeforeClass
    public void init() throws Exception {
        this.dfsCluster = EmbeddedCluster.newCluster("testCluster");
        this.conf = dfsCluster.getConf();
    }

    @AfterClass
    public void tearDown() {
        this.dfsCluster.shutdown();
    }
}
