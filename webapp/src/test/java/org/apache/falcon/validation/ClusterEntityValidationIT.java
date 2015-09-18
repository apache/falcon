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

package org.apache.falcon.validation;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.parser.ClusterEntityParser;
import org.apache.falcon.entity.parser.EntityParserFactory;
import org.apache.falcon.entity.parser.ValidationException;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.ACL;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.ClusterLocationType;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.resource.TestContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Tests cluster entity validation to verify if each of the specified
 * interface endpoints are valid.
 */
public class ClusterEntityValidationIT {
    private static final FsPermission OWNER_ONLY_PERMISSION =
            new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);

    private final TestContext context = new TestContext();
    private Map<String, String> overlay;

    private final ClusterEntityParser parser =
            (ClusterEntityParser) EntityParserFactory.getParser(EntityType.CLUSTER);
    private Cluster cluster;
    private FileSystem fs;


    @BeforeClass
    public void setup() throws Exception {
        TestContext.prepare();

        overlay = context.getUniqueOverlay();
        String filePath = TestContext.overlayParametersOverTemplate(
                TestContext.CLUSTER_TEMPLATE, overlay);
        context.setCluster(filePath);
        InputStream stream = new FileInputStream(filePath);
        cluster = (Cluster) EntityType.CLUSTER.getUnmarshaller().unmarshal(stream);
        Assert.assertNotNull(cluster);

        fs = FileSystem.get(ClusterHelper.getConfiguration(cluster));
    }

    @AfterClass
    public void tearDown() throws Exception {
        TestContext.deleteEntitiesFromStore();
    }

    /**
     * Positive test.
     *
     * @throws Exception
     */
    @Test
    public void testClusterEntityWithValidInterfaces() throws Exception {
        overlay = context.getUniqueOverlay();
        overlay.put("colo", "default");
        ClientResponse response = context.submitToFalcon(TestContext.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);
    }


    @DataProvider(name = "interfaceToInvalidURLs")
    public Object[][] createInterfaceToInvalidURLData() {
        return new Object[][] {
            // TODO FileSystem validates invalid hftp url, does NOT fail
            // {Interfacetype.READONLY, "hftp://localhost:41119"},
            {Interfacetype.READONLY, ""},
            {Interfacetype.READONLY, "localhost:41119"},
            {Interfacetype.WRITE, "write-interface:9999"},
            {Interfacetype.WRITE, "hdfs://write-interface:9999"},
            {Interfacetype.EXECUTE, "execute-interface:9999"},
            {Interfacetype.WORKFLOW, "workflow-interface:9999/oozie/"},
            {Interfacetype.WORKFLOW, "http://workflow-interface:9999/oozie/"},
            {Interfacetype.MESSAGING, "messaging-interface:9999"},
            {Interfacetype.MESSAGING, "tcp://messaging-interface:9999"},
            {Interfacetype.REGISTRY, "catalog-interface:9999"},
            {Interfacetype.REGISTRY, "http://catalog-interface:9999"},
        };
    }

    @Test (dataProvider = "interfaceToInvalidURLs")
    public void testClusterEntityWithInvalidInterfaces(Interfacetype interfacetype, String endpoint)
        throws Exception {
        overlay = context.getUniqueOverlay();
        String filePath = TestContext.overlayParametersOverTemplate(TestContext.CLUSTER_TEMPLATE, overlay);
        InputStream stream = new FileInputStream(filePath);
        Cluster clusterEntity = (Cluster) EntityType.CLUSTER.getUnmarshaller().unmarshal(stream);
        Assert.assertNotNull(clusterEntity);
        clusterEntity.setColo("default");  // validations will be ignored if not default & tests fail

        Interface anInterface = ClusterHelper.getInterface(clusterEntity, interfacetype);
        anInterface.setEndpoint(endpoint);

        File tmpFile = TestContext.getTempFile();
        EntityType.CLUSTER.getMarshaller().marshal(clusterEntity, tmpFile);
        System.out.println("Starting Interface type " + interfacetype + "Endpoint " + endpoint);
        ClientResponse response = context.submitFileToFalcon(EntityType.CLUSTER, tmpFile.getAbsolutePath());
        context.assertFailure(response);
        System.out.println("Completed Interface type " + interfacetype + "Endpoint " + endpoint);
    }

    @Test
    public void testValidateACL() throws Exception {
        overlay = context.getUniqueOverlay();
        String filePath = TestContext.overlayParametersOverTemplate(TestContext.CLUSTER_TEMPLATE, overlay);
        InputStream stream = new FileInputStream(filePath);
        Cluster clusterEntity = (Cluster) EntityType.CLUSTER.getUnmarshaller().unmarshal(stream);
        Assert.assertNotNull(clusterEntity);

        // Adding ACL with authorization disabled must not hurt
        ACL clusterACL = new ACL();
        clusterACL.setOwner(TestContext.REMOTE_USER);
        clusterACL.setGroup(TestContext.REMOTE_USER);
        clusterEntity.setACL(clusterACL);

        clusterEntity.setColo("default");  // validations will be ignored if not default & tests fail

        File tmpFile = TestContext.getTempFile();
        EntityType.CLUSTER.getMarshaller().marshal(clusterEntity, tmpFile);
        ClientResponse response = context.submitFileToFalcon(EntityType.CLUSTER, tmpFile.getAbsolutePath());
        context.assertSuccessful(response);
    }

    @Test
    public void testValidateClusterLocations() throws Exception {
        TestContext.createClusterLocations(cluster, fs);
        parser.validate(cluster);
    }

    @Test
    public void testValidateClusterLocationsWithoutWorking() throws Exception {
        overlay = context.getUniqueOverlay();
        String filePath = TestContext.overlayParametersOverTemplate(TestContext.CLUSTER_TEMPLATE, overlay);
        InputStream stream = new FileInputStream(filePath);
        Cluster clusterEntity = (Cluster) EntityType.CLUSTER.getUnmarshaller().unmarshal(stream);
        clusterEntity.getLocations().getLocations().remove(2);
        FileSystem clusterFileSystem = FileSystem.get(ClusterHelper.getConfiguration(cluster));
        TestContext.createClusterLocations(clusterEntity, clusterFileSystem, false);
        parser.validate(clusterEntity);
        String expectedPath =
                ClusterHelper.getLocation(clusterEntity, ClusterLocationType.STAGING).getPath() + "/working";
        Assert.assertEquals(ClusterHelper.getLocation(clusterEntity, ClusterLocationType.WORKING).getPath(),
                expectedPath);
        Assert.assertTrue(clusterFileSystem.getFileLinkStatus(new Path(expectedPath)).isDirectory());
        Assert.assertEquals(clusterFileSystem.getFileLinkStatus(new Path(expectedPath)).getPermission(),
                HadoopClientFactory.READ_EXECUTE_PERMISSION);
    }

    @Test(expectedExceptions = ValidationException.class)
    public void testValidateClusterLocationsThatDontExist() throws Exception {
        TestContext.deleteClusterLocations(cluster, fs);
        parser.validate(cluster);
        Assert.fail("Should have thrown a validation exception");
    }

    @Test(expectedExceptions = ValidationException.class)
    public void testValidateClusterLocationsThatExistWithBadOwner() throws Exception {
        createClusterLocationsBadPermissions(cluster);
        parser.validate(cluster);
        Assert.fail("Should have thrown a validation exception");
    }

    private void createClusterLocationsBadPermissions(Cluster clusterEntity) throws IOException {
        FileSystem clusterFileSystem = FileSystem.get(ClusterHelper.getConfiguration(clusterEntity));
        TestContext.deleteClusterLocations(clusterEntity, clusterFileSystem);
        String stagingLocation = ClusterHelper.getLocation(clusterEntity, ClusterLocationType.STAGING).getPath();
        Path stagingPath = new Path(stagingLocation);
        FileSystem.mkdirs(clusterFileSystem, stagingPath, OWNER_ONLY_PERMISSION);

        String workingLocation = ClusterHelper.getLocation(clusterEntity, ClusterLocationType.WORKING).getPath();
        Path workingPath = new Path(workingLocation);
        FileSystem.mkdirs(clusterFileSystem, stagingPath, OWNER_ONLY_PERMISSION);
    }
}
