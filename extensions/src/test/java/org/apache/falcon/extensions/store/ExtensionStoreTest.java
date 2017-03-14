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

package org.apache.falcon.extensions.store;

import com.google.common.collect.ImmutableMap;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.parser.ValidationException;
import org.apache.falcon.entity.store.StoreAccessException;
import org.apache.falcon.extensions.ExtensionStatus;
import org.apache.falcon.extensions.jdbc.ExtensionMetaStore;
import org.apache.falcon.extensions.mirroring.hdfs.HdfsMirroringExtension;
import org.apache.falcon.hadoop.JailedFileSystem;
import org.apache.falcon.service.FalconJPAService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.io.OutputStreamWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * Tests for extension store.
 */
public class ExtensionStoreTest extends AbstractTestExtensionStore {
    private static Map<String, String> resourcesMap;
    private static JailedFileSystem fs;
    protected static final String EXTENSION_PATH = "/projects/falcon/extension/";
    private static final String STORAGE_URL = "jail://global:00";

    @BeforeClass
    public void init() throws Exception {
        initExtensionStore();
        resourcesMap = ImmutableMap.of(
                "hdfs-mirroring-template.xml", extensionStorePath
                        + "/hdfs-mirroring/resources/runtime/hdfs-mirroring-template.xml",
                "hdfs-mirroring-workflow.xml", extensionStorePath
                        + "/hdfs-mirroring/resources/runtime/hdfs-mirroring-workflow.xml",
                "hdfs-snapshot-mirroring-template.xml", extensionStorePath
                        + "/hdfs-mirroring/resources/runtime/hdfs-snapshot-mirroring-template.xml",
                "hdfs-snapshot-mirroring-workflow.xml", extensionStorePath
                        + "/hdfs-mirroring/resources/runtime/hdfs-snapshot-mirroring-workflow.xml"
        );
        fs = new JailedFileSystem();
        initFileSystem();
    }

    @Test
    public void testGetExtensionResources() throws StoreAccessException {
        String extensionName = new HdfsMirroringExtension().getName();
        Map<String, String> resources = store.getExtensionResources(extensionName);

        for (Map.Entry<String, String> entry : resources.entrySet()) {
            String path = resourcesMap.get(entry.getKey());
            Assert.assertEquals(entry.getValue(), path);
        }
    }

    @Test
    public void testGetExtensionLibPath() throws StoreAccessException {
        String extensionName = new HdfsMirroringExtension().getName();
        String libPath = extensionStorePath + "/hdfs-mirroring/libs";
        Assert.assertEquals(store.getExtensionLibPath(extensionName), libPath);
    }

    private static void initFileSystem() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", STORAGE_URL);
        fs.initialize(LocalFileSystem.getDefaultUri(conf), conf);
    }

    @BeforeMethod
    public void clean() {
        clearDB();
        // Clean up FS
        try {
            fs.delete(new Path(extensionStorePath + "/hdfs-mirroring"), true);
        } catch (IOException e) {
            // Ignore
        }
    }

    @Test(expectedExceptions=ValidationException.class)
    public void testFailureCaseRegisterExtensionForURL() throws IOException, URISyntaxException, FalconException{
        store = ExtensionStore.get();
        createLibs(EXTENSION_PATH);
        store.registerExtension("test", EXTENSION_PATH, "test desc", "falconUser");
    }

    @Test
    public void testRegisterExtension() throws IOException, URISyntaxException, FalconException {
        String extensionPath = EXTENSION_PATH + "testRegister";
        createLibs(extensionPath);
        createReadmeAndJar(extensionPath);
        createMETA(extensionPath);
        store = ExtensionStore.get();
        store.registerExtension("test", STORAGE_URL + extensionPath, "test desc", "falconUser");
        ExtensionMetaStore metaStore = new ExtensionMetaStore();
        Assert.assertEquals(metaStore.getAllExtensions().size(), 1);
    }

    @Test(expectedExceptions = ValidationException.class)
    public void testFailureCaseRegisterExtension() throws IOException, URISyntaxException, FalconException {
        String extensionPath = EXTENSION_PATH + "testRegister";
        store = ExtensionStore.get();
        createLibs(extensionPath);
        store.registerExtension("test", STORAGE_URL + EXTENSION_PATH, "test desc", "falconUser");
    }

    @Test
    public void testDeleteExtension() throws IOException, URISyntaxException, FalconException {
        String extensionPath = EXTENSION_PATH + "testDelete";
        createLibs(extensionPath);
        createReadmeAndJar(extensionPath);
        createMETA(extensionPath);
        store = ExtensionStore.get();
        store.registerExtension("toBeDeleted", STORAGE_URL + extensionPath, "test desc", "falconUser");
        Assert.assertTrue(store.getResource(STORAGE_URL + extensionPath + "/README").equals("README"));
        store.deleteExtension("toBeDeleted", "falconUser");
        ExtensionMetaStore metaStore = new ExtensionMetaStore();
        Assert.assertEquals(metaStore.getAllExtensions().size(), 0);
    }

    @Test(expectedExceptions = FalconException.class)
    public void testFailureDeleteExtension() throws IOException, URISyntaxException, FalconException {
        String extensionPath = EXTENSION_PATH + "testACLOnDelete";
        createLibs(extensionPath);
        createReadmeAndJar(extensionPath);
        createMETA(extensionPath);
        store = ExtensionStore.get();
        store.registerExtension("ACLFailure", STORAGE_URL + extensionPath, "test desc", "oozieUser");
        store.deleteExtension("ACLFailure", "falconUser");
    }

    @Test(expectedExceptions = FalconException.class)
    public void testStatusChangeExtensionACLFailure() throws IOException, URISyntaxException, FalconException {
        String extensionPath = EXTENSION_PATH + "testStatusChangeACLFailure";
        createLibs(extensionPath);
        createReadmeAndJar(extensionPath);
        createMETA(extensionPath);
        store = ExtensionStore.get();
        store.registerExtension("testStatusChangeACLFailure", STORAGE_URL + extensionPath, "test desc", "falconUser");
        store.updateExtensionStatus("testStatusChangeACLFailure", "oozieUser", ExtensionStatus.DISABLED);
    }

    @Test(expectedExceptions = ValidationException.class)
    public void testStatusChangeExtensionValidationFailure() throws IOException, URISyntaxException, FalconException {
        String extensionPath = EXTENSION_PATH + "testStatusChangeValidationFailure";
        createLibs(extensionPath);
        createReadmeAndJar(extensionPath);
        createMETA(extensionPath);
        store = ExtensionStore.get();
        store.registerExtension("testStatusChangeValidationFailure", STORAGE_URL + extensionPath, "test desc",
                "falconUser");
        store.updateExtensionStatus("testStatusChangeValidationFailure", "falconUser", ExtensionStatus.ENABLED);
    }

    @Test()
    public void testStatusChangeExtension() throws IOException, URISyntaxException, FalconException {
        String extensionPath = EXTENSION_PATH + "testStatusChange";
        createLibs(extensionPath);
        createReadmeAndJar(extensionPath);
        createMETA(extensionPath);
        store = ExtensionStore.get();
        store.registerExtension("testStatusChange", STORAGE_URL + extensionPath, "test desc", "falconUser");
        store.updateExtensionStatus("testStatusChange", "falconUser", ExtensionStatus.DISABLED);
        ExtensionMetaStore metaStore = new ExtensionMetaStore();
        Assert.assertEquals(metaStore.getDetail("testStatusChange").getStatus(), ExtensionStatus.DISABLED);
    }

    private void createMETA(String extensionPath) throws IOException {
        Path path = new Path(extensionPath + "/META");
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        fs.mkdirs(path);
        path = new Path(extensionPath + "/META/test.properties");
        OutputStream os = fs.create(path);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
        br.write("Hello World");
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        br.write("test properties");
        fs.create(path);
        br.close();
    }

    private void createLibs(String extensionPath) throws IOException {
        Path path = new Path(extensionPath);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        fs.mkdirs(path);
        path = new Path(extensionPath + "/libs//libs/build");
        fs.mkdirs(path);
    }

    private void createReadmeAndJar(String extensionPath) throws IOException {
        Path path = new Path(extensionPath + "/README");
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        OutputStream os = fs.create(path);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
        br.write("README");
        fs.create(path);
        br.close();
        os.close();
        path = new Path(extensionPath + "/libs/build/test.jar");
        os = fs.create(path);
        br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
        br.write("Hello World");
        br.write("test jar");
        fs.create(path);
        br.close();
        os.close();
    }

    private void clearDB() {
        EntityManager em = FalconJPAService.get().getEntityManager();
        em.getTransaction().begin();
        try {
            Query query = em.createNativeQuery("delete from EXTENSIONS");
            query.executeUpdate();
        } finally {
            em.getTransaction().commit();
            em.close();
        }
    }
}

