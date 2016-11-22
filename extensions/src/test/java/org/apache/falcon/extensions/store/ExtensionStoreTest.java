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
import org.apache.falcon.entity.store.StoreAccessException;
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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.Map;

/**
 *  Tests for extension store.
 */
public class ExtensionStoreTest extends AbstractTestExtensionStore {
    private static Map<String, String> resourcesMap;
    private static JailedFileSystem fs;
    protected static final String EXTENSION_PATH = "/projects/falcon/extension";
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
        clear();
    }


    @Test(enabled = false)
    public void testRegisterExtensionMetadata() throws IOException, URISyntaxException, FalconException{
        createlibs();
        createReadmeAndJar();
        createMETA();
        store = ExtensionStore.get();
        store.registerExtensionMetadata("test", STORAGE_URL + EXTENSION_PATH, "test desc");
        ExtensionMetaStore metaStore = new ExtensionMetaStore();
        Assert.assertEquals(metaStore.getAllExtensions().size(), 1);
    }

    @Test(expectedExceptions=FileNotFoundException.class)
    public void testFailureCaseRegisterExtensionMetadata() throws IOException, URISyntaxException, FalconException{
        store = ExtensionStore.get();
        createlibs();
        store.registerExtensionMetadata("test", STORAGE_URL + EXTENSION_PATH, "test desc");
    }

    private void createMETA() throws IOException{
        Path path = new Path(EXTENSION_PATH + "/META");
        if (fs.exists(path)){
            fs.delete(path, true);
        }
        fs.mkdirs(path);
        path = new Path(EXTENSION_PATH + "/META/test.properties");
        OutputStream os = fs.create(path);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
        br.write("Hello World");
        if (fs.exists(path)){
            fs.delete(path, true);
        }
        br.write("test properties");
        fs.create(path);
        br.close();
    }

    private void createlibs() throws IOException{
        Path path = new Path(EXTENSION_PATH);
        if (fs.exists(path)){
            fs.delete(path, true);
        }
        fs.mkdirs(path);
        path = new Path(EXTENSION_PATH + "/libs//libs/build");
        fs.mkdirs(path);
    }

    private void createReadmeAndJar() throws IOException{
        Path path = new Path(EXTENSION_PATH + "/README");
        if (fs.exists(path)){
            fs.delete(path, true);
        }
        fs.create(path);
        path = new Path(EXTENSION_PATH + "/libs/build/test.jar");
        OutputStream os = fs.create(path);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
        br.write("Hello World");
        br.write("test jar");
        fs.create(path);
        br.close();
    }

    private void clear() {
        EntityManager em = FalconJPAService.get().getEntityManager();
        em.getTransaction().begin();
        try {
            Query query = em.createNativeQuery("delete from EXTENSION_METADATA");
            query.executeUpdate();
        } finally {
            em.getTransaction().commit();
            em.close();
        }
    }

}

