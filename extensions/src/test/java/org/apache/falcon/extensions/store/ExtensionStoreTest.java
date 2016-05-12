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
import org.apache.falcon.entity.store.StoreAccessException;
import org.apache.falcon.extensions.mirroring.hdfs.HdfsMirroringExtension;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;

/**
 *  Tests for extension store.
 */
public class ExtensionStoreTest extends AbstractTestExtensionStore {
    private static Map<String, String> resourcesMap;

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

}

