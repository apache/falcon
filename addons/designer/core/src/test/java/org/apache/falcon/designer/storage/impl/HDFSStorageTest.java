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
package org.apache.falcon.designer.storage.impl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.falcon.designer.storage.StorageException;
import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Will test HDFSStorage for create open remove.
 */
public class HDFSStorageTest {
    private HDFSStorage hdfsStorageInst;

    @BeforeClass
    public void setUpDFS() throws Exception {
        Configuration conf = new Configuration();
        conf.set("falcon.designer.hdfsstorage.defaultpath", "/tmp/");
        conf.set("fs.default.name", "file:///");
        hdfsStorageInst = new HDFSStorage(conf);

    }

    @Test
    public void testCreateOpenDelete() {
        try {
            final String testNameSpace = "testNS";
            final String testEntity = "testEntity";
            OutputStream opStream =
                    hdfsStorageInst.create(testNameSpace, testEntity);
            String testMessage = "testing HDFSStorage";
            byte[] outputByte = new byte[testMessage.length()];
            opStream.write(testMessage.getBytes());
            opStream.close();
            InputStream ipStream =
                    hdfsStorageInst.open(testNameSpace, testEntity);
            ipStream.read(outputByte, 0, testMessage.length());
            ipStream.close();
            hdfsStorageInst.delete(testNameSpace, testEntity);
            try {
                hdfsStorageInst.open(testNameSpace, testEntity);
                Assert
                    .fail("file should be present and should have thrown an exception");
            } catch (StorageException ex) {
                Assert.assertEquals(ex.getCause().getClass(),
                        FileNotFoundException.class);
            }
            Assert.assertEquals(new String(outputByte), testMessage);
        } catch (StorageException ex) {
            Assert.fail(ex.getMessage());
        } catch (IOException ex) {
            Assert.fail(ex.getMessage());
        }

    }
}
