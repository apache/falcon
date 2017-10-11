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

package org.apache.falcon;

import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Test Class for validating Extension util helper methods.
 */
public class ExtensionHandlerTest {
    private static final String PROCESS_XML = "/extension-example-duplicate.xml";
    private static final String JARS_DIR = "file:///" + System.getProperty("user.dir") + "/src/test/resources";
    private static final String CONFIG_PATH = "file:///" + System.getProperty("user.dir")
            + "/src/test/resources/extension.properties";

    @Test
    public void testPrepareAndSetEntityTags() throws Exception {
        Entity process = (Entity) EntityType.PROCESS.getUnmarshaller().unmarshal(
                getClass().getResourceAsStream(PROCESS_XML));
        EntityUtil.setEntityTags(process, "testTag");
        Assert.assertTrue(EntityUtil.getTags(process).contains("testTag"));

        List<URL> urls = new ArrayList<>();

        InputStream configStream = null;
        try {
            configStream = new FileInputStream(CONFIG_PATH);
        } catch (FileNotFoundException e) {
            //ignore
        }

        urls.addAll(ExtensionHandler.getFilesInPath(new Path(JARS_DIR).toUri().toURL()));
        List<Entity> entities = ExtensionHandler.prepare("extensionName", "jobName", configStream, urls);
        Assert.assertEquals(entities.size(), 1);
        Assert.assertEquals(entities.get(0), process);
    }
}

