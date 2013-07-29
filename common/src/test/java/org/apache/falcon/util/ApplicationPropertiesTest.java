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

package org.apache.falcon.util;

import org.apache.falcon.FalconException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;

/**
 * Test for Application properties test.
 */
public class ApplicationPropertiesTest {

    @Test
    public void testConfigLocation() throws Exception {
        File target = new File("target");
        if (!target.exists()) {
            target = new File("common/target");
        }

        FileOutputStream out = new FileOutputStream(new File(target, "config.properties"));
        out.write("*.domain=unittest\n".getBytes());
        out.write("unittest.test=hello world\n".getBytes());
        out.close();
        ApplicationProperties configLocation = new ConfigLocation();
        configLocation.loadProperties("config.properties", target.getAbsolutePath());
        Assert.assertEquals(configLocation.getDomain(), "unittest");
        Assert.assertEquals(configLocation.get("test"), "hello world");
    }

    @Test
    public void testClassPathLocation() throws Exception {
        ApplicationProperties classPathLocation = new ClassPathLocation();
        classPathLocation.loadProperties("classpath.properties", null);
        Assert.assertEquals(classPathLocation.getDomain(), "unittest");
        Assert.assertEquals(classPathLocation.get("test"), "hello world");
    }

    @Test (expectedExceptions = FalconException.class)
    public void testMissingLocation() throws FalconException {
        new MissingLocation().loadProperties();
    }

    private class ConfigLocation extends ApplicationProperties {

        protected ConfigLocation() throws FalconException {
        }

        protected void init() {}

        @Override
        protected String getPropertyFile() {
            return "config.properties";
        }
    }

    private class ClassPathLocation extends ApplicationProperties {

        protected ClassPathLocation() throws FalconException {
        }

        protected void init() {}

        @Override
        protected String getPropertyFile() {
            return "classpath.properties";
        }
    }

    private class MissingLocation extends ApplicationProperties {

        protected MissingLocation() throws FalconException {
        }

        @Override
        protected String getPropertyFile() {
            return "missing.properties";
        }
    }

}
