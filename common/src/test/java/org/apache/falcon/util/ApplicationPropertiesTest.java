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
import org.apache.falcon.security.CredentialProviderHelper;
import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;

/**
 * Test for Application properties test.
 */
public class ApplicationPropertiesTest {
    private static final String ALIAS_1 = "alias-key-1";
    private static final String ALIAS_2 = "alias-key-2";
    private static final String PASSWORD_1 = "password1";
    private static final String PASSWORD_2 = "password2";
    private static final String PROPERTY_1 = "property-key-1";
    private static final String PROPERTY_2 = "property-key-2";
    private static final String JKS_FILE_NAME = "credentials.jks";

    private static final File CRED_DIR = new File(".");

    @AfterClass
    public void tearDown() throws Exception {
        // delete temporary jks files
        File file = new File(CRED_DIR, JKS_FILE_NAME);
        file.delete();
        file = new File(CRED_DIR, "." + JKS_FILE_NAME + ".crc");
        file.delete();
    }

    @Test
    public void testResolveAlias() throws Exception {
        // hadoop credential provider needs to be available
        Assert.assertTrue(CredentialProviderHelper.isProviderAvailable());

        // clean credential provider store
        File file = new File(CRED_DIR, JKS_FILE_NAME);
        file.delete();

        // add alias to hadoop credential provider
        Configuration conf = new Configuration();
        String providerPath = "jceks://file/" + CRED_DIR.getAbsolutePath() + "/" + JKS_FILE_NAME;
        conf.set(CredentialProviderHelper.CREDENTIAL_PROVIDER_PATH, providerPath);
        CredentialProviderHelper.createCredentialEntry(conf, ALIAS_1, PASSWORD_1);
        CredentialProviderHelper.createCredentialEntry(conf, ALIAS_2, PASSWORD_2);

        // test case: no credential properties to resolve
        ApplicationProperties properties = new ConfigLocation();
        properties.resolveAlias();

        // test case: multiple credential properties to resolve
        properties.put(ApplicationProperties.CREDENTIAL_PROVIDER_PROPERTY, providerPath);
        properties.put(ApplicationProperties.ALIAS_PROPERTY_PREFIX + PROPERTY_1, ALIAS_1);
        properties.put(ApplicationProperties.ALIAS_PROPERTY_PREFIX + PROPERTY_2, ALIAS_2);
        properties.resolveAlias();
        Assert.assertEquals(properties.getProperty(PROPERTY_1), PASSWORD_1);
        Assert.assertEquals(properties.getProperty(PROPERTY_2), PASSWORD_2);
    }

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

    @Test
    public void testPropertiesWithSpaces() throws Exception{
        ApplicationProperties properties = new ConfigLocation();
        properties.put("key1", "value with trailing spaces.  ");
        properties.put("key2", "  value with leading spaces.");
        properties.put("key3", "  value with spaces on both ends. ");
        Assert.assertEquals(properties.getProperty("key1"), "value with trailing spaces.");
        Assert.assertEquals(properties.getProperty("key2"), "value with leading spaces.");
        Assert.assertEquals(properties.getProperty("key3"), "value with spaces on both ends.");
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
