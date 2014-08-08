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

package org.apache.falcon.hadoop;

import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.security.CurrentUser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;

/**
 * Unit tests for HadoopClientFactory that doles out FileSystem handles.
 */
public class HadoopClientFactoryTest {

    private EmbeddedCluster embeddedCluster;

    @BeforeClass
    public void setUp() throws Exception {
        embeddedCluster = EmbeddedCluster.newCluster(getClass().getSimpleName());
    }

    @AfterClass
    public void tearDown() throws Exception {
        if (embeddedCluster != null) {
            embeddedCluster.shutdown();
        }
    }

    @Test
    public void testGet() throws Exception {
        HadoopClientFactory clientFactory = HadoopClientFactory.get();
        Assert.assertNotNull(clientFactory);
    }

    @Test (enabled = false) // todo: cheated the conf to impersonate as same user
    public void testCreateFileSystemWithSameUser() {
        String user = System.getProperty("user.name");
        CurrentUser.authenticate(user);
        try {
            Configuration conf = embeddedCluster.getConf();
            URI uri = new URI(conf.get(HadoopClientFactory.FS_DEFAULT_NAME_KEY));
            Assert.assertNotNull(uri);
            HadoopClientFactory.get().createFileSystem(CurrentUser.getProxyUgi(), uri, conf);
            Assert.fail("Impersonation should have failed.");
        } catch (Exception e) {
            Assert.assertEquals(e.getCause().getClass(), RemoteException.class);
        }
    }

    @Test
    public void testCreateFileSystem() throws Exception {
        Configuration conf = embeddedCluster.getConf();

        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation realUser = UserGroupInformation.createUserForTesting(
                "testuser", new String[]{"testgroup"});
        UserGroupInformation.createProxyUserForTesting("proxyuser", realUser, new String[]{"proxygroup"});

        URI uri = new URI(conf.get(HadoopClientFactory.FS_DEFAULT_NAME_KEY));
        Assert.assertNotNull(uri);
        FileSystem fs = HadoopClientFactory.get().createFileSystem(realUser, uri, conf);
        Assert.assertNotNull(fs);
    }

    @Test
    public void testCreateFileSystemWithUser() throws Exception {
        Configuration conf = embeddedCluster.getConf();

        UserGroupInformation realUser = UserGroupInformation.createUserForTesting(
                "testuser", new String[]{"testgroup"});
        UserGroupInformation.createProxyUserForTesting("proxyuser", realUser, new String[]{"proxygroup"});
        UserGroupInformation.setConfiguration(conf);

        URI uri = new URI(conf.get(HadoopClientFactory.FS_DEFAULT_NAME_KEY));
        Assert.assertNotNull(uri);

        CurrentUser.authenticate(System.getProperty("user.name"));
        FileSystem fs = HadoopClientFactory.get().createFileSystem(CurrentUser.getProxyUgi(), uri, conf);
        Assert.assertNotNull(fs);
    }
}
