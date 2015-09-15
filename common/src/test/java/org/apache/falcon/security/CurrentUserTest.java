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

package org.apache.falcon.security;

import org.apache.falcon.cluster.util.EntityBuilderTestUtil;
import org.apache.falcon.service.GroupsService;
import org.apache.falcon.service.ProxyUserService;
import org.apache.falcon.service.Services;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.falcon.util.FalconTestUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test for current user's thread safety.
 */
public class CurrentUserTest {
    private ProxyUserService proxyUserService;
    private GroupsService groupsService;

    @BeforeClass
    public void setUp() throws Exception {
        Services.get().register(new ProxyUserService());
        Services.get().register(new GroupsService());
        groupsService = Services.get().getService(GroupsService.SERVICE_NAME);
        proxyUserService = Services.get().getService(ProxyUserService.SERVICE_NAME);
        groupsService.init();

        RuntimeProperties.get().setProperty("falcon.service.ProxyUserService.proxyuser.foo.hosts", "*");
        RuntimeProperties.get().setProperty("falcon.service.ProxyUserService.proxyuser.foo.groups", "*");
        proxyUserService.init();
    }

    @AfterClass
    public void tearDown() throws Exception {
        proxyUserService.destroy();
        groupsService.destroy();
        Services.get().reset();
    }

    @AfterMethod
    public void cleanUp() {
        CurrentUser.clear();
    }

    @Test(threadPoolSize = 10, invocationCount = 10, timeOut = 10000)
    public void testGetUser() throws Exception {
        String id = Long.toString(System.nanoTime());
        CurrentUser.authenticate(id);
        Assert.assertEquals(CurrentUser.getAuthenticatedUser(), id);
        Assert.assertEquals(CurrentUser.getUser(), id);
    }

    @Test (expectedExceptions = IllegalStateException.class)
    public void testAuthenticateBadUser() throws Exception {
        CurrentUser.authenticate("");
    }

    @Test (expectedExceptions = IllegalStateException.class)
    public void testGetAuthenticatedUserInvalid() throws Exception {
        CurrentUser.getAuthenticatedUser();
    }

    @Test (expectedExceptions = IllegalStateException.class)
    public void testGetUserInvalid() throws Exception {
        CurrentUser.getUser();
    }

    @Test (expectedExceptions = IllegalStateException.class)
    public void testProxyBadUser() throws Exception {
        CurrentUser.authenticate(FalconTestUtil.TEST_USER_1);
        CurrentUser.proxy("", "");
    }

    @Test (expectedExceptions = IllegalStateException.class)
    public void testProxyWithNoAuth() throws Exception {
        CurrentUser.proxy(FalconTestUtil.TEST_USER_1, "falcon");
    }

    @Test
    public void testGetProxyUserForAuthenticatedUser() throws Exception {
        CurrentUser.authenticate("proxy");
        UserGroupInformation proxyUgi = CurrentUser.getProxyUGI();
        Assert.assertNotNull(proxyUgi);
        Assert.assertEquals(proxyUgi.getUserName(), "proxy");
    }

    @Test
    public void testProxy() throws Exception {
        CurrentUser.authenticate("real");

        CurrentUser.proxy(EntityBuilderTestUtil.USER, "users");
        UserGroupInformation proxyUgi = CurrentUser.getProxyUGI();
        Assert.assertNotNull(proxyUgi);
        Assert.assertEquals(proxyUgi.getUserName(), EntityBuilderTestUtil.USER);

        Assert.assertEquals(CurrentUser.getAuthenticatedUser(), "real");
        Assert.assertEquals(CurrentUser.getUser(), EntityBuilderTestUtil.USER);
    }

    @Test
    public void testProxySameUser() throws Exception {
        CurrentUser.authenticate(FalconTestUtil.TEST_USER_1);

        CurrentUser.proxy(FalconTestUtil.TEST_USER_1, "users");
        UserGroupInformation proxyUgi = CurrentUser.getProxyUGI();
        Assert.assertNotNull(proxyUgi);
        Assert.assertEquals(proxyUgi.getUserName(), FalconTestUtil.TEST_USER_1);

        Assert.assertEquals(CurrentUser.getAuthenticatedUser(), FalconTestUtil.TEST_USER_1);
        Assert.assertEquals(CurrentUser.getUser(), FalconTestUtil.TEST_USER_1);
    }

    @Test
    public void testSuperUser() throws Exception {
        CurrentUser.authenticate(EntityBuilderTestUtil.USER);
        CurrentUser.proxy("proxy", "users");

        UserGroupInformation proxyUgi = CurrentUser.getProxyUGI();
        Assert.assertNotNull(proxyUgi);
        Assert.assertEquals(proxyUgi.getUserName(), "proxy");

        Assert.assertEquals(CurrentUser.getAuthenticatedUser(), EntityBuilderTestUtil.USER);
        Assert.assertEquals(CurrentUser.getUser(), "proxy");
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testProxyDoAsUserWithNoAuth() throws Exception {
        CurrentUser.proxyDoAsUser("falcon", "localhost");
    }

    @Test
    public void testProxyDoAsUser() throws Exception {
        CurrentUser.authenticate("foo");

        CurrentUser.proxyDoAsUser(EntityBuilderTestUtil.USER, "localhost");
        UserGroupInformation proxyUgi = CurrentUser.getProxyUGI();
        Assert.assertNotNull(proxyUgi);
        Assert.assertEquals(proxyUgi.getUserName(), EntityBuilderTestUtil.USER);

        Assert.assertEquals(CurrentUser.getAuthenticatedUser(), "foo");
        Assert.assertEquals(CurrentUser.getUser(), EntityBuilderTestUtil.USER);
    }

    @Test
    public void testProxyDoAsSameUser() throws Exception {
        CurrentUser.authenticate("foo");

        CurrentUser.proxyDoAsUser("foo", "localhost");
        UserGroupInformation proxyUgi = CurrentUser.getProxyUGI();
        Assert.assertNotNull(proxyUgi);
        Assert.assertEquals(proxyUgi.getUserName(), "foo");

        Assert.assertEquals(CurrentUser.getAuthenticatedUser(), "foo");
        Assert.assertEquals(CurrentUser.getUser(), "foo");
    }
}
