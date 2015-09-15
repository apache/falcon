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

package org.apache.falcon.service;

import org.apache.falcon.FalconException;
import org.apache.falcon.util.RuntimeProperties;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.security.AccessControlException;
import java.util.List;

/**
 * Unit tests for ProxyUserService.
 */
public class ProxyUserServiceTest {

    private ProxyUserService proxyUserService;
    private GroupsService groupsService;

    @BeforeClass
    public void setUp() throws Exception {
        Services.get().register(new ProxyUserService());
        Services.get().register(new GroupsService());

        groupsService = Services.get().getService(GroupsService.SERVICE_NAME);
        proxyUserService = Services.get().getService(ProxyUserService.SERVICE_NAME);
        groupsService.init();
    }

    @AfterClass
    public void tearDown() throws Exception {
        proxyUserService.destroy();
        groupsService.destroy();
        Services.get().reset();
    }

    @Test
    public void testGetName() throws Exception {
        proxyUserService.init();
        Assert.assertEquals(proxyUserService.getName(), ProxyUserService.SERVICE_NAME);
    }

    @Test (expectedExceptions = FalconException.class, expectedExceptionsMessageRegExp = ".*falcon.service"
            + ".ProxyUserService.proxyuser.foo.groups property not set in runtime properties.*")
    public void testWrongConfigGroups() throws Exception {
        RuntimeProperties.get().setProperty("falcon.service.ProxyUserService.proxyuser.foo.hosts", "*");
        RuntimeProperties.get().remove("falcon.service.ProxyUserService.proxyuser.foo.groups");
        proxyUserService.init();
    }

    @Test (expectedExceptions = FalconException.class, expectedExceptionsMessageRegExp = ".*falcon.service"
            + ".ProxyUserService.proxyuser.foo.hosts property not set in runtime properties.*")
    public void testWrongConfigHosts() throws Exception {
        RuntimeProperties.get().setProperty("falcon.service.ProxyUserService.proxyuser.foo.groups", "*");
        RuntimeProperties.get().remove("falcon.service.ProxyUserService.proxyuser.foo.hosts");
        proxyUserService.init();
    }

    @Test (expectedExceptions = FalconException.class,
           expectedExceptionsMessageRegExp = "Exception normalizing host name.*")
    public void testWrongHost() throws Exception {
        RuntimeProperties.get().setProperty("falcon.service.ProxyUserService.proxyuser.foo.hosts", "otherhost");
        RuntimeProperties.get().setProperty("falcon.service.ProxyUserService.proxyuser.foo.groups", "*");
        proxyUserService.init();
    }

    @Test
    public void testValidateAnyHostAnyUser() throws Exception {
        RuntimeProperties.get().setProperty("falcon.service.ProxyUserService.proxyuser.foo.hosts", "*");
        RuntimeProperties.get().setProperty("falcon.service.ProxyUserService.proxyuser.foo.groups", "*");
        proxyUserService.init();
        proxyUserService.validate("foo", "localhost", "bar");
    }

    @Test (expectedExceptions = AccessControlException.class,
           expectedExceptionsMessageRegExp = "User .* not defined as proxyuser.*")
    public void testInvalidProxyUser() throws Exception {
        RuntimeProperties.get().setProperty("falcon.service.ProxyUserService.proxyuser.foo.hosts", "*");
        RuntimeProperties.get().setProperty("falcon.service.ProxyUserService.proxyuser.foo.groups", "*");
        proxyUserService.init();
        proxyUserService.validate("bar", "localhost", "foo");
    }

    @Test
    public void testValidateHost() throws Exception {
        RuntimeProperties.get().setProperty("falcon.service.ProxyUserService.proxyuser.foo.hosts", "*");
        RuntimeProperties.get().setProperty("falcon.service.ProxyUserService.proxyuser.foo.groups", "*");
        proxyUserService.init();
        proxyUserService.validate("foo", "localhost", "bar");
    }

    private String getGroup() throws Exception {
        List<String> g = groupsService.getGroups(System.getProperty("user.name"));
        return g.get(0);
    }

    @Test
    public void testValidateGroup() throws Exception {
        RuntimeProperties.get().setProperty("falcon.service.ProxyUserService.proxyuser.foo.hosts", "*");
        RuntimeProperties.get().setProperty("falcon.service.ProxyUserService.proxyuser.foo.groups",
                    getGroup());

        proxyUserService.init();
        proxyUserService.validate("foo", "localhost", System.getProperty("user.name"));
    }

    @Test (expectedExceptions = AccessControlException.class,
        expectedExceptionsMessageRegExp = "Could not resolve host .*")
    public void testUnknownHost() throws Exception {
        RuntimeProperties.get().setProperty("falcon.service.ProxyUserService.proxyuser.foo.hosts", "localhost");
        RuntimeProperties.get().setProperty("falcon.service.ProxyUserService.proxyuser.foo.groups", "*");
        proxyUserService.init();
        proxyUserService.validate("foo", "unknownhost.bar.foo", "bar");
    }

    @Test (expectedExceptions = AccessControlException.class,
            expectedExceptionsMessageRegExp = "Unauthorized host .*")
    public void testInvalidHost() throws Exception {
        RuntimeProperties.get().setProperty("falcon.service.ProxyUserService.proxyuser.foo.hosts", "localhost");
        RuntimeProperties.get().setProperty("falcon.service.ProxyUserService.proxyuser.foo.groups", "*");
        proxyUserService.init();
        proxyUserService.validate("foo", "www.example.com", "bar");
    }

    @Test (expectedExceptions = AccessControlException.class,
           expectedExceptionsMessageRegExp = "Unauthorized proxyuser .*, not in proxyuser groups")
    public void testInvalidGroup() throws Exception {
        RuntimeProperties.get().setProperty("falcon.service.ProxyUserService.proxyuser.foo.hosts", "localhost");
        RuntimeProperties.get().setProperty("falcon.service.ProxyUserService.proxyuser.foo.groups", "nobody");
        proxyUserService.init();
        proxyUserService.validate("foo", "localhost", System.getProperty("user.name"));
    }

    @Test (expectedExceptions = IllegalArgumentException.class,
           expectedExceptionsMessageRegExp = "proxyUser cannot be null or empty, .*")
    public void testNullProxyUser() throws Exception {
        proxyUserService.init();
        proxyUserService.validate(null, "localhost", "bar");
    }

    @Test (expectedExceptions = IllegalArgumentException.class,
           expectedExceptionsMessageRegExp = "proxyHost cannot be null or empty, .*")
    public void testNullHost() throws Exception {
        proxyUserService.init();
        proxyUserService.validate("foo", null, "bar");
    }

}
