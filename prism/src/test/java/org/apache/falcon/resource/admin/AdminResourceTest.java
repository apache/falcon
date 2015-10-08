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
package org.apache.falcon.resource.admin;

import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.util.StartupProperties;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletResponse;

/**
 * Unit test for AdminResource.
 */
public class AdminResourceTest {
    public static final String FALCON_USER = "falcon-user";

    @BeforeClass
    public void setUp() throws Exception {
        CurrentUser.authenticate(FALCON_USER);
    }

    @Test
    public void testAdminVersion() throws Exception {
        AdminResource resource = new AdminResource();
        AdminResource.PropertyList propertyList = resource.getVersion();
        for(AdminResource.Property property : propertyList.properties) {
            if (property.key.equalsIgnoreCase("authentication")) {
                Assert.assertEquals(property.value, "simple");
            }
        }

        StartupProperties.get().setProperty("falcon.authentication.type", "kerberos");
        resource = new AdminResource();
        propertyList = resource.getVersion();
        for(AdminResource.Property property : propertyList.properties) {
            if (property.key.equalsIgnoreCase("authentication")) {
                Assert.assertEquals(property.value, "kerberos");
            }
        }
    }

    @Test
    public void testUserHandling() throws Exception {
        AdminResource resource = new AdminResource();
        Assert.assertEquals(FALCON_USER, resource.getAuthenticatedUser());
        HttpServletResponse response = new MockHttpServletResponse();
        Assert.assertEquals("ok", resource.clearUser(response));
    }

}
