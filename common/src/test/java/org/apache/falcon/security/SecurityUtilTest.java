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


import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.process.ACL;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.util.FalconTestUtil;
import org.apache.falcon.util.StartupProperties;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * Unit test for Security utils.
 */
public class SecurityUtilTest {

    @Test
    public void testDefaultGetAuthenticationType() throws Exception {
        Assert.assertEquals(SecurityUtil.getAuthenticationType(), "simple");
    }

    @Test
    public void testGetAuthenticationType() throws Exception {
        try {
            StartupProperties.get().setProperty(SecurityUtil.AUTHENTICATION_TYPE, "kerberos");
            Assert.assertEquals(SecurityUtil.getAuthenticationType(), "kerberos");
        } finally {
            // reset
            StartupProperties.get().setProperty(SecurityUtil.AUTHENTICATION_TYPE, "simple");
        }
    }

    @Test
    public void testIsSecurityEnabledByDefault() throws Exception {
        Assert.assertFalse(SecurityUtil.isSecurityEnabled());
    }

    @Test
    public void testIsSecurityEnabled() throws Exception {
        try {
            StartupProperties.get().setProperty(SecurityUtil.AUTHENTICATION_TYPE, "kerberos");
            Assert.assertTrue(SecurityUtil.isSecurityEnabled());
        } finally {
            // reset
            StartupProperties.get().setProperty(SecurityUtil.AUTHENTICATION_TYPE, "simple");
        }
    }

    @Test
    public void testIsAuthorizationEnabledByDefault() throws Exception {
        Assert.assertFalse(SecurityUtil.isAuthorizationEnabled());
    }

    @Test
    public void testIsAuthorizationEnabled() throws Exception {
        try {
            StartupProperties.get().setProperty("falcon.security.authorization.enabled", "true");
            Assert.assertTrue(SecurityUtil.isAuthorizationEnabled());
        } finally {
            // reset
            StartupProperties.get().setProperty("falcon.security.authorization.enabled", "false");
        }
    }

    @Test
    public void testGetAuthorizationProviderByDefault() throws Exception {
        Assert.assertNotNull(SecurityUtil.getAuthorizationProvider());
        Assert.assertEquals(SecurityUtil.getAuthorizationProvider().getClass(),
                DefaultAuthorizationProvider.class);
    }

    @Test
    public void testTryProxy() throws IOException, FalconException {
        Process process = Mockito.mock(Process.class);
        StartupProperties.get().setProperty("falcon.security.authorization.enabled", "true");
        final String currentUser = System.getProperty("user.name");

        // When ACL not specified
        CurrentUser.authenticate(currentUser);
        SecurityUtil.tryProxy(process);
        Assert.assertEquals(CurrentUser.getUser(), currentUser);

        ACL acl = new ACL();
        acl.setOwner(FalconTestUtil.TEST_USER_2);
        acl.setGroup("users");
        Mockito.when(process.getACL()).thenReturn(acl);

        // When ACL is specified
        SecurityUtil.tryProxy(process);
        Assert.assertEquals(CurrentUser.getUser(), FalconTestUtil.TEST_USER_2);
    }
}
