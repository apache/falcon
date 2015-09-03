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
import org.apache.falcon.util.FalconTestUtil;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;


/**
 * Unit test for AuthenticationInitializationService that employs mocks.
 */
public class AuthenticationInitializationServiceTest {

    private AuthenticationInitializationService authenticationService;

    @Mock
    private UserGroupInformation mockLoginUser;

    @BeforeClass
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        authenticationService = new AuthenticationInitializationService();
    }

    @Test
    public void testGetName() {
        Assert.assertEquals("Authentication initialization service",
                authenticationService.getName());
    }

    @Test
    public void testInitForSimpleAuthenticationMethod() {
        try {
            StartupProperties.get().setProperty(SecurityUtil.AUTHENTICATION_TYPE,
                    PseudoAuthenticationHandler.TYPE);
            authenticationService.init();

            UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
            Assert.assertFalse(loginUser.isFromKeytab());
            Assert.assertEquals(loginUser.getAuthenticationMethod().name().toLowerCase(),
                    PseudoAuthenticationHandler.TYPE);
            Assert.assertEquals(System.getProperty("user.name"), loginUser.getUserName());
        } catch (Exception e) {
            Assert.fail("AuthenticationInitializationService init failed.", e);
        }
    }

    @Test
    public void testKerberosAuthenticationWithKeytabFileDoesNotExist() {
        try {
            StartupProperties.get().setProperty(SecurityUtil.AUTHENTICATION_TYPE,
                    KerberosAuthenticationHandler.TYPE);
            StartupProperties.get().setProperty(AuthenticationInitializationService.KERBEROS_KEYTAB, "/blah/blah");
            authenticationService.init();
            Assert.fail("The keytab file does not exist! must have been thrown.");
        } catch (Exception e) {
            Assert.assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
    }

    @Test
    public void testKerberosAuthenticationWithKeytabFileIsADirectory() {
        try {
            StartupProperties.get().setProperty(SecurityUtil.AUTHENTICATION_TYPE,
                    KerberosAuthenticationHandler.TYPE);
            StartupProperties.get().setProperty(AuthenticationInitializationService.KERBEROS_KEYTAB, "/tmp/");
            authenticationService.init();
            Assert.fail("The keytab file cannot be a directory! must have been thrown.");
        } catch (Exception e) {
            Assert.assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        }
    }

    @Test
    public void testKerberosAuthenticationWithKeytabFileNotReadable() {
        File tempFile = new File(".keytabFile");
        try {
            assert tempFile.createNewFile();
            assert tempFile.setReadable(false);

            StartupProperties.get().setProperty(SecurityUtil.AUTHENTICATION_TYPE,
                    KerberosAuthenticationHandler.TYPE);
            StartupProperties.get().setProperty(
                    AuthenticationInitializationService.KERBEROS_KEYTAB, tempFile.toString());
            authenticationService.init();
            Assert.fail("The keytab file is not readable! must have been thrown.");
        } catch (Exception e) {
            Assert.assertEquals(e.getCause().getClass(), IllegalArgumentException.class);
        } finally {
            assert tempFile.delete();
        }
    }

    @Test (enabled = false)
    public void testInitForKerberosAuthenticationMethod() throws FalconException {
        Mockito.when(mockLoginUser.getAuthenticationMethod())
                .thenReturn(UserGroupInformation.AuthenticationMethod.KERBEROS);
        Mockito.when(mockLoginUser.getUserName()).thenReturn(FalconTestUtil.TEST_USER_1);
        Mockito.when(mockLoginUser.isFromKeytab()).thenReturn(Boolean.TRUE);

        StartupProperties.get().setProperty(SecurityUtil.AUTHENTICATION_TYPE,
                KerberosAuthenticationHandler.TYPE);
        StartupProperties.get().setProperty(
                AuthenticationInitializationService.KERBEROS_KEYTAB, "falcon.kerberos.keytab");
        StartupProperties.get().setProperty(AuthenticationInitializationService.KERBEROS_PRINCIPAL,
                FalconTestUtil.TEST_USER_1);

        authenticationService.init();

        Assert.assertTrue(mockLoginUser.isFromKeytab());
        Assert.assertEquals(mockLoginUser.getAuthenticationMethod().name(),
                KerberosAuthenticationHandler.TYPE);
        Assert.assertEquals(FalconTestUtil.TEST_USER_1, mockLoginUser.getUserName());
    }
}
