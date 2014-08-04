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

package org.apache.falcon.regression.core.util;

import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.testng.Assert;
import org.apache.log4j.Logger;

/**
 * Util methods for Kerberos.
 */
public final class KerberosHelper {
    private KerberosHelper() {
        throw new AssertionError("Instantiating utility class...");
    }

    private static final Logger LOGGER = Logger.getLogger(KerberosHelper.class);

    public static void loginFromKeytab(String user) {
        if (!MerlinConstants.IS_SECURE) {
            LOGGER.info("Kerberos is disabled, hence no user switching.");
            return;
        }
        if (user == null) {
            user = MerlinConstants.CURRENT_USER_NAME;
        }
        final String keytab = MerlinConstants.getKeytabForUser(user);
        final String command = String.format("kinit -kt %s %s", keytab, user);
        final int exitVal = ExecUtil.executeCommandGetExitCode(command);
        Assert.assertEquals(exitVal, 0, "Switching Kerberos credential did not succeed.");
    }

}
