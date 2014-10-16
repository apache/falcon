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

import org.apache.hadoop.security.UserGroupInformation;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test for current user's thread safety.
 */
public class CurrentUserTest {

    @Test(threadPoolSize = 10, invocationCount = 10, timeOut = 10000)
    public void testGetUser() throws Exception {
        String id = Long.toString(System.nanoTime());
        CurrentUser.authenticate(id);
        Assert.assertEquals(CurrentUser.getUser(), id);
    }

    @Test
    public void testGetProxyUser() throws Exception {
        CurrentUser.authenticate("proxy");
        UserGroupInformation proxyUgi = CurrentUser.getProxyUGI();
        Assert.assertNotNull(proxyUgi);
        Assert.assertEquals(proxyUgi.getUserName(), "proxy");
    }
}
