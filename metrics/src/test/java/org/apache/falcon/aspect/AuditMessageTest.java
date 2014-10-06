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

package org.apache.falcon.aspect;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test Message to be sent to the auditing system.
 */
public class AuditMessageTest {

    private final AuditMessage auditMessage = new AuditMessage(
            "falcon", "127.0.0.1", "LOCALHOST", "action", "127.0.0.1", "2014-09-15T20:56Z");

    @Test
    public void testGetRequestUrl() throws Exception {
        Assert.assertEquals(auditMessage.getRequestUrl(), "action");
    }

    @Test
    public void testGetUser() throws Exception {
        Assert.assertEquals(auditMessage.getUser(), "falcon");
    }

    @Test
    public void testGetRemoteHost() throws Exception {
        Assert.assertEquals(auditMessage.getRemoteHost(), "LOCALHOST");
    }

    @Test
    public void testGetCurrentTimeMillis() throws Exception {
        Assert.assertEquals(auditMessage.getRequestTimeISO9601(), "2014-09-15T20:56Z");
    }

    @Test
    public void testToString() throws Exception {
        Assert.assertEquals(auditMessage.toString(),
                "Audit: falcon@LOCALHOST performed action (127.0.0.1) at 2014-09-15T20:56Z");
    }
}
