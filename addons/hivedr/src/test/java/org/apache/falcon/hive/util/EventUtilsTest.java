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

package org.apache.falcon.hive.util;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for EventUtils.
 */
@Test
public class EventUtilsTest {
    private static final String JDBC_PREFIX = "jdbc:";
    private static final String HS2_URI = "hive2://localhost:10000:";
    private static final String HS2_ZK_URI = "hive2://host1.com:2181,host2.com:2181/";
    private static final String DATABASE = "test";
    private static final String HS2_SSL_EXTRA_OPTS = "ssl=true;"
            + "sslTrustStore=/var/lib/security/keystores/gateway.jks;"
            + "trustStorePassword=1234?hive.server2.transport.mode=http;hive.server2.thrift.http"
            + ".path=gateway/primaryCLuster/hive";
    private static final String HS2_ZK_EXTRA_OPTS = ";serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";
    private static final String AUTHTOKEN_STRING = ";auth=delegationToken";
    public EventUtilsTest() {
    }

    @Test
    public void validateHs2Uri() {
        final String expectedUri = JDBC_PREFIX + HS2_URI + "/" + DATABASE;

        Assert.assertEquals(EventUtils.getHS2ConnectionUrl(HS2_URI, DATABASE, null, null), expectedUri);
        Assert.assertEquals(EventUtils.getHS2ConnectionUrl(HS2_URI, DATABASE, null, "NA"), expectedUri);
        Assert.assertEquals(EventUtils.getHS2ConnectionUrl(HS2_URI, DATABASE, AUTHTOKEN_STRING,
                null), expectedUri + AUTHTOKEN_STRING);
    }

    @Test
    public void validateHs2UriWhenSSLEnabled() {
        final String expectedUri = JDBC_PREFIX + HS2_URI + "/" + DATABASE;

        Assert.assertEquals(EventUtils.getHS2ConnectionUrl(HS2_URI, DATABASE, null, HS2_SSL_EXTRA_OPTS),
                expectedUri + ";" + HS2_SSL_EXTRA_OPTS);
        Assert.assertEquals(EventUtils.getHS2ConnectionUrl(HS2_URI, DATABASE, AUTHTOKEN_STRING, HS2_SSL_EXTRA_OPTS),
                expectedUri + AUTHTOKEN_STRING + ";" + HS2_SSL_EXTRA_OPTS);
    }

    @Test
    public void validateHs2UriWhenZKDiscoveryEnabled() {
        final String expectedUri = JDBC_PREFIX + HS2_ZK_URI + DATABASE;

        Assert.assertEquals(EventUtils.getHS2ConnectionUrl(HS2_ZK_URI, DATABASE, null, HS2_ZK_EXTRA_OPTS),
                expectedUri + HS2_ZK_EXTRA_OPTS);
        Assert.assertEquals(EventUtils.getHS2ConnectionUrl(HS2_ZK_URI, DATABASE, AUTHTOKEN_STRING, HS2_ZK_EXTRA_OPTS),
                expectedUri + AUTHTOKEN_STRING + HS2_ZK_EXTRA_OPTS);
    }
}
