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

package org.apache.falcon.entity;

import junit.framework.Assert;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfaces;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.cluster.Property;
import org.apache.falcon.security.SecurityUtil;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.security.authentication.server.KerberosAuthenticationHandler;
import org.apache.hadoop.security.authentication.server.PseudoAuthenticationHandler;
import org.testng.annotations.Test;

import java.util.Properties;

/**
 * Tests for HiveUtil.
 */
public class HiveUtilTest {

    @Test
    public void testGetHiveCredentialsWithoutKerberos() {
        StartupProperties.get().setProperty(SecurityUtil.AUTHENTICATION_TYPE, PseudoAuthenticationHandler.TYPE);
        Cluster cluster = new Cluster();
        String metaStoreUrl = "thrift://localhost:19083";

        // set registry interface
        Interfaces interfaces = new Interfaces();
        Interface registry = new Interface();
        registry.setEndpoint(metaStoreUrl);
        registry.setType(Interfacetype.REGISTRY);
        registry.setVersion("0.1");
        interfaces.getInterfaces().add(registry);
        cluster.setInterfaces(interfaces);

        Properties expected = new Properties();
        expected.put(HiveUtil.METASTORE_UGI, "true");
        expected.put(HiveUtil.NODE, metaStoreUrl.replace("thrift", "hcat"));
        expected.put(HiveUtil.METASTORE_URI, metaStoreUrl);
        expected.put(HiveUtil.METASTOREURIS, metaStoreUrl);

        Properties actual = HiveUtil.getHiveCredentials(cluster);
        Assert.assertTrue(actual.equals(expected));
    }

    @Test
    public void testGetHiveCredentialsWithKerberos() {
        StartupProperties.get().setProperty(SecurityUtil.AUTHENTICATION_TYPE, KerberosAuthenticationHandler.TYPE);
        Cluster cluster = new Cluster();
        String metaStoreUrl = "thrift://localhost:19083";
        String principal = "kerberosPrincipal";

        // set registry interface
        Interfaces interfaces = new Interfaces();
        Interface registry = new Interface();
        registry.setEndpoint(metaStoreUrl);
        registry.setType(Interfacetype.REGISTRY);
        registry.setVersion("0.1");
        interfaces.getInterfaces().add(registry);
        cluster.setInterfaces(interfaces);

        // set security properties
        org.apache.falcon.entity.v0.cluster.Properties props = new org.apache.falcon.entity.v0.cluster.Properties();
        Property principal2 = new Property();
        principal2.setName(SecurityUtil.HIVE_METASTORE_KERBEROS_PRINCIPAL);
        principal2.setValue(principal);
        props.getProperties().add(principal2);
        cluster.setProperties(props);
        Properties expected = new Properties();
        expected.put(SecurityUtil.METASTORE_USE_THRIFT_SASL, "true");
        expected.put(SecurityUtil.HIVE_METASTORE_KERBEROS_PRINCIPAL, principal);
        expected.put(SecurityUtil.METASTORE_PRINCIPAL, principal);
        expected.put(HiveUtil.METASTORE_UGI, "true");
        expected.put(HiveUtil.NODE, metaStoreUrl.replace("thrift", "hcat"));
        expected.put(HiveUtil.METASTORE_URI, metaStoreUrl);
        expected.put(HiveUtil.METASTOREURIS, metaStoreUrl);

        Properties actual = HiveUtil.getHiveCredentials(cluster);
        Assert.assertTrue(actual.equals(expected));
    }


}

