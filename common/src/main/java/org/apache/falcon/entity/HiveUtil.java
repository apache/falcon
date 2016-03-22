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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.security.SecurityUtil;

import java.util.Properties;

/**
 * Hive Utilities.
 */
public final class HiveUtil {
    public static final String METASTOREURIS = "hive.metastore.uris";
    public static final String METASTORE_URI = "hcat.metastore.uri";
    public static final String NODE = "hcatNode";
    public static final String METASTORE_UGI = "hive.metastore.execute.setugi";

    private HiveUtil() {

    }

    public static Properties getHiveCredentials(Cluster cluster) {
        String metaStoreUrl = ClusterHelper.getRegistryEndPoint(cluster);
        if (StringUtils.isBlank(metaStoreUrl)) {
            throw new IllegalStateException(
                    "Registry interface is not defined in cluster: " + cluster.getName());
        }

        Properties hiveCredentials = new Properties();
        hiveCredentials.put(METASTOREURIS, metaStoreUrl);
        hiveCredentials.put(METASTORE_UGI, "true");
        hiveCredentials.put(NODE, metaStoreUrl.replace("thrift", "hcat"));
        hiveCredentials.put(METASTORE_URI, metaStoreUrl);

        if (SecurityUtil.isSecurityEnabled()) {
            String principal = ClusterHelper
                    .getPropertyValue(cluster, SecurityUtil.HIVE_METASTORE_KERBEROS_PRINCIPAL);
            hiveCredentials.put(SecurityUtil.HIVE_METASTORE_KERBEROS_PRINCIPAL, principal);
            hiveCredentials.put(SecurityUtil.METASTORE_PRINCIPAL, principal);
            hiveCredentials.put(SecurityUtil.METASTORE_USE_THRIFT_SASL, "true");
        }
        return hiveCredentials;
    }
}
