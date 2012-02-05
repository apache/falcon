/*
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

package org.apache.ivory.workflow.engine;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.oozie.client.OozieClientException;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class OozieClient extends org.apache.oozie.client.OozieClient{

    private static final ConcurrentHashMap<Cluster, OozieClient> ref =
            new ConcurrentHashMap<Cluster, OozieClient>();

    public static OozieClient get(Cluster cluster) throws IvoryException {
        return new OozieClient(ClusterHelper.getOozieUrl(cluster));
    }

    private OozieClient(String oozieUrl) {
        super(oozieUrl);
    }

    public String schedule(Properties conf) throws IvoryException {
        try {
            return super.run(conf);
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }
}
