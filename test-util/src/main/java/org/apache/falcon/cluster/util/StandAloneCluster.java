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

package org.apache.falcon.cluster.util;

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * A standalone cluster.
 */
public final class StandAloneCluster extends EmbeddedCluster {
    private static final Logger LOG = LoggerFactory.getLogger(StandAloneCluster.class);

    private StandAloneCluster() {
    }

    public static StandAloneCluster newCluster(String clusterFile) throws Exception {
        LOG.debug("Initialising standalone cluster");
        StandAloneCluster cluster = new StandAloneCluster();
        cluster.clusterEntity = (Cluster) EntityType.CLUSTER.getUnmarshaller().unmarshal(new File(clusterFile));

        for (Interface inter : cluster.getCluster().getInterfaces().getInterfaces()) {
            if (inter.getType() == Interfacetype.WRITE) {
                cluster.getConf().set("fs.defaultFS", inter.getEndpoint());
                break;
            }
        }

        LOG.info("Cluster Namenode = {}", cluster.getConf().get("fs.defaultFS"));
        return cluster;
    }

    @Override
    public void shutdown() {
    }
}
