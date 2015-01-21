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

import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.ClusterLocationType;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfaces;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.cluster.Location;
import org.apache.falcon.entity.v0.cluster.Locations;
import org.apache.falcon.hadoop.JailedFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * A utility class that doles out an embedded Hadoop cluster with DFS and/or MR.
 */
public class EmbeddedCluster {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedCluster.class);

    protected EmbeddedCluster() {
    }

    protected Configuration conf = newConfiguration();
    protected Cluster clusterEntity;

    public Configuration getConf() {
        return conf;
    }

    public static Configuration newConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("fs.jail.impl", JailedFileSystem.class.getName());
        return configuration;
    }

    public static EmbeddedCluster newCluster(final String name) throws Exception {
        return createClusterAsUser(name, false, null, null);
    }

    public static EmbeddedCluster newCluster(final String name, boolean global) throws Exception {
        return createClusterAsUser(name, global, null, null);
    }

    public static EmbeddedCluster newCluster(final String name,
                                             final String user) throws Exception {
        UserGroupInformation hdfsUser = UserGroupInformation.createRemoteUser(user);
        return hdfsUser.doAs(new PrivilegedExceptionAction<EmbeddedCluster>() {
            @Override
            public EmbeddedCluster run() throws Exception {
                return createClusterAsUser(name, false, null, null);
            }
        });
    }

    public static EmbeddedCluster newCluster(final String name, boolean global, final String colo,
                                             final String tags) throws Exception {
        return createClusterAsUser(name, global, colo, tags);
    }

    private static EmbeddedCluster createClusterAsUser(String name, boolean global, String colo,
                                                       String tags) throws IOException {
        EmbeddedCluster cluster = new EmbeddedCluster();
        cluster.conf.set("fs.defaultFS", "jail://" + (global ? "global" : name) + ":00");

        String hdfsUrl = cluster.conf.get("fs.defaultFS");
        LOG.info("Cluster Namenode = {}", hdfsUrl);
        cluster.buildClusterObject(name, colo, tags);
        return cluster;
    }

    public FileSystem getFileSystem() throws IOException {
        return FileSystem.get(conf);
    }

    protected void buildClusterObject(String name, String colo, String tags) {
        clusterEntity = new Cluster();
        clusterEntity.setName(name);
        clusterEntity.setColo((colo == null) ? "local" : colo);
        clusterEntity.setDescription("Embedded cluster: " + name);
        if (tags != null) {
            clusterEntity.setTags(tags);
        }
        clusterEntity.setDescription("Embedded cluster: " + name);

        Interfaces interfaces = new Interfaces();
        interfaces.getInterfaces().add(newInterface(Interfacetype.WORKFLOW,
                "http://localhost:41000/oozie", "0.1"));
        String fsUrl = conf.get("fs.defaultFS");
        interfaces.getInterfaces().add(newInterface(Interfacetype.READONLY, fsUrl, "0.1"));
        interfaces.getInterfaces().add(newInterface(Interfacetype.WRITE, fsUrl, "0.1"));
        interfaces.getInterfaces().add(newInterface(Interfacetype.EXECUTE,
                "localhost:41021", "0.1"));
        interfaces.getInterfaces().add(
                newInterface(Interfacetype.REGISTRY, "thrift://localhost:49083", "0.1"));
        interfaces.getInterfaces().add(
                newInterface(Interfacetype.MESSAGING, "vm://localhost", "0.1"));
        clusterEntity.setInterfaces(interfaces);

        Location location = new Location();
        location.setName(ClusterLocationType.STAGING);
        location.setPath("/projects/falcon/staging");
        Locations locs = new Locations();
        locs.getLocations().add(location);
        location = new Location();
        location.setName(ClusterLocationType.WORKING);
        location.setPath("/projects/falcon/working");
        locs.getLocations().add(location);
        clusterEntity.setLocations(locs);
    }

    private Interface newInterface(Interfacetype type,
                                   String endPoint, String version) {
        Interface iface = new Interface();
        iface.setType(type);
        iface.setEndpoint(endPoint);
        iface.setVersion(version);
        return iface;
    }

    public void shutdown() {
    }

    public Cluster getCluster() {
        return clusterEntity;
    }
}
