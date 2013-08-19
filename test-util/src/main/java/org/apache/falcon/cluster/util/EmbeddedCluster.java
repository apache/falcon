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

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfaces;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.cluster.Location;
import org.apache.falcon.entity.v0.cluster.Locations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.log4j.Logger;

/**
 * A utility class that doles out an embedded Hadoop cluster with DFS and/or MR.
 */
public class EmbeddedCluster {

    private static final Logger LOG = Logger.getLogger(EmbeddedCluster.class);

    protected EmbeddedCluster() {
    }

    private Configuration conf = new Configuration();
    private MiniDFSCluster dfsCluster;
    protected Cluster clusterEntity;

    public Configuration getConf() {
        return conf;
    }

    public static EmbeddedCluster newCluster(final String name) throws Exception {
        return createClusterAsUser(name);
    }

    public static EmbeddedCluster newCluster(final String name,
                                             final String user) throws Exception {
        UserGroupInformation hdfsUser = UserGroupInformation.createRemoteUser(user);
        return hdfsUser.doAs(new PrivilegedExceptionAction<EmbeddedCluster>() {
            @Override
            public EmbeddedCluster run() throws Exception {
                return createClusterAsUser(name);
            }
        });
    }

    private static EmbeddedCluster createClusterAsUser(String name) throws IOException {
        EmbeddedCluster cluster = new EmbeddedCluster();
        File target = new File("webapp/target");
        if (!target.exists()) {
            target = new File("target");
            System.setProperty("test.build.data", "target/" + name + "/data");
        } else {
            System.setProperty("test.build.data", "webapp/target/" + name + "/data");
        }
        cluster.conf.set("hadoop.tmp.dir", target.getAbsolutePath());
        cluster.conf.set("hadoop.log.dir", new File(target, "tmp").getAbsolutePath());
        cluster.conf.set("hadoop.proxyuser.oozie.groups", "*");
        cluster.conf.set("hadoop.proxyuser.oozie.hosts", "127.0.0.1");
        cluster.conf.set("hadoop.proxyuser.hdfs.groups", "*");
        cluster.conf.set("hadoop.proxyuser.hdfs.hosts", "127.0.0.1");
        cluster.conf.set("mapreduce.jobtracker.kerberos.principal", "");
        cluster.conf.set("dfs.namenode.kerberos.principal", "");
        cluster.dfsCluster = new MiniDFSCluster(cluster.conf, 1, true, null);
        ProxyUsers.refreshSuperUserGroupsConfiguration(cluster.conf);
        String hdfsUrl = cluster.conf.get("fs.default.name");
        LOG.info("Cluster Namenode = " + hdfsUrl);
        cluster.buildClusterObject(name);
        return cluster;
    }

    public FileSystem getFileSystem() throws IOException {
        return FileSystem.get(conf);
    }

    private void buildClusterObject(String name) {
        clusterEntity = new Cluster();
        clusterEntity.setName(name);
        clusterEntity.setColo("local");
        clusterEntity.setDescription("Embeded cluster: " + name);

        Interfaces interfaces = new Interfaces();
        interfaces.getInterfaces().add(newInterface(Interfacetype.WORKFLOW,
                "http://localhost:11000/oozie", "0.1"));
        String fsUrl = conf.get("fs.default.name");
        interfaces.getInterfaces().add(newInterface(Interfacetype.READONLY, fsUrl, "0.1"));
        interfaces.getInterfaces().add(newInterface(Interfacetype.WRITE, fsUrl, "0.1"));
        interfaces.getInterfaces().add(newInterface(Interfacetype.EXECUTE,
                conf.get("mapred.job.tracker"), "0.1"));
        interfaces
                .getInterfaces()
                .add(newInterface(
                        Interfacetype.MESSAGING,
                        "vm://localhost",
                        "0.1"));
        clusterEntity.setInterfaces(interfaces);

        Location location = new Location();
        location.setName("staging");
        location.setPath("/workflow/staging");
        Locations locs = new Locations();
        locs.getLocations().add(location);
        location = new Location();
        location.setName("working");
        location.setPath("/workflow/work");
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
        dfsCluster.shutdown();
    }

    public Cluster getCluster() {
        return clusterEntity;
    }

    public Cluster clone(String cloneName) {
        EmbeddedCluster clone = new EmbeddedCluster();
        clone.conf = this.conf;
        clone.buildClusterObject(cloneName);
        return clone.clusterEntity;
    }
}
