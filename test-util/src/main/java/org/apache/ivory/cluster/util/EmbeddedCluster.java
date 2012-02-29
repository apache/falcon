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

package org.apache.ivory.cluster.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ivory.entity.v0.cluster.*;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

public class EmbeddedCluster {

    private static Logger LOG = Logger.getLogger(EmbeddedCluster.class);

    private EmbeddedCluster() {
    }

    private Configuration conf = new Configuration();
    private MiniDFSCluster dfsCluster;
    private MiniMRCluster mrCluster;
    private Cluster clusterEntity;

    public Configuration getConf() {
        return conf;
    }

    public static EmbeddedCluster newCluster(final String name, final boolean withMR)
            throws Exception {
        return createClusterAsUser(name, withMR);
    }

    public static EmbeddedCluster newCluster(final String name,
                                             final boolean withMR,
                                             final String user)
            throws Exception {

        UserGroupInformation hdfsUser = UserGroupInformation.createRemoteUser(user);
        return hdfsUser.doAs(new PrivilegedExceptionAction<EmbeddedCluster>() {
            @Override
            public EmbeddedCluster run() throws Exception {
                return createClusterAsUser(name, withMR);
            }
        });
    }

    private static EmbeddedCluster createClusterAsUser(String name,
                                                       boolean withMR)
            throws IOException {

        EmbeddedCluster cluster = new EmbeddedCluster();
        File target = new File("webapp/target");
        if (!target.exists()) {
            target = new File("target");
            System.setProperty("test.build.data", "target/" + name + "/data");
        } else {
            System.setProperty("test.build.data", "webapp/target/" + name + "/data");
        }
        String user = System.getProperty("user.name");
        cluster.conf.set("hadoop.log.dir", "/tmp");
        cluster.conf.set("hadoop.proxyuser.oozie.groups", "*");
        cluster.conf.set("hadoop.proxyuser.oozie.hosts", "127.0.0.1");
        cluster.conf.set("hadoop.proxyuser.hdfs.groups", "*");
        cluster.conf.set("hadoop.proxyuser.hdfs.hosts", "127.0.0.1");
        cluster.conf.set("mapreduce.jobtracker.kerberos.principal", "");
        cluster.conf.set("dfs.namenode.kerberos.principal", "");
        cluster.dfsCluster = new MiniDFSCluster(cluster.conf, 1, true, null);
        String hdfsUrl = cluster.conf.get("fs.default.name");
        LOG.info("Cluster Namenode = " + hdfsUrl);
        if (withMR) {
            System.setProperty("hadoop.log.dir", "/tmp");
            System.setProperty("org.apache.hadoop.mapred.TaskTracker", "/tmp");
            cluster.conf.set("org.apache.hadoop.mapred.TaskTracker", "/tmp");
            cluster.conf.set("org.apache.hadoop.mapred.TaskTracker", "/tmp");
            cluster.conf.set("mapreduce.jobtracker.staging.root.dir", "/user");
            Path path = new Path("/tmp/hadoop-" + user, "mapred");
            FileSystem.get(cluster.conf).mkdirs(path);
            FileSystem.get(cluster.conf).setPermission(path, new FsPermission((short)511));
            cluster.mrCluster = new MiniMRCluster(1,
                    hdfsUrl, 1);
            Configuration mrConf = cluster.mrCluster.createJobConf();
            cluster.conf.set("mapred.job.tracker",
                    mrConf.get("mapred.job.tracker"));
            cluster.conf.set("mapred.job.tracker.http.address",
                    mrConf.get("mapred.job.tracker.http.address"));
            LOG.info("Cluster JobTracker = " + cluster.conf.
                    get("mapred.job.tracker"));
        }
        cluster.buildClusterObject(name);
        return cluster;
    }

    private void buildClusterObject(String name) {
        clusterEntity = new Cluster();
        clusterEntity.setName(name);
        clusterEntity.setColo("local");
        clusterEntity.setDescription("Embeded cluster: " + name);

        Map<Interfacetype, Interface> interfaces = new
                HashMap<Interfacetype, Interface>();
        interfaces.put(Interfacetype.WORKFLOW, newInterface(Interfacetype.WORKFLOW,
                "http://localhost:11000/oozie", "0.1"));
        String fsUrl = conf.get("fs.default.name");
        interfaces.put(Interfacetype.READONLY,
                newInterface(Interfacetype.READONLY, fsUrl, "0.1"));
        interfaces.put(Interfacetype.WRITE,
                newInterface(Interfacetype.WRITE, fsUrl, "0.1"));
        interfaces.put(Interfacetype.EXECUTE,
                newInterface(Interfacetype.EXECUTE,
                        conf.get("mapred.job.tracker"), "0.1"));
        interfaces.put(Interfacetype.MESSAGING,
                newInterface(Interfacetype.MESSAGING, "N/A", "0.1"));
        clusterEntity.setInterfaces(interfaces);

        Map<String, Location> locations = new HashMap<String, Location>();
        Location location = new Location();
        location.setName("staging");
        location.setPath("/workflow/staging");
        locations.put("staging", location);
        clusterEntity.setLocations(locations);

        clusterEntity.setProperties(new HashMap<String, Property>());
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
        if (mrCluster != null) mrCluster.shutdown();
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
