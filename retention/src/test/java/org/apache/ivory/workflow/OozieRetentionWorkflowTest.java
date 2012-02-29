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

package org.apache.ivory.workflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ivory.cluster.util.EmbeddedCluster;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.util.StartupProperties;
import org.apache.ivory.workflow.engine.OozieWorkflowEngine;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.local.LocalOozie;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class OozieRetentionWorkflowTest {

    private static Logger LOG = Logger.getLogger(OozieRetentionWorkflowTest.class);

    private EmbeddedCluster cluster;

    //@BeforeClass
    public void start() throws Exception{
        cluster = EmbeddedCluster.newCluster("eviction", true, "hdfs");
    }

    //@AfterClass
    public void close() throws Exception {
        cluster.shutdown();
    }

    private void startOozie() throws Exception {
        String oozieHome = System.getProperty("user.dir") + "/retention/src/test/resources";
        if (!new File(oozieHome).exists()) {
            oozieHome = System.getProperty("user.dir") + "/src/test/resources";
        }
        System.setProperty("oozie.home.dir", oozieHome);
        LocalOozie.start();
    }

    //@Test
    public void test() throws Exception {
        UserGroupInformation hdfUser = UserGroupInformation.createRemoteUser("hdfs");
        hdfUser.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
                startOozie();
                try {
                    testInternal();
                } finally {
                    LocalOozie.stop();
                }
                return null;
            }
        });
    }

    private void testInternal() throws Exception {
        OozieClient client = LocalOozie.getClient("oozie");
        Properties properties = new Properties();
        properties.put(OozieWorkflowEngine.NAME_NODE, ClusterHelper.
                getHdfsUrl(cluster.getCluster()));
        properties.put(OozieWorkflowEngine.JOB_TRACKER, ClusterHelper.
                getMREndPoint(cluster.getCluster()));
        properties.put("queueName", "default");
        properties.put("feedDataPath", "/data/feed1/${YEAR}-${MONTH}-${DAY}-${HOUR}");
        properties.put("limit", "hours(5)");
        properties.put("timeZone", "UTC");
        properties.put("frequency", "hourly");
        properties.put(OozieClient.USER_NAME, "guest");
        properties.put(OozieClient.GROUP_NAME, "users");
        properties.put(OozieClient.APP_PATH, "${nameNode}/ivory/workflow");
        properties.put(OozieClient.LIBPATH, "${nameNode}/ivory/workflow/lib");
        stageSystemFiles();
        createTestData();
        System.out.println(client.run(properties));
        Thread.sleep(100000);
    }

    private void stageSystemFiles() throws Exception {
        Configuration conf = cluster.getConf();
        FileSystem fs = FileSystem.get(conf);
        File systemLoc = new File(System.getProperty("user.dir") + "/retention/target");

        if (!systemLoc.exists()) {
            LOG.info(systemLoc + " is not found");
            systemLoc = new File(System.getProperty("user.dir") + "/target");
        }

        if (!systemLoc.exists()) {
            LOG.info(systemLoc + " is not found");
            systemLoc = new File(StartupProperties.get().
                     getProperty("system.lib.location"));
        }

        Assert.assertTrue(systemLoc.exists(), systemLoc.getAbsolutePath());

        fs.copyFromLocalFile(new Path(systemLoc.getAbsolutePath(),
                "ivory-common-0.1-SNAPSHOT.jar"), new Path("/ivory/workflow/lib"));
        fs.copyFromLocalFile(new Path(systemLoc.getAbsolutePath(),
                "ivory-retention-0.1-SNAPSHOT.jar"), new Path("/ivory/workflow/lib"));
        fs.copyFromLocalFile(new Path(systemLoc.getAbsolutePath(),
                "ivory-oozie-adaptor-0.1-SNAPSHOT.jar"), new Path("/ivory/workflow/lib"));

        InputStream in = getClass().getResourceAsStream("/retention-workflow.xml");
        Assert.assertNotNull(in, "retention-workflow.xml is not found in cp");
        OutputStream out = fs.create(new Path("/ivory/workflow/workflow.xml"));
        IOUtils.copyBytes(in, out, 4096, true);
    }

    private List<Path> createTestData() throws IOException {
        List<Path> list = new ArrayList<Path>();
        Configuration conf = cluster.getConf();
        FileSystem fs = FileSystem.get(conf);
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd-HH");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date date = new Date(System.currentTimeMillis() + 3 * 86400000L);
        Path path = new Path("/data/feed1/" + formatter.format(date) + "/file");
        fs.create(path).close();
        date = new Date(date.getTime() - 86400000L);
        path = new Path("/data/feed1/" + formatter.format(date) + "/file");
        fs.create(path).close();
        date = new Date(date.getTime() - 86400000L);
        path = new Path("/data/feed1/" + formatter.format(date) + "/file");
        fs.create(path).close();
        date = new Date(date.getTime() - 86400000L);
        path = new Path("/data/feed1/" + formatter.format(date) + "/file");
        list.add(path);
        fs.create(path).close();
        date = new Date(date.getTime() - 86400000L);
        path = new Path("/data/feed1/" + formatter.format(date) + "/file");
        list.add(path);
        fs.create(path).close();
        date = new Date(date.getTime() - 86400000L);
        path = new Path("/data/feed1/" + formatter.format(date) + "/file");
        list.add(path);
        fs.create(path).close();
        date = new Date(date.getTime() - 86400000L);
        path = new Path("/data/feed1/" + formatter.format(date) + "/file");
        list.add(path);
        fs.create(path).close();
        date = new Date(date.getTime() - 86400000L);
        path = new Path("/data/feed1/" + formatter.format(date) + "/file");
        list.add(path);
        fs.create(path).close();
        date = new Date(date.getTime() - 86400000L);
        path = new Path("/data/feed1/" + formatter.format(date) + "/file");
        list.add(path);
        fs.create(path).close();
        date = new Date(date.getTime() - 86400000L);
        path = new Path("/data/feed1/" + formatter.format(date) + "/file");
        list.add(path);
        fs.create(path).close();
        fs.setOwner(new Path("/data/feed1"), "guest", "users");
        for (FileStatus file : fs.globStatus(new Path("/data/feed1/*"))) {
            fs.setOwner(file.getPath(), "guest", "users");
        }
        return list;
    }
}
