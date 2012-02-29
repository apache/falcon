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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import java.util.Properties;

public class OozieRetentionWorkflowTest {

    private static Logger LOG = Logger.getLogger(OozieRetentionWorkflowTest.class);

    private EmbeddedCluster cluster;

    @BeforeClass
    public void start() throws Exception{
        cluster = EmbeddedCluster.newCluster("eviction", true);
        LocalOozie.start();
    }

    @AfterClass
    public void close() throws Exception {
        cluster.shutdown();
        LocalOozie.stop();
    }

    //@Test
    public void test() throws Exception {
        OozieClient client = LocalOozie.getClient();
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
        properties.put(OozieClient.APP_PATH, "/ivory/workflow");
        properties.put(OozieClient.LIBPATH, "/ivory/workflow/lib");
        stageSystemFiles();
        createTestData();
        client.run(properties);
    }

    private void stageSystemFiles() throws Exception {
        Configuration conf = cluster.getConf();
        FileSystem fs = FileSystem.get(conf);
        File systemLoc = new File(System.getProperty("user.dir") +
                "/webapps/target/ivory-webapp-0.1-SNAPSHOT/WEB-INF/lib");

        if (!systemLoc.exists()) {
            systemLoc = new File(System.getProperty("user.dir") +
                "../webapps/target/ivory-webapp-0.1-SNAPSHOT/WEB-INF/lib");
        }

        if (!systemLoc.exists()) {
             systemLoc = new File(StartupProperties.get().
                     getProperty("system.lib.location"));
        }

        Assert.assertTrue(systemLoc.exists());

        fs.copyFromLocalFile(new Path(systemLoc.getAbsolutePath(),
                "ivory-common-0.1-SNAPSHOT.jar"), new Path("/ivory/workflow/lib"));
        fs.copyFromLocalFile(new Path(systemLoc.getAbsolutePath(),
                "ivory-retention-0.1-SNAPSHOT.jar"), new Path("/ivory/workflow/lib"));
        fs.copyFromLocalFile(new Path(systemLoc.getAbsolutePath(),
                "ivory-oozie-adaptor-0.1-SNAPSHOT.jar"), new Path("/ivory/workflow/lib"));


    }

    private void createTestData() {

    }


}
