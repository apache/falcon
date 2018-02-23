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

package org.apache.falcon.retention;

import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Unit test for EvictedInstanceSerDe.
 */
public class EvictedInstanceSerDeTest {

    private EmbeddedCluster cluster;
    private FileSystem fs;
    private Path csvFilePathWithNoContent;
    private Path csvFilePathWithContent;
    private StringBuffer evictedInstancePaths = new StringBuffer(
            "thrift://falcon-distcp-1.cs1cloud.internal:9083/default/retention_hours_7/year=2010")
            .append(EvictedInstanceSerDe.INSTANCEPATH_SEPARATOR)
            .append("thrift://falcon-distcp-1.cs1cloud.internal:9083/default/retention_hours_7/year=2011");

    @BeforeClass
    public void start() throws Exception {
        cluster = EmbeddedCluster.newCluster("test");
        String hdfsUrl = cluster.getConf().get(HadoopClientFactory.FS_DEFAULT_NAME_KEY);

        fs = FileSystem.get(cluster.getConf());
        csvFilePathWithNoContent = new Path(hdfsUrl + "/falcon/staging/feed/instancePaths-2014-10-01-01-00.csv");
        csvFilePathWithContent = new Path(hdfsUrl + "/falcon/staging/feed/instancePaths-2014-10-01-02-00.csv");
    }

    @AfterClass
    public void close() throws Exception {
        cluster.shutdown();
    }

    @Test
    public void testSerializeEvictedInstancePathsForNoEviction() throws Exception {
        EvictedInstanceSerDe.serializeEvictedInstancePaths(fs, csvFilePathWithNoContent, new StringBuffer());

        Assert.assertEquals(readLogFile(csvFilePathWithNoContent),
                EvictedInstanceSerDe.INSTANCEPATH_PREFIX);
    }

    @Test
    public void testSerializeEvictedInstancePathsWithEviction() throws Exception {
        EvictedInstanceSerDe.serializeEvictedInstancePaths(fs, csvFilePathWithContent, evictedInstancePaths);
        Assert.assertEquals(readLogFile(csvFilePathWithContent), evictedInstancePaths.toString());
    }

    @Test(dependsOnMethods = "testSerializeEvictedInstancePathsForNoEviction")
    public void testDeserializeEvictedInstancePathsForNoEviction() throws Exception {
        String[] instancePaths = EvictedInstanceSerDe.deserializeEvictedInstancePaths(fs, csvFilePathWithNoContent);
        Assert.assertEquals(instancePaths.length, 0);
    }

    @Test(dependsOnMethods = "testSerializeEvictedInstancePathsWithEviction")
    public void testDeserializeEvictedInstancePathsWithEviction() throws Exception {
        String[] instancePaths = EvictedInstanceSerDe.deserializeEvictedInstancePaths(fs, csvFilePathWithContent);
        Assert.assertEquals(instancePaths.length, 2);
        Assert.assertTrue(instancePaths[0].equals(
                "thrift://falcon-distcp-1.cs1cloud.internal:9083/default/retention_hours_7/year=2010"));
        Assert.assertTrue(instancePaths[1].equals(
                "thrift://falcon-distcp-1.cs1cloud.internal:9083/default/retention_hours_7/year=2011"));

    }

    private String readLogFile(Path logFile) throws IOException {
        ByteArrayOutputStream writer = new ByteArrayOutputStream();
        InputStream date = fs.open(logFile);
        IOUtils.copyBytes(date, writer, 4096, true);
        return writer.toString();
    }
}
