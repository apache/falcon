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
package org.apache.falcon.repliation;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.falcon.replication.FeedReplicator;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class FeedReplicatorTest {

    @Test
    public void testArguments() throws Exception {
        /*
         * <arg>-update</arg>
         * <arg>-blocking</arg><arg>true</arg> <arg>-maxMaps</arg><arg>20</arg>
         * <arg>-sourcePaths</arg><arg>${distcpSourcePaths}</arg>
         * <arg>-targetPath</arg><arg>${distcpTargetPaths}</arg>
         */
        FeedReplicator replicator = new FeedReplicator();
        DistCpOptions options = replicator.getDistCpOptions(new String[] { "true", "-maxMaps", "5", "-sourcePaths",
                "hdfs://localhost:8020/tmp/", "-targetPath",
                "hdfs://localhost1:8020/tmp/" });

        List<Path> srcPaths = new ArrayList<Path>();
        srcPaths.add(new Path("hdfs://localhost:8020/tmp/"));
        Assert.assertEquals(options.getSourcePaths(), srcPaths);
        Assert.assertEquals(options.getTargetPath(), new Path("hdfs://localhost1:8020/tmp/"));
    }
}
