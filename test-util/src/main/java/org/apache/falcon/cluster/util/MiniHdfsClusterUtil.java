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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import java.io.File;

/**
 * Create a local MiniDFS cluster for testing snapshots et al.
 */
public final class MiniHdfsClusterUtil {

    private MiniHdfsClusterUtil() {}

    public static final int EXTENSION_TEST_PORT = 54134;
    public static final int SNAPSHOT_EVICTION_TEST_PORT = 54135;
    public static final int SNAPSHOT_REPL_TEST_PORT = 54136;


    public static MiniDFSCluster initMiniDfs(int port, File baseDir) throws Exception {
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        builder.nameNodePort(port);
        return builder.build();
    }

    public static void cleanupDfs(MiniDFSCluster miniDFSCluster, File baseDir) throws Exception {
        miniDFSCluster.shutdown();
        FileUtil.fullyDelete(baseDir);
    }

}
