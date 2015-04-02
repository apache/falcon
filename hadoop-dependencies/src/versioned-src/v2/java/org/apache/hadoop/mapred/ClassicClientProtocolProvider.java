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
package org.apache.hadoop.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.protocol.ClientProtocolProvider;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Classic protocol provider for Hadoop v2 based tests.
 */
public class ClassicClientProtocolProvider extends ClientProtocolProvider {

    private static final String LOCALHOST = "localhost";

    private static final ConcurrentHashMap<String, LocalJobRunner> CACHE = new ConcurrentHashMap<String, LocalJobRunner>();

    private boolean initialised = false;

    @Override
    public ClientProtocol create(Configuration conf) throws IOException {
        String framework = conf.get(MRConfig.FRAMEWORK_NAME, "unittests");
        String tracker = conf.get("mapreduce.jobtracker.address",
                conf.get("yarn.resourcemanager.address", LOCALHOST));
        if (!"unittests".equals(framework) || !tracker.startsWith(LOCALHOST)) {
            return null;
        }

        if (!CACHE.containsKey(tracker)) {
            CACHE.putIfAbsent(tracker, new LocalJobRunner(conf));
        }

        if (!initialised) {
            System.setOut(new PrintStream(new BufferedOutputStream(new FileOutputStream("target/logs/system-out.log")), true));
            initialised = true;
        }
        return CACHE.get(tracker);
    }

    @Override
    public ClientProtocol create(InetSocketAddress addr, Configuration conf) throws IOException {
        return create(conf);
    }

    @Override
    public void close(ClientProtocol clientProtocol) throws IOException {
    }
}
