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
package org.apache.falcon.unit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.LocalJobRunner;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.protocol.ClientProtocolProvider;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Local ClientProtocol provider for Hadoop.
 */
public class LocalFalconClientProtocolProvider extends ClientProtocolProvider {

    private LocalJobRunner localJobRunner = null;
    private static final String UNIT = "unit";

    @Override
    public ClientProtocol create(Configuration conf) throws IOException {
        String framework = conf.get(MRConfig.FRAMEWORK_NAME, UNIT);
        if (!UNIT.equals(framework)) {
            return null;
        }
        return getLocalJobRunner(conf);
    }

    @Override
    public ClientProtocol create(InetSocketAddress inetSocketAddress, Configuration conf) throws IOException {
        return create(conf);
    }

    @Override
    public void close(ClientProtocol clientProtocol) throws IOException {

    }

    private synchronized LocalJobRunner getLocalJobRunner(Configuration conf) throws IOException {
        if (localJobRunner == null) {
            localJobRunner = new LocalJobRunner(conf);
        }
        return localJobRunner;
    }
}
