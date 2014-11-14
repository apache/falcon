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

package org.apache.falcon.workflow.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;

/**
 * Utility to read oozie action conf at oozie.action.conf.xml.
 */
public final class OozieActionConfigurationHelper {

    private static final Logger LOG = LoggerFactory.getLogger(OozieActionConfigurationHelper.class);

    private OozieActionConfigurationHelper() {
    }

    public static Configuration createActionConf() throws IOException {
        Configuration conf = new Configuration();
        Path confPath = new Path("file:///" + System.getProperty("oozie.action.conf.xml"));

        final boolean actionConfExists = confPath.getFileSystem(conf).exists(confPath);
        LOG.info("Oozie Action conf {} found ? {}", confPath, actionConfExists);
        if (actionConfExists) {
            LOG.info("Oozie Action conf found, adding path={}, conf={}", confPath, conf.toString());
            conf.addResource(confPath);
            dumpConf(conf, "oozie action conf ");
        }

        String tokenFile = System.getenv("HADOOP_TOKEN_FILE_LOCATION");
        if (tokenFile != null) {
            if (Shell.WINDOWS) {
                if (tokenFile.charAt(0) == '"') {
                    tokenFile = tokenFile.substring(1);
                }
                if (tokenFile.charAt(tokenFile.length() - 1) == '"') {
                    tokenFile = tokenFile.substring(0, tokenFile.length() - 1);
                }
            }

            conf.set("mapreduce.job.credentials.binary", tokenFile);
            System.setProperty("mapreduce.job.credentials.binary", tokenFile);
            conf.set("tez.credentials.path", tokenFile);
            System.setProperty("tez.credentials.path", tokenFile);
        }

        conf.set("datanucleus.plugin.pluginRegistryBundleCheck", "LOG");
        conf.setBoolean("hive.exec.mode.local.auto", false);

        return conf;
    }

    public static void dumpConf(Configuration conf, String message) throws IOException {
        StringWriter writer = new StringWriter();
        Configuration.dumpConfiguration(conf, writer);
        LOG.info(message + " {}", writer);
    }
}
