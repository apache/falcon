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

package org.apache.falcon.hive.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.job.JobCounters;
import org.apache.falcon.job.JobCountersHandler;
import org.apache.falcon.job.JobType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Hive replication utility class.
 */
public final class HiveDRUtils {
    /**
     * Enum for Hive replication type.
     */
    public enum ReplicationType {
        TABLE,
        DB
    }

    /**
     * Enum for hive-dr action type.
     */
    public enum ExecutionStage {
        IMPORT,
        EXPORT,
        LASTEVENTS
    }

    private static final String ALL_TABLES = "*";

    public static final String SEPARATOR = File.separator;

    private static final Logger LOG = LoggerFactory.getLogger(HiveDRUtils.class);

    private HiveDRUtils() {}

    public static ReplicationType getReplicationType(List<String> sourceTables) {
        return (sourceTables.size() == 1 && sourceTables.get(0).equals(ALL_TABLES)) ? ReplicationType.DB
                : ReplicationType.TABLE;
    }

    public static Configuration getDefaultConf() throws IOException {
        Configuration conf = new Configuration();

        if (System.getProperty("oozie.action.conf.xml") != null) {
            Path confPath = new Path("file:///", System.getProperty("oozie.action.conf.xml"));

            final boolean actionConfExists = confPath.getFileSystem(conf).exists(confPath);
            LOG.info("Oozie Action conf {} found ? {}", confPath, actionConfExists);
            if (actionConfExists) {
                LOG.info("Oozie Action conf found, adding path={}, conf={}", confPath, conf.toString());
                conf.addResource(confPath);
            }
        }

        String tokenFile = System.getenv("HADOOP_TOKEN_FILE_LOCATION");
        if (StringUtils.isNotBlank(tokenFile)) {
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

        return conf;
    }

    public static String getFilePathFromEnv(String env) {
        String path = System.getenv(env);
        if (path != null && Shell.WINDOWS) {
            // In Windows, file paths are enclosed in \" so remove them here
            // to avoid path errors
            if (path.charAt(0) == '"') {
                path = path.substring(1);
            }
            if (path.charAt(path.length() - 1) == '"') {
                path = path.substring(0, path.length() - 1);
            }
        }
        return path;
    }

    public static Map<String, Long> fetchReplicationCounters(Configuration conf,
                                                             Job job) throws IOException, InterruptedException {
        JobCounters hiveReplicationCounters = JobCountersHandler.getCountersType(
                JobType.HIVEREPLICATION.name());
        hiveReplicationCounters.obtainJobCounters(conf, job, true);
        return hiveReplicationCounters.getCountersMap();
    }
}
