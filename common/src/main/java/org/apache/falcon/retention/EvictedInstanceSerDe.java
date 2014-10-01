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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Utility class for serializing and deserializing the evicted instance paths.
 */

public final class EvictedInstanceSerDe {

    private static final Logger LOG = LoggerFactory.getLogger(EvictedInstanceSerDe.class);

    public static final String INSTANCEPATH_PREFIX = "instancePaths=";
    public static final String INSTANCEPATH_SEPARATOR = ",";


    private EvictedInstanceSerDe() {}

    /**
     * This method serializes the evicted instances to a file in logs dir for a given feed.
     * @see org.apache.falcon.retention.FeedEvictor
     *
     * *Note:* This is executed with in the map task for evictor action
     *
     * @param fileSystem file system handle
     * @param logFilePath       File path to serialize the instances to
     * @param instances  list of instances, comma separated
     * @throws IOException
     */
    public static void serializeEvictedInstancePaths(final FileSystem fileSystem,
                                                     final Path logFilePath,
                                                     StringBuffer instances) throws IOException {
        LOG.info("Writing deleted instances {} to path {}", instances, logFilePath);
        OutputStream out = null;
        try {
            out = fileSystem.create(logFilePath);
            instances.insert(0, INSTANCEPATH_PREFIX); // add the prefix
            out.write(instances.toString().getBytes());

            // To make sure log cleaning service can delete this file
            FsPermission permission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
            fileSystem.setPermission(logFilePath, permission);
        } finally {
            if (out != null) {
                out.close();
            }
        }

        if (LOG.isDebugEnabled()) {
            logEvictedInstancePaths(fileSystem, logFilePath);
        }
    }

    private static void logEvictedInstancePaths(final FileSystem fs,
                                                final Path outPath) throws IOException {
        ByteArrayOutputStream writer = new ByteArrayOutputStream();
        InputStream instance = fs.open(outPath);
        IOUtils.copyBytes(instance, writer, 4096, true);
        LOG.debug("Instance Paths copied to {}", outPath);
        LOG.debug("Written {}", writer);
    }

    /**
     * This method deserializes the evicted instances from a log file on hdfs.
     * @see org.apache.falcon.messaging.JMSMessageProducer
     * *Note:* This is executed with in the falcon server
     *
     * @param fileSystem file system handle
     * @param logFile    File path to serialize the instances to
     * @return list of instances, comma separated
     * @throws IOException
     */
    public static String[] deserializeEvictedInstancePaths(final FileSystem fileSystem,
                                                           final Path logFile) throws IOException {
        ByteArrayOutputStream writer = new ByteArrayOutputStream();
        InputStream instance = fileSystem.open(logFile);
        IOUtils.copyBytes(instance, writer, 4096, true);
        String[] instancePaths = writer.toString().split(INSTANCEPATH_PREFIX);

        if (instancePaths.length <= 1) {
            LOG.info("Returning 0 instance paths for feed ");
            return new String[0];
        } else {
            LOG.info("Returning instance paths for feed {}", instancePaths[1]);
            return instancePaths[1].split(INSTANCEPATH_SEPARATOR);
        }
    }
}
