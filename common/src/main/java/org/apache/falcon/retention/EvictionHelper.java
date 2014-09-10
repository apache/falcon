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

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Helper methods to facilitate eviction.
 */

public final class EvictionHelper {

    private static final Logger LOG = LoggerFactory.getLogger(EvictionHelper.class);

    private static final String INSTANCEPATH_FORMAT = "instancePaths=";
    public static final String INSTANCEPATH_SEPARATOR = ",";


    private EvictionHelper() {}

    public static void logInstancePaths(final FileSystem logfs, final Path path,
                                        final String data) throws IOException {
        LOG.info("Writing deleted instances to path {}", path);
        OutputStream out = logfs.create(path);
        out.write(INSTANCEPATH_FORMAT.getBytes());
        out.write(data.getBytes());
        out.close();
        debug(logfs, path);
    }

    public static String[] getInstancePaths(final FileSystem fs,
                                            final Path logFile) throws FalconException {
        ByteArrayOutputStream writer = new ByteArrayOutputStream();
        try {
            InputStream date = fs.open(logFile);
            IOUtils.copyBytes(date, writer, 4096, true);
        } catch (IOException e) {
            throw new FalconException(e);
        }
        String logData = writer.toString();
        if (StringUtils.isEmpty(logData)) {
            throw new FalconException("csv file is empty");
        }

        String[] parts = logData.split(INSTANCEPATH_FORMAT);
        if (parts.length != 2) {
            throw new FalconException("Instance path in csv file not in required format: " + logData);
        }

        // part[0] is instancePaths=
        return parts[1].split(INSTANCEPATH_SEPARATOR);
    }

    private static void debug(final FileSystem fs, final Path outPath) throws IOException {
        ByteArrayOutputStream writer = new ByteArrayOutputStream();
        InputStream instance = fs.open(outPath);
        IOUtils.copyBytes(instance, writer, 4096, true);
        LOG.debug("Instance Paths copied to {}", outPath);
        LOG.debug("Written {}", writer);
    }
}
