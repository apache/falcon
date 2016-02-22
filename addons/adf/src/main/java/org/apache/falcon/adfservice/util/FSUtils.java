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

package org.apache.falcon.adfservice.util;

import org.apache.commons.io.IOUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URISyntaxException;

/**
 * Utility for file operations.
 */
public final class FSUtils {
    private static final Logger LOG = LoggerFactory.getLogger(FSUtils.class);
    private FSUtils() {
    }

    public static String readHDFSFile(final String filePath, final String fileName)
        throws URISyntaxException, FalconException {
        BufferedReader br = null;
        try {
            Path path = new Path(filePath, fileName);
            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(path.toUri());
            br = new BufferedReader(new InputStreamReader(fs.open(path)));
            StringBuilder fileContent = new StringBuilder();
            String line;
            while (true) {
                line = br.readLine();
                if (line == null) {
                    break;
                }
                fileContent.append(line);
            }
            return fileContent.toString();
        } catch (IOException e) {
            throw new FalconException("Error reading file from hdfs: " + filePath + fileName + ": " + e.toString(), e);
        } finally {
            IOUtils.closeQuietly(br);
        }
    }

    public static String createFile(final Path path,
                                    final String content) throws FalconException {
        OutputStream out = null;
        try {
            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(path.toUri());
            out = fs.create(path);
            out.write(content.getBytes());
        } catch (IOException e) {
            throw new FalconException("Error preparing script file: " + path, e);
        } finally {
            IOUtils.closeQuietly(out);
        }
        return path.toString();
    }

    public static void createDir(final Path dirPath) throws FalconException {
        FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(dirPath.toUri());
        try {
            if (!fs.exists(dirPath)) {
                LOG.info("Creating directory: {}", dirPath);
                HadoopClientFactory.mkdirsWithDefaultPerms(fs, dirPath);
            }
        } catch (IOException e) {
            throw new FalconException("Error creating directory: " + dirPath, e);
        }
    }

    public static void removeDir(final Path dirPath) throws FalconException {
        FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(dirPath.toUri());
        try {
            fs.delete(dirPath, true);
        } catch (IOException e) {
            throw new FalconException("Error creating directory: " + dirPath, e);
        }
    }
}
