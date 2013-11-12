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

package org.apache.falcon.util;


import org.apache.commons.io.IOUtils;
import org.apache.falcon.resource.TestContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * File System Utility class for integration-tests.
 */
public final class FSUtils {

    private FSUtils() {
    }

    public static void copyOozieShareLibsToHDFS(String shareLibLocalPath, String shareLibHdfsPath)
        throws IOException {
        File shareLibDir = new File(shareLibLocalPath);
        if (!shareLibDir.exists()) {
            throw new IllegalArgumentException("Sharelibs dir must exist for tests to run.");
        }

        File[] jarFiles = shareLibDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.isFile() && file.getName().endsWith(".jar");
            }
        });

        for (File jarFile : jarFiles) {
            copyFileToHDFS(jarFile, shareLibHdfsPath);
        }
    }

    public static void copyFileToHDFS(File jarFile, String shareLibHdfsPath) throws IOException {
        System.out.println("Copying jarFile = " + jarFile);
        Path shareLibPath = new Path(shareLibHdfsPath);
        FileSystem fs = shareLibPath.getFileSystem(new Configuration());
        if (!fs.exists(shareLibPath)) {
            fs.mkdirs(shareLibPath);
        }

        OutputStream os = null;
        InputStream is = null;
        try {
            os = fs.create(new Path(shareLibPath, jarFile.getName()));
            is = new FileInputStream(jarFile);
            IOUtils.copy(is, os);
        } finally {
            IOUtils.closeQuietly(is);
            IOUtils.closeQuietly(os);
        }
    }

    public static void copyResourceToHDFS(String localResource, String resourceName, String hdfsPath)
        throws IOException {
        Path appPath = new Path(hdfsPath);
        FileSystem fs = appPath.getFileSystem(new Configuration());
        if (!fs.exists(appPath)) {
            fs.mkdirs(appPath);
        }

        OutputStream os = null;
        InputStream is = null;
        try {
            os = fs.create(new Path(appPath, resourceName));
            is = TestContext.class.getResourceAsStream(localResource);
            IOUtils.copy(is, os);
        } finally {
            IOUtils.closeQuietly(is);
            IOUtils.closeQuietly(os);
        }
    }
}
