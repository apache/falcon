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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.File;
import java.io.IOException;

/**
 * Utility class to validate HDFS files.
 */
public final class FileUtils {

    public static final String DEFAULT_EVENT_STORE_PATH = StringUtils.removeEnd(DRStatusStore
            .BASE_DEFAULT_STORE_PATH,  File.separator) + File.separator + "Events" + File.separator;
    public static final FsPermission FS_PERMISSION_700 = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);
    public static final FsPermission DEFAULT_DIR_PERMISSION =
            new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.NONE);


    private FileUtils() {}

    public static Configuration getConfiguration(Configuration conf,
                                final String writeEP, final String nnKerberosPrincipal) throws IOException {
        Configuration newConf = new Configuration(conf);
        newConf.set("fs.defaultFS", writeEP);
        if (StringUtils.isNotEmpty(nnKerberosPrincipal)) {
            newConf.set("dfs.namenode.kerberos.principal", nnKerberosPrincipal);
        }
        return newConf;
    }

    public static void validatePath(final FileSystem fileSystem, final Path basePath) throws IOException {
        if (!fileSystem.exists(basePath)) {
            throw new IOException("Please create base dir " + fileSystem.getUri() + basePath
                    + ". Please set group to " + DRStatusStore.getStoreGroup()
                    + " and permissions to " + DRStatusStore.DEFAULT_STORE_PERMISSION.toString());
        }

        if (!fileSystem.getFileStatus(basePath).getPermission().equals(DRStatusStore.DEFAULT_STORE_PERMISSION)
                || !fileSystem.getFileStatus(basePath).getGroup().equalsIgnoreCase(DRStatusStore.getStoreGroup())) {
            throw new IOException("Base dir " + fileSystem.getUri() + basePath
                    + " does not have correct ownership/permissions."
                    + " Please set group to " + DRStatusStore.getStoreGroup()
                    + " and permissions to " + DRStatusStore.DEFAULT_STORE_PERMISSION.toString());
        }

    }
}
