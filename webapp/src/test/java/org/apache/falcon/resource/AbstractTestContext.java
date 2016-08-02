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

package org.apache.falcon.resource;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.File;
import java.io.IOException;

/**
 * Base class for context for unit and integration tests.
 */
public abstract class AbstractTestContext {

    public static final String FEED_TEMPLATE1 = "/feed-template1.xml";
    public static final String FEED_TEMPLATE2 = "/feed-template2.xml";
    public static final String FEED_TEMPLATE3 = "/feed-template3.xml";
    public static final String FEED_TEMPLATE5 = "/feed-template5.xml";
    public static final String FEED_EXPORT_TEMPLATE6 = "/feed-export-template6.xml";
    public static final String PROCESS_TEMPLATE = "/process-template.xml";

    protected static void mkdir(FileSystem fileSystem, Path path) throws Exception {
        if (!fileSystem.exists(path) && !fileSystem.mkdirs(path)) {
            throw new Exception("mkdir failed for " + path);
        }
    }

    protected static void mkdir(FileSystem fileSystem, Path path, FsPermission perm) throws Exception {
        if (!fileSystem.exists(path) && !fileSystem.mkdirs(path, perm)) {
            throw new Exception("mkdir failed for " + path);
        }
    }

    public static File getTempFile() throws IOException {
        return getTempFile("test", ".xml");
    }

    public static File getTempFile(String prefix, String suffix) throws IOException {
        return getTempFile("target", prefix, suffix);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static File getTempFile(String path, String prefix, String suffix) throws IOException {
        File f = new File(path);
        if (!f.exists()) {
            f.mkdirs();
        }

        return File.createTempFile(prefix, suffix, f);
    }

}
