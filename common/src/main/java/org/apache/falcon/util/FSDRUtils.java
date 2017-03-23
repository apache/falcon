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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Util class for FS DR.
 */
public final class FSDRUtils {
    private FSDRUtils() {
    }

    private static final List<String> HDFS_SCHEME_PREFIXES =
            Arrays.asList("file", "hdfs", "hftp", "hsftp", "webhdfs", "swebhdfs");

    private static Configuration defaultConf = new Configuration();

    public static Configuration getDefaultConf() {
        return defaultConf;
    }

    public static void setDefaultConf(Configuration conf) {
        defaultConf = conf;
    }

    public static boolean isHCFS(Path filePath) throws FalconException {
        if (filePath == null) {
            throw new FalconException("filePath cannot be empty");
        }

        String scheme;
        try {
            FileSystem f = FileSystem.get(filePath.toUri(), getDefaultConf());
            scheme = f.getScheme();
            if (StringUtils.isBlank(scheme)) {
                throw new FalconException("Cannot get valid scheme for " + filePath);
            }
        } catch (IOException e) {
            throw new FalconException(e);
        }

        return HDFS_SCHEME_PREFIXES.contains(scheme.toLowerCase().trim()) ? false : true;
    }
}
