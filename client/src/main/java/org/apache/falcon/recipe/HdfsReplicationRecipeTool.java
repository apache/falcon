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

package org.apache.falcon.recipe;

import org.apache.commons.lang3.StringUtils;

import java.util.Properties;
import java.io.File;

/**
 * Hdfs Replication recipe tool for Falcon recipes.
 */
public class HdfsReplicationRecipeTool implements Recipe {

    private static final String COMMA_SEPARATOR = ",";

    @Override
    public void validate(final Properties recipeProperties) {
        for (HdfsReplicationRecipeToolOptions option : HdfsReplicationRecipeToolOptions.values()) {
            if (recipeProperties.getProperty(option.getName()) == null && option.isRequired()) {
                throw new IllegalArgumentException("Missing argument: " + option.getName());
            }
        }
    }

    @Override
    public Properties getAdditionalSystemProperties(final Properties recipeProperties) {
        Properties additionalProperties = new Properties();

        // Construct fully qualified hdfs src path
        String srcPaths = recipeProperties.getProperty(HdfsReplicationRecipeToolOptions
                .REPLICATION_SOURCE_DIR.getName());
        StringBuilder absoluteSrcPaths = new StringBuilder();
        String srcFsPath = recipeProperties.getProperty(
                HdfsReplicationRecipeToolOptions.REPLICATION_SOURCE_CLUSTER_FS_WRITE_ENDPOINT.getName());
        if (StringUtils.isNotEmpty(srcFsPath)) {
            srcFsPath = StringUtils.removeEnd(srcFsPath, File.separator);
        }
        if (StringUtils.isNotEmpty(srcPaths)) {
            String[] paths = srcPaths.split(COMMA_SEPARATOR);

            for (String path : paths) {
                StringBuilder srcpath = new StringBuilder(srcFsPath);
                srcpath.append(path.trim());
                srcpath.append(COMMA_SEPARATOR);
                absoluteSrcPaths.append(srcpath);
            }
        }

        additionalProperties.put(HdfsReplicationRecipeToolOptions.REPLICATION_SOURCE_DIR.getName(),
                StringUtils.removeEnd(absoluteSrcPaths.toString(), COMMA_SEPARATOR));
        return additionalProperties;
    }
}
