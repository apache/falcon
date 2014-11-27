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

package org.apache.falcon.regression.core.util;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
/**
 * Util class for local files.
 */
public final class FileUtil {
    private static final Logger LOGGER = Logger.getLogger(FileUtil.class);
    private FileUtil() {
    }

    /**
     * Writes an entity to a file and returns the filename.
     * @param entity to be written
     * @return name of the file
     * @throws IOException
     */
    public static String writeEntityToFile(String entity) throws IOException {
        final String entityName = Util.readEntityName(entity);
        final File entityFile = new File(entityName + ".xml");
        LOGGER.info("attempting to write: " + entityName + " at location "
            + entityFile.getAbsolutePath());
        FileUtils.write(entityFile, entity);
        return entityFile.getAbsolutePath();
    }
}
