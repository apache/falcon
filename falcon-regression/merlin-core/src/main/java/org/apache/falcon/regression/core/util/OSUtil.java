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

import org.apache.commons.io.FilenameUtils;

/**
 * Util methods related to OS.
 */
public final class OSUtil {
    private OSUtil() {
        throw new AssertionError("Instantiating utility class...");
    }

    public static final boolean IS_WINDOWS = System.getProperty("os.name").toLowerCase().startsWith("windows");
    public static final String WIN_SU_BINARY =
            Config.getProperty("windows.su.binary", "ExecuteAs.exe");

    private static final String SEPARATOR = System.getProperty("file.separator", "/");

    public static final String RESOURCES = concat("src", "test", "resources");
    public static final String RESOURCES_OOZIE = concat(RESOURCES, "oozie");
    public static final String OOZIE_EXAMPLE_INPUT_DATA = concat(RESOURCES, "OozieExampleInputData");
    public static final String NORMAL_INPUT = concat(OOZIE_EXAMPLE_INPUT_DATA, "normalInput");
    public static final String SINGLE_FILE = concat(OOZIE_EXAMPLE_INPUT_DATA, "SingleFile");
    public static final String OOZIE_COMBINED_ACTIONS = concat(RESOURCES, "combinedWorkflow");

    public static final String OOZIE_LIB_FOLDER = concat(RESOURCES, "oozieLib");
    public static final String MULTIPLE_ACTION_WORKFLOW = concat(RESOURCES, "MultipleActionWorkflow");
    public static final String PIG_DIR = concat(RESOURCES, "pig");


    public static String concat(String path1, String path2, String... pathParts) {
        String path = FilenameUtils.concat(path1, path2);
        for (String pathPart : pathParts) {
            path = FilenameUtils.concat(path, pathPart);
        }
        return path;
    }
}
