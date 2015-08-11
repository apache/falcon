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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;

/**
 * Recipe tool args.
 */
public enum RecipeToolArgs {
    RECIPE_FILE_ARG("file", "recipe template file path"),
    RECIPE_PROPERTIES_FILE_ARG("propertiesFile", "recipe properties file path"),
    RECIPE_PROCESS_XML_FILE_PATH_ARG(
            "recipeProcessFilePath", "file path of recipe process to be submitted"),
    RECIPE_OPERATION_ARG("recipeOperation", "recipe operation");

    private final String name;
    private final String description;
    private final boolean isRequired;
    RecipeToolArgs(String name, String description) {
        this(name, description, true);
    }

    RecipeToolArgs(String name, String description, boolean isRequired) {
        this.name = name;
        this.description = description;
        this.isRequired = isRequired;
    }

    public Option getOption() {
        return new Option(this.name, true, this.description);
    }

    public String getName() {
        return this.name;
    }

    public String getDescription() {
        return description;
    }

    public boolean isRequired() {
        return isRequired;
    }

    public String getOptionValue(CommandLine cmd) {
        return cmd.getOptionValue(this.name);
    }

    @Override
    public String toString() {
        return getName();
    }
}
