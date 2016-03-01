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

package org.apache.falcon.cli.commands;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import static org.apache.falcon.cli.FalconCLI.validateNotEmpty;
import static org.apache.falcon.cli.FalconRecipeCLI.RECIPE_NAME;
import static org.apache.falcon.cli.FalconRecipeCLI.RECIPE_NAME_DESCRIPTION;
import static org.apache.falcon.cli.FalconRecipeCLI.RECIPE_OPERATION;
import static org.apache.falcon.cli.FalconRecipeCLI.RECIPE_OPERATION_DESCRIPTION;
import static org.apache.falcon.cli.FalconRecipeCLI.RECIPE_PROPERTIES_FILE;
import static org.apache.falcon.cli.FalconRecipeCLI.RECIPE_PROPERTIES_FILE_DESCRIPTION;
import static org.apache.falcon.cli.FalconRecipeCLI.RECIPE_TOOL_CLASS_NAME;
import static org.apache.falcon.cli.FalconRecipeCLI.RECIPE_TOOL_CLASS_NAME_DESCRIPTION;
import static org.apache.falcon.cli.FalconRecipeCLI.SKIPDRYRUN_OPT;
import static org.apache.falcon.cli.FalconRecipeCLI.SKIPDRYRUN_OPT_DESCRIPTION;
import static org.apache.falcon.cli.FalconRecipeCLI.validateRecipeOperations;
import static org.apache.falcon.cli.FalconRecipeCLI.validateRecipePropertiesFile;


/**
 * Instance commands.
 */
@Component
public class FalconRecipeCommands extends BaseFalconCommands {
    public static final String RECIPE_PREFIX = "recipe";
    public static final String RECIPE_COMMAND_PREFIX = RECIPE_PREFIX + " ";

    @CliCommand(value = {RECIPE_COMMAND_PREFIX})
    public String submitRecipe(
            @CliOption(key = {RECIPE_NAME}, mandatory = true, help = RECIPE_NAME_DESCRIPTION) final String recipeName,
            @CliOption(key = {RECIPE_TOOL_CLASS_NAME}, mandatory = true, help = RECIPE_TOOL_CLASS_NAME_DESCRIPTION)
            final String recipeToolClass,
            @CliOption(key = {RECIPE_OPERATION}, mandatory = true, help = RECIPE_OPERATION_DESCRIPTION)
            final String recipeOperation,
            @CliOption(key = {RECIPE_PROPERTIES_FILE}, mandatory = true, help = RECIPE_PROPERTIES_FILE_DESCRIPTION)
            final String recipePropertiesFile,
            @CliOption(key = {SKIPDRYRUN_OPT}, mandatory = false, specifiedDefaultValue = "true",
                    help = SKIPDRYRUN_OPT_DESCRIPTION) final Boolean skipDryRun
    ) {
        validateNotEmpty(recipeName, RECIPE_NAME);
        validateNotEmpty(recipeOperation, RECIPE_OPERATION);
        validateRecipeOperations(recipeOperation);
        validateRecipePropertiesFile(recipePropertiesFile, recipeName);
        return getFalconClient().submitRecipe(recipeName, recipeToolClass,
                recipeOperation, recipePropertiesFile, skipDryRun, getDoAs()).toString();
    }
}
