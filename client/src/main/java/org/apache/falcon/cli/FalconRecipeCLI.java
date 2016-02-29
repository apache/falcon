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

package org.apache.falcon.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.client.FalconCLIException;
import org.apache.falcon.client.FalconClient;

import java.util.HashSet;
import java.util.Set;

/**
 * Recipe extension to Falcon Command Line Interface - wraps the RESTful API for Recipe.
 */
public class FalconRecipeCLI extends FalconCLI {

    public FalconRecipeCLI() throws Exception {
        super();
    }

    public static final String RECIPE_NAME = "name";
    public static final String RECIPE_OPERATION= "operation";
    public static final String RECIPE_TOOL_CLASS_NAME = "tool";
    public static final String RECIPE_PROPERTIES_FILE = "properties";
    public static final String URL_OPTION_DESCRIPTION = "Falcon URL";
    public static final String RECIPE_NAME_DESCRIPTION = "recipe name";
    public static final String RECIPE_TOOL_CLASS_NAME_DESCRIPTION = "recipe class";
    public static final String RECIPE_OPERATION_DESCRIPTION = "recipe operation";
    public static final String RECIPE_PROPERTIES_FILE_DESCRIPTION = "recipe properties file path";
    public static final String SKIPDRYRUN_OPT_DESCRIPTION = "skip dryrun operation";
    public static final String DO_AS_OPT_DESCRIPTION = "doAs user";

    public Options createRecipeOptions() {
        Options recipeOptions = new Options();
        Option url = new Option(URL_OPTION, true, URL_OPTION_DESCRIPTION);
        recipeOptions.addOption(url);

        Option recipeFileOpt = new Option(RECIPE_NAME, true, RECIPE_NAME_DESCRIPTION);
        recipeOptions.addOption(recipeFileOpt);

        Option recipeToolClassName = new Option(RECIPE_TOOL_CLASS_NAME, true, RECIPE_TOOL_CLASS_NAME_DESCRIPTION);
        recipeOptions.addOption(recipeToolClassName);

        Option recipeOperation = new Option(RECIPE_OPERATION, true, RECIPE_OPERATION_DESCRIPTION);
        recipeOptions.addOption(recipeOperation);

        Option recipeProperties = new Option(RECIPE_PROPERTIES_FILE, true, RECIPE_PROPERTIES_FILE_DESCRIPTION);
        recipeOptions.addOption(recipeProperties);

        Option skipDryRunOperation = new Option(SKIPDRYRUN_OPT, false, SKIPDRYRUN_OPT_DESCRIPTION);
        recipeOptions.addOption(skipDryRunOperation);

        Option doAs = new Option(DO_AS_OPT, true, DO_AS_OPT_DESCRIPTION);
        recipeOptions.addOption(doAs);

        return recipeOptions;
    }

    public void recipeCommand(CommandLine commandLine, FalconClient client) {
        Set<String> optionsList = new HashSet<String>();
        for (Option option : commandLine.getOptions()) {
            optionsList.add(option.getOpt());
        }

        String recipeName = commandLine.getOptionValue(RECIPE_NAME);
        String recipeToolClass = commandLine.getOptionValue(RECIPE_TOOL_CLASS_NAME);
        String recipeOperation = commandLine.getOptionValue(RECIPE_OPERATION);
        String recipePropertiesFile = commandLine.getOptionValue(RECIPE_PROPERTIES_FILE);
        String doAsUser = commandLine.getOptionValue(DO_AS_OPT);

        validateNotEmpty(recipeName, RECIPE_NAME);
        validateNotEmpty(recipeOperation, RECIPE_OPERATION);
        validateRecipeOperations(recipeOperation);
        validateRecipePropertiesFile(recipePropertiesFile, recipeName);
        Boolean skipDryRun = null;
        if (optionsList.contains(SKIPDRYRUN_OPT)) {
            skipDryRun = true;
        }

        String result = client.submitRecipe(recipeName, recipeToolClass,
                recipeOperation, recipePropertiesFile, skipDryRun, doAsUser).toString();
        OUT.get().println(result);
    }

    public static void validateRecipeOperations(String recipeOperation) {
        for(RecipeOperation operation : RecipeOperation.values()) {
            if (operation.toString().equalsIgnoreCase(recipeOperation)) {
                return;
            }
        }
        throw new FalconCLIException("Allowed Recipe operations: "
                + java.util.Arrays.asList((RecipeOperation.values())));
    }

    public static void validateRecipePropertiesFile(String recipePropertiesFile, String recipeName) {
        if (StringUtils.isBlank(recipePropertiesFile)) {
            return;
        }

        String []fileSplits = recipePropertiesFile.split("/");
        String recipePropertiesName = (fileSplits[fileSplits.length-1]).split("\\.")[0];
        if (recipePropertiesName.equals(recipeName)) {
            return;
        }

        throw new FalconCLIException("Provided properties file name do match with recipe name: " +recipeName);
    }
}
