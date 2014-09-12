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
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.cli.Options;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStream;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base recipe tool for Falcon recipes.
 */
public class RecipeTool extends Configured implements Tool {
    private static final String HDFS_WF_PATH = "falcon" + File.separator + "recipes" + File.separator;
    private static final String RECIPE_PREFIX = "falcon.recipe.";
    private static final Pattern RECIPE_VAR_PATTERN = Pattern.compile("##[A-Za-z0-9_.]*##");

    private FileSystem hdfsFileSystem;

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new RecipeTool(), args);
    }

    @Override
    public int run(String[] arguments) throws Exception {
        Map<RecipeToolArgs, String> argMap = setupArgs(arguments);
        if (argMap == null || argMap.isEmpty()) {
            throw new Exception("Arguments passed to recipe is null");
        }

        String recipePropertiesFilePath = argMap.get(RecipeToolArgs.RECIPE_PROPERTIES_FILE_ARG);
        Properties recipeProperties = loadProperties(recipePropertiesFilePath);
        validateProperties(recipeProperties);

        validateArtifacts(recipeProperties);
        String recipeName = FilenameUtils.getBaseName(recipePropertiesFilePath);

        FileSystem fs = getFileSystemForHdfs(recipeProperties);
        copyFilesToHdfsIfRequired(recipeProperties, fs, recipeName);

        Map<String, String> overlayMap = getOverlay(recipeProperties);
        overlayParametersOverTemplate(argMap.get(RecipeToolArgs.RECIPE_FILE_ARG), argMap.get(RecipeToolArgs
                .RECIPE_PROCESS_XML_FILE_PATH_ARG), overlayMap);

        System.out.println("Completed disaster recovery");
        return 0;
    }

    private Map<RecipeToolArgs, String> setupArgs(final String[] arguments) throws ParseException {
        Options options = new Options();
        Map<RecipeToolArgs, String> argMap = new HashMap<RecipeToolArgs, String>();

        for (RecipeToolArgs arg : RecipeToolArgs.values()) {
            addOption(options, arg, arg.isRequired());
        }

        CommandLine cmd =  new GnuParser().parse(options, arguments);
        for (RecipeToolArgs arg : RecipeToolArgs.values()) {
            String optionValue = arg.getOptionValue(cmd);
            if (StringUtils.isNotEmpty(optionValue)) {
                argMap.put(arg, optionValue);
            }
        }
        return argMap;
    }

    private static void addOption(final Options options, final RecipeToolArgs arg,
                                  final boolean isRequired) {
        Option option = arg.getOption();
        option.setRequired(isRequired);
        options.addOption(option);
    }

    private static void validateProperties(final Properties recipeProperties) {
        for (RecipeToolOptions option : RecipeToolOptions.values()) {
            if (recipeProperties.getProperty(option.getName()) == null && option.isRequired()) {
                throw new IllegalArgumentException("Missing argument: " + option.getName());
            }
        }
    }

    private static Properties loadProperties(final String propertiesFilePath) throws Exception {
        InputStream inputStream = null;
        try {
            inputStream = new FileInputStream(propertiesFilePath);
            Properties prop = new Properties();
            prop.load(inputStream);
            return prop;
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
    }

    private static void validateArtifacts(final Properties recipeProperties) throws Exception{
        // validate the WF path
        String wfPath = recipeProperties.getProperty(RecipeToolOptions.WORKFLOW_PATH.getName());

        // If the file doesn't exist locally throw exception
        if (!StringUtils.isEmpty(wfPath) && !doesFileExist(wfPath)) {
            throw new Exception("Recipe workflow file does not exist : " + wfPath);
        }

        // validate lib path
        String libPath = recipeProperties.getProperty(RecipeToolOptions.WORKFLOW_LIB_PATH.getName());
        if (!StringUtils.isEmpty(libPath) && !doesFileExist(libPath)) {
            throw new Exception("Recipe lib file path does not exist : " + libPath);
        }
    }

    private static Map<String, String> getOverlay(final Properties recipeProperties) {
        Map<String, String> overlay = new HashMap<String, String>();
        for (Map.Entry<Object, Object> entry : recipeProperties.entrySet()) {
            String key = StringUtils.removeStart((String) entry.getKey(), RECIPE_PREFIX);
            overlay.put(key, (String) entry.getValue());
        }

        return overlay;
    }

    private static String overlayParametersOverTemplate(final String templateFile,
                                                        final String outFilename,
                                                        Map<String, String> overlay) throws Exception {
        if (templateFile == null || outFilename == null || overlay == null || overlay.isEmpty()) {
            throw new IllegalArgumentException("Invalid arguments passed");
        }

        String line;
        OutputStream out = null;
        BufferedReader reader = null;

        try {
            out = new FileOutputStream(outFilename);

            reader = new BufferedReader(new FileReader(templateFile));
            while ((line = reader.readLine()) != null) {
                Matcher matcher = RECIPE_VAR_PATTERN.matcher(line);
                while (matcher.find()) {
                    String variable = line.substring(matcher.start(), matcher.end());
                    String paramString = overlay.get(variable.substring(2, variable.length() - 2));
                    if (paramString == null) {
                        throw new Exception("Match not found for the template: " + variable);
                    }
                    line = line.replace(variable, paramString);
                    matcher = RECIPE_VAR_PATTERN.matcher(line);
                }
                out.write(line.getBytes());
                out.write("\n".getBytes());
            }
        } finally {
            IOUtils.closeQuietly(reader);
            IOUtils.closeQuietly(out);
        }
        return outFilename;
    }

    private static void copyFilesToHdfsIfRequired(final Properties recipeProperties,
                                                  final FileSystem fs,
                                                  final String recipeName) throws Exception {
        String recipeWfPathName = RecipeToolOptions.WORKFLOW_PATH.getName();
        String wfPath = recipeProperties.getProperty(recipeWfPathName);
        String wfPathValue;

        String hdfsPath = HDFS_WF_PATH + recipeName + File.separator;
        if (!StringUtils.isEmpty(wfPath)) {
            createDirOnHdfs(hdfsPath, fs);
            if (new File(wfPath).isDirectory()) {
                wfPathValue = hdfsPath + getLastPartOfPath(wfPath);
            } else {
                wfPathValue = hdfsPath + new File(wfPath).getName();
            }
            copyFileFromLocalToHdfs(wfPath, hdfsPath, fs);
            // Update the property with the hdfs path
            recipeProperties.setProperty(recipeWfPathName, wfPathValue);
            System.out.println("recipeWfPathName: " + recipeProperties.getProperty(recipeWfPathName));
        }

        String recipeWfLibPathName = RecipeToolOptions.WORKFLOW_LIB_PATH.getName();
        String libPath = recipeProperties.getProperty(recipeWfLibPathName);
        String libPathValue;
        if (!StringUtils.isEmpty(libPath)) {
            if (new File(libPath).isDirectory()) {
                libPathValue = hdfsPath + getLastPartOfPath(libPath);
                copyFileFromLocalToHdfs(libPath, hdfsPath, fs);
            } else {
                libPathValue = hdfsPath + "lib" + File.separator + new File(libPath).getName();
                copyFileFromLocalToHdfs(libPath, libPathValue, fs);
            }

            // Update the property with the hdfs path
            recipeProperties.setProperty(recipeWfLibPathName, libPathValue);
            System.out.println("recipeWfLibPathName: " + recipeProperties.getProperty(recipeWfLibPathName));
        }
    }

    private static String getLastPartOfPath(final String path) {
        String normalizedWfPath = FilenameUtils.normalizeNoEndSeparator(path);
        return (normalizedWfPath == null) ? FilenameUtils.getName(path)
                : FilenameUtils.getName(normalizedWfPath);
    }

    private static void createDirOnHdfs(String path, FileSystem fs) throws IOException {
        Path hdfsPath = new Path(path);
        if (!fs.exists(hdfsPath)) {
            fs.mkdirs(hdfsPath);
        }
    }

    private static boolean doesFileExist(final String filename) {
        return new File(filename).exists();
    }

    private static void copyFileFromLocalToHdfs(final String localFilePath,
                                                final String hdfsFilePath,
                                                final FileSystem fs) throws IOException {
        // For cases where validation of process entity file fails, the artifacts would have been already copied to
        // HDFS. Set overwrite to true so that next submit recipe copies updated artifats from local FS to HDFS
        fs.copyFromLocalFile(false, true, new Path(localFilePath), new Path(hdfsFilePath));
    }

    private static Configuration getConfiguration(final String storageEndpoint) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", storageEndpoint);
        return conf;
    }

    private FileSystem getFileSystemForHdfs(final Properties recipeProperties) throws Exception {
        if (hdfsFileSystem == null) {
            String storageEndpoint = RecipeToolOptions.SOURCE_CLUSTER_HDFS_WRITE_ENDPOINT.getName();
            hdfsFileSystem =  FileSystem.get(
                    getConfiguration(recipeProperties.getProperty(storageEndpoint)));
        }

        return hdfsFileSystem;
    }
}
