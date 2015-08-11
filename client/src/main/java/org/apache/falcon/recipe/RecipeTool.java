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
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.recipe.util.RecipeProcessBuilderUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Base recipe tool for Falcon recipes.
 */
public class RecipeTool extends Configured implements Tool {
    private static final String HDFS_WF_PATH = "falcon" + File.separator + "recipes" + File.separator;
    private static final FsPermission FS_PERMISSION =
            new FsPermission(FsAction.ALL, FsAction.READ, FsAction.NONE);
    private static final String FS_DEFAULT_NAME_KEY = "fs.defaultFS";
    private static final String NN_PRINCIPAL = "dfs.namenode.kerberos.principal";

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new RecipeTool(), args);
    }

    @Override
    public int run(String[] arguments) throws Exception {

        Map<RecipeToolArgs, String> argMap = setupArgs(arguments);
        if (argMap == null || argMap.isEmpty()) {
            throw new Exception("Arguments passed to recipe is null");
        }
        Configuration conf = getConf();
        String recipePropertiesFilePath = argMap.get(RecipeToolArgs.RECIPE_PROPERTIES_FILE_ARG);
        Properties recipeProperties = loadProperties(recipePropertiesFilePath);
        validateProperties(recipeProperties);

        String recipeOperation = argMap.get(RecipeToolArgs.RECIPE_OPERATION_ARG);
        Recipe recipeType = RecipeFactory.getRecipeToolType(recipeOperation);
        if (recipeType != null) {
            recipeType.validate(recipeProperties);
            Properties props = recipeType.getAdditionalSystemProperties(recipeProperties);
            if (props != null && !props.isEmpty()) {
                recipeProperties.putAll(props);
            }
        }

        String processFilename;

        FileSystem fs = getFileSystemForHdfs(recipeProperties, conf);
        validateArtifacts(recipeProperties, fs);

        String recipeName = recipeProperties.getProperty(RecipeToolOptions.RECIPE_NAME.getName());
        copyFilesToHdfsIfRequired(recipeProperties, fs, recipeName);

        processFilename = RecipeProcessBuilderUtils.createProcessFromTemplate(argMap.get(RecipeToolArgs
                .RECIPE_FILE_ARG), recipeProperties, argMap.get(RecipeToolArgs.RECIPE_PROCESS_XML_FILE_PATH_ARG));


        System.out.println("Generated process file to be scheduled: ");
        System.out.println(FileUtils.readFileToString(new File(processFilename)));

        System.out.println("Completed recipe processing");
        return 0;
    }

    private Map<RecipeToolArgs, String> setupArgs(final String[] arguments) throws ParseException {
        Options options = new Options();
        Map<RecipeToolArgs, String> argMap = new HashMap<RecipeToolArgs, String>();

        for (RecipeToolArgs arg : RecipeToolArgs.values()) {
            addOption(options, arg, arg.isRequired());
        }

        CommandLine cmd = new GnuParser().parse(options, arguments);
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

    private static void validateArtifacts(final Properties recipeProperties, final FileSystem fs) throws Exception {
        // validate the WF path
        String wfPath = recipeProperties.getProperty(RecipeToolOptions.WORKFLOW_PATH.getName());

        // Check if file exists on HDFS
        if (StringUtils.isNotEmpty(wfPath) && !fs.exists(new Path(wfPath))) {
            // If the file doesn't exist locally throw exception
            if (!doesFileExist(wfPath)) {
                throw new Exception("Recipe workflow file does not exist : " + wfPath + " on local FS or HDFS");
            }
        }

        // validate lib path
        String libPath = recipeProperties.getProperty(RecipeToolOptions.WORKFLOW_LIB_PATH.getName());
        if (StringUtils.isNotEmpty(libPath) && !fs.exists(new Path(libPath))) {
            if (!doesFileExist(libPath)) {
                throw new Exception("Recipe lib file path does not exist : " + libPath + " on local FS or HDFS");
            }
        }
    }

    private static void copyFilesToHdfsIfRequired(final Properties recipeProperties,
                                                  final FileSystem fs,
                                                  final String recipeName) throws Exception {

        String hdfsPath = HDFS_WF_PATH + recipeName + File.separator;

        String recipeWfPathName = RecipeToolOptions.WORKFLOW_PATH.getName();
        String wfPath = recipeProperties.getProperty(recipeWfPathName);
        String wfPathValue;

        // Copy only if files are on local FS
        if (StringUtils.isNotEmpty(wfPath) && !fs.exists(new Path(wfPath))) {
            createDirOnHdfs(hdfsPath, fs);
            if (new File(wfPath).isDirectory()) {
                wfPathValue = hdfsPath + getLastPartOfPath(wfPath);
                copyFileFromLocalToHdfs(wfPath, hdfsPath, true, wfPathValue, fs);
            } else {
                wfPathValue = hdfsPath + new File(wfPath).getName();
                copyFileFromLocalToHdfs(wfPath, hdfsPath, false, null, fs);
            }
            // Update the property with the hdfs path
            recipeProperties.setProperty(recipeWfPathName,
                    fs.getFileStatus(new Path(wfPathValue)).getPath().toString());
            System.out.println("Copied WF to: " + recipeProperties.getProperty(recipeWfPathName));
        }

        String recipeWfLibPathName = RecipeToolOptions.WORKFLOW_LIB_PATH.getName();
        String libPath = recipeProperties.getProperty(recipeWfLibPathName);
        String libPathValue;
        // Copy only if files are on local FS
        boolean isLibPathEmpty = StringUtils.isEmpty(libPath);
        if (!isLibPathEmpty && !fs.exists(new Path(libPath))) {
            if (new File(libPath).isDirectory()) {
                libPathValue = hdfsPath + getLastPartOfPath(libPath);
                copyFileFromLocalToHdfs(libPath, hdfsPath, true, libPathValue, fs);
            } else {
                libPathValue = hdfsPath + "lib" + File.separator + new File(libPath).getName();
                copyFileFromLocalToHdfs(libPath, libPathValue, false, null, fs);
            }

            // Update the property with the hdfs path
            recipeProperties.setProperty(recipeWfLibPathName,
                    fs.getFileStatus(new Path(libPathValue)).getPath().toString());
            System.out.println("Copied WF libs to: " + recipeProperties.getProperty(recipeWfLibPathName));
        } else if (isLibPathEmpty) {
            // Replace ##workflow.lib.path## with "" to ignore lib in workflow template
            recipeProperties.setProperty(recipeWfLibPathName, "");
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
            FileSystem.mkdirs(fs, hdfsPath, FS_PERMISSION);
        }
    }

    private static boolean doesFileExist(final String filename) {
        return new File(filename).exists();
    }

    private static void copyFileFromLocalToHdfs(final String localFilePath,
                                                final String hdfsFilePath,
                                                final boolean copyDir,
                                                final String hdfsFileDirPath,
                                                final FileSystem fs) throws IOException {
        /* If directory already exists and has contents, copyFromLocalFile with overwrite set to yes will fail with
         * "Target is a directory". Delete the directory */
        if (copyDir) {
            Path hdfsPath = new Path(hdfsFileDirPath);
            fs.delete(hdfsPath, true);
        }

        /* For cases where validation of process entity file fails, the artifacts would have been already copied to
         * HDFS. Set overwrite to true so that next submit recipe copies updated artifacts from local FS to HDFS */
        fs.copyFromLocalFile(false, true, new Path(localFilePath), new Path(hdfsFilePath));
    }

    private FileSystem getFileSystemForHdfs(final Properties recipeProperties,
                                            final Configuration conf) throws Exception {
        String storageEndpoint = RecipeToolOptions.CLUSTER_HDFS_WRITE_ENDPOINT.getName();
        String nameNode = recipeProperties.getProperty(storageEndpoint);
        conf.set(FS_DEFAULT_NAME_KEY, nameNode);
        if (UserGroupInformation.isSecurityEnabled()) {
            String nameNodePrincipal = recipeProperties.getProperty(RecipeToolOptions.RECIPE_NN_PRINCIPAL.getName());
            conf.set(NN_PRINCIPAL, nameNodePrincipal);
        }
        return createFileSystem(UserGroupInformation.getLoginUser(), new URI(nameNode), conf);
    }

    private FileSystem createFileSystem(UserGroupInformation ugi, final URI uri,
                                       final Configuration conf) throws Exception {
        try {
            final String proxyUserName = ugi.getShortUserName();
            if (proxyUserName.equals(UserGroupInformation.getLoginUser().getShortUserName())) {
                return FileSystem.get(uri, conf);
            }

            return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                public FileSystem run() throws Exception {
                    return FileSystem.get(uri, conf);
                }
            });
        } catch (InterruptedException ex) {
            throw new IOException("Exception creating FileSystem:" + ex.getMessage(), ex);
        }
    }
}
