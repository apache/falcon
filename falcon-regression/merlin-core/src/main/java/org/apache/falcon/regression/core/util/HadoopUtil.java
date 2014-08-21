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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Util methods related to hadoop.
 */
public final class HadoopUtil {

    public static final String SOMETHING_RANDOM = "somethingRandom";
    private static final Logger LOGGER = Logger.getLogger(HadoopUtil.class);

    private HadoopUtil() {
        throw new AssertionError("Instantiating utility class...");
    }
    public static List<String> getAllFilesHDFS(FileSystem fs, Path location) throws IOException {

        List<String> files = new ArrayList<String>();
        if (!fs.exists(location)) {
            return files;
        }
        FileStatus[] stats = fs.listStatus(location);

        for (FileStatus stat : stats) {
            if (!isDir(stat)) {
                files.add(stat.getPath().toString());
            }
        }
        return files;
    }

    public static List<Path> getAllDirsRecursivelyHDFS(
        FileSystem fs, Path location, int depth) throws IOException {

        List<Path> returnList = new ArrayList<Path>();

        FileStatus[] stats = fs.listStatus(location);

        for (FileStatus stat : stats) {
            if (isDir(stat)) {
                returnList.add(stat.getPath());
                if (depth > 0) {
                    returnList.addAll(getAllDirsRecursivelyHDFS(fs, stat.getPath(), depth - 1));
                }
            }

        }

        return returnList;
    }

    public static List<Path> getAllFilesRecursivelyHDFS(
        FileSystem fs, Path location) throws IOException {

        List<Path> returnList = new ArrayList<Path>();

        FileStatus[] stats;
        try {
            stats = fs.listStatus(location);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return new ArrayList<Path>();
        }

        if (stats == null) {
            return returnList;
        }
        for (FileStatus stat : stats) {

            if (!isDir(stat)) {
                if (!stat.getPath().toUri().toString().contains("_SUCCESS")) {
                    returnList.add(stat.getPath());
                }
            } else {
                returnList.addAll(getAllFilesRecursivelyHDFS(fs, stat.getPath()));
            }
        }

        return returnList;

    }

    @SuppressWarnings("deprecation")
    private static boolean isDir(FileStatus stat) {
        return stat.isDir();
    }

    public static void copyDataToFolder(final FileSystem fs, final String dstHdfsDir,
                                        final String srcFileLocation)
        throws IOException {
        LOGGER.info(String.format("Copying local dir %s to hdfs location %s on %s",
            srcFileLocation, dstHdfsDir, fs.getUri()));
        fs.copyFromLocalFile(new Path(srcFileLocation), new Path(dstHdfsDir));
    }

    public static void uploadDir(final FileSystem fs, final String dstHdfsDir,
                                 final String localLocation)
        throws IOException {
        LOGGER.info(String.format("Uploading local dir %s to hdfs location %s", localLocation,
            dstHdfsDir));
        HadoopUtil.deleteDirIfExists(dstHdfsDir, fs);
        HadoopUtil.copyDataToFolder(fs, dstHdfsDir, localLocation);
    }

    public static List<String> getHDFSSubFoldersName(FileSystem fs,
                                                     String baseDir) throws IOException {

        List<String> returnList = new ArrayList<String>();

        FileStatus[] stats = fs.listStatus(new Path(baseDir));


        for (FileStatus stat : stats) {
            if (isDir(stat)) {
                returnList.add(stat.getPath().getName());
            }

        }


        return returnList;
    }

    public static boolean isFilePresentHDFS(FileSystem fs, String hdfsPath, String fileToCheckFor)
        throws IOException {

        LOGGER.info("getting file from folder: " + hdfsPath);

        List<String> fileNames = getAllFileNamesFromHDFS(fs, hdfsPath);

        for (String filePath : fileNames) {

            if (filePath.contains(fileToCheckFor)) {
                return true;
            }
        }

        return false;
    }

    private static List<String> getAllFileNamesFromHDFS(
        FileSystem fs, String hdfsPath) throws IOException {

        List<String> returnList = new ArrayList<String>();

        LOGGER.info("getting file from folder: " + hdfsPath);
        FileStatus[] stats = fs.listStatus(new Path(hdfsPath));

        for (FileStatus stat : stats) {
            String currentPath = stat.getPath().toUri().getPath(); // gives directory name
            if (!isDir(stat)) {
                returnList.add(currentPath);
            }


        }
        return returnList;
    }

    public static void recreateDir(FileSystem fs, String path) throws IOException {

        deleteDirIfExists(path, fs);
        LOGGER.info("creating hdfs dir: " + path + " on " + fs.getConf().get("fs.default.name"));
        fs.mkdirs(new Path(path));

    }

    public static void recreateDir(List<FileSystem> fileSystems, String path) throws IOException {

        for (FileSystem fs : fileSystems) {
            recreateDir(fs, path);
        }
    }

    public static void deleteDirIfExists(String hdfsPath, FileSystem fs) throws IOException {
        Path path = new Path(hdfsPath);
        if (fs.exists(path)) {
            LOGGER.info(String.format("Deleting HDFS path: %s on %s", path, fs.getUri()));
            fs.delete(path, true);
        } else {
            LOGGER.info(String.format(
                "Not deleting non-existing HDFS path: %s on %s", path, fs.getUri()));
        }
    }

    public static void flattenAndPutDataInFolder(FileSystem fs, String inputPath,
                                                 List<String> remoteLocations) throws IOException {
        flattenAndPutDataInFolder(fs, inputPath, "", remoteLocations);
    }

    public static List<String> flattenAndPutDataInFolder(FileSystem fs, String inputPath,
                                                 String remotePathPrefix,
                                                 List<String> remoteLocations) throws IOException {
        if (StringUtils.isNotEmpty(remotePathPrefix)) {
            deleteDirIfExists(remotePathPrefix, fs);
        }
        LOGGER.info("Creating data in folders: \n" + remoteLocations);
        File input = new File(inputPath);
        File[] files = input.isDirectory() ? input.listFiles() : new File[]{input};
        List<Path> filePaths = new ArrayList<Path>();
        assert files != null;
        for (final File file : files) {
            if (!file.isDirectory()) {
                final Path filePath = new Path(file.getAbsolutePath());
                filePaths.add(filePath);
            }
        }

        if (!remotePathPrefix.endsWith("/") && !remoteLocations.get(0).startsWith("/")) {
            remotePathPrefix += "/";
        }
        Pattern pattern = Pattern.compile(":[\\d]+/"); // remove 'hdfs(hftp)://server:port'
        List<String> locations = new ArrayList<String>();
        for (String remoteDir : remoteLocations) {
            String remoteLocation = remotePathPrefix + remoteDir;
            if (pattern.matcher(remoteLocation).find()) {
                remoteLocation = remoteLocation.split(":[\\d]+")[1];
            }
            locations.add(remoteLocation);
            LOGGER.info(String.format("copying to: %s files: %s",
                fs.getUri() + remoteLocation, Arrays.toString(files)));
            if (!fs.exists(new Path(remoteLocation))) {
                fs.mkdirs(new Path(remoteLocation));
            }

            fs.copyFromLocalFile(false, true, filePaths.toArray(new Path[filePaths.size()]),
                new Path(remoteLocation));
        }
        return locations;
    }

    public static void createLateDataFoldersWithRandom(FileSystem fs, String folderPrefix,
        List<String> folderList) throws IOException {
        LOGGER.info("creating late data folders.....");
        folderList.add(SOMETHING_RANDOM);

        for (final String folder : folderList) {
            fs.mkdirs(new Path(folderPrefix + folder));
        }

        LOGGER.info("created all late data folders.....");
    }

    public static void copyDataToFolders(FileSystem fs, List<String> folderList,
        String directory, String folderPrefix) throws IOException {
        LOGGER.info("copying data into folders....");
        List<String> fileLocations = new ArrayList<String>();
        File[] files = new File(directory).listFiles();
        if (files != null) {
            for (final File file : files) {
                fileLocations.add(file.toString());
            }
        }
        copyDataToFolders(fs, folderPrefix, folderList,
                fileLocations.toArray(new String[fileLocations.size()]));
    }

    public static void copyDataToFolders(FileSystem fs, final String folderPrefix,
        List<String> folderList, String... fileLocations) throws IOException {
        for (final String folder : folderList) {
            boolean r;
            String folderSpace = folder.replaceAll("/", "_");
            File f = new File(OSUtil.NORMAL_INPUT + folderSpace + ".txt");
            if (!f.exists()) {
                r = f.createNewFile();
                if (!r) {
                    LOGGER.info("file could not be created");
                }
            }

            FileWriter fr = new FileWriter(f);
            fr.append("folder");
            fr.close();
            fs.copyFromLocalFile(new Path(f.getAbsolutePath()), new Path(folderPrefix + folder));
            r = f.delete();
            if (!r) {
                LOGGER.info("delete was not successful");
            }

            Path[] srcPaths = new Path[fileLocations.length];
            for (int i = 0; i < srcPaths.length; ++i) {
                srcPaths[i] = new Path(fileLocations[i]);
            }
            LOGGER.info(String.format("copying  %s to %s%s on %s", Arrays.toString(srcPaths),
                folderPrefix, folder, fs.getUri()));
            fs.copyFromLocalFile(false, true, srcPaths, new Path(folderPrefix + folder));
        }
    }

    public static void lateDataReplenish(FileSystem fs, int interval,
        int minuteSkip, String folderPrefix) throws IOException {
        List<String> folderData = TimeUtil.getMinuteDatesOnEitherSide(interval, minuteSkip);

        createLateDataFoldersWithRandom(fs, folderPrefix, folderData);
        copyDataToFolders(fs, folderData, OSUtil.NORMAL_INPUT, folderPrefix);
    }

    public static void createLateDataFolders(FileSystem fs, final String folderPrefix,
                                             List<String> folderList) throws IOException {
        for (final String folder : folderList) {
            fs.mkdirs(new Path(folderPrefix + folder));
        }
    }

    public static void injectMoreData(FileSystem fs, final String remoteLocation,
                                      String localLocation) throws IOException {
        File[] files = new File(localLocation).listFiles();
        assert files != null;
        for (final File file : files) {
            if (!file.isDirectory()) {
                String path = remoteLocation + "/" + System.currentTimeMillis() / 1000 + "/";
                LOGGER.info("inserting data@ " + path);
                fs.copyFromLocalFile(new Path(file.getAbsolutePath()), new Path(path));
            }
        }

    }

    public static void putFileInFolderHDFS(FileSystem fs, int interval, int minuteSkip,
                                           String folderPrefix, String fileToBePut)
        throws IOException {
        List<String> folderPaths = TimeUtil.getMinuteDatesOnEitherSide(interval, minuteSkip);
        LOGGER.info("folderData: " + folderPaths.toString());

        createLateDataFolders(fs, folderPrefix, folderPaths);

        if (fileToBePut.equals("_SUCCESS")) {
            copyDataToFolders(fs, folderPrefix, folderPaths, OSUtil.NORMAL_INPUT + "_SUCCESS");
        } else {
            copyDataToFolders(fs, folderPrefix, folderPaths, OSUtil.NORMAL_INPUT + "log_01.txt");
        }

    }

    public static void lateDataReplenishWithoutSuccess(FileSystem fs, int interval,
        int minuteSkip, String folderPrefix, String postFix) throws IOException {
        List<String> folderPaths = TimeUtil.getMinuteDatesOnEitherSide(interval, minuteSkip);
        LOGGER.info("folderData: " + folderPaths.toString());

        if (postFix != null) {
            for (int i = 0; i < folderPaths.size(); i++) {
                folderPaths.set(i, folderPaths.get(i) + postFix);
            }
        }

        createLateDataFolders(fs, folderPrefix, folderPaths);
        copyDataToFolders(fs, folderPrefix, folderPaths,
                OSUtil.NORMAL_INPUT + "log_01.txt");
    }

    public static void lateDataReplenish(FileSystem fs, int interval, int minuteSkip,
                                         String folderPrefix, String postFix) throws IOException {
        List<String> folderPaths = TimeUtil.getMinuteDatesOnEitherSide(interval, minuteSkip);
        LOGGER.info("folderData: " + folderPaths.toString());

        if (postFix != null) {
            for (int i = 0; i < folderPaths.size(); i++) {
                folderPaths.set(i, folderPaths.get(i) + postFix);
            }
        }

        createLateDataFolders(fs, folderPrefix, folderPaths);
        copyDataToFolders(fs, folderPrefix, folderPaths,
            OSUtil.NORMAL_INPUT + "_SUCCESS", OSUtil.NORMAL_INPUT + "log_01.txt");
    }

    /**
     * Removes general folder on file system. Creates folders according to passed folders list
     * and fills them with data if required.
     *
     * @param fileSystem destination file system
     * @param prefix prefix of path where data should be placed
     * @param folderList list of folders to be created and filled with data if required
     * @param uploadData if folders should be filled with data
     * @throws IOException
     */
    public static void replenishData(FileSystem fileSystem, String prefix, List<String> folderList,
        boolean uploadData) throws IOException {
        //purge data first
        deleteDirIfExists(prefix, fileSystem);

        folderList.add(SOMETHING_RANDOM);

        for (final String folder : folderList) {
            final String pathString = prefix + folder;
            LOGGER.info(fileSystem.getUri() + pathString);
            fileSystem.mkdirs(new Path(pathString));
            if (uploadData) {
                fileSystem.copyFromLocalFile(new Path(OSUtil.RESOURCES + "log_01.txt"),
                        new Path(pathString));
            }
        }
    }
}
