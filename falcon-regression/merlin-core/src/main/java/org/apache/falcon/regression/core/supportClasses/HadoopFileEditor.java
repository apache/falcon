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

package org.apache.falcon.regression.core.supportClasses;

import org.apache.commons.io.FileUtils;
import org.apache.falcon.regression.core.util.Util;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Class for simulating editing and restoring of a file in hdfs. */
public class HadoopFileEditor {
    private static final Logger LOGGER = Logger.getLogger(HadoopFileEditor.class);
    private FileSystem fs;
    private List<String> paths;
    private List<String> files;

    public HadoopFileEditor(FileSystem fs) {
        this.fs = fs;
        paths = new ArrayList<>();
        files = new ArrayList<>();
    }

    /**
     * Method to edit a file present on HDFS. Path is the location on HDFS,
     * @param path path of the file to be edited
     * @param putAfterString first instance of string after which the text is to be
     * @param toBeInserted the text to be inserted
     * @throws IOException
     */
    public void edit(String path, String putAfterString, String toBeInserted) throws IOException {
        paths.add(path);
        String currentFile = Util.getFileNameFromPath(path);
        files.add(currentFile);
        FileUtils.deleteQuietly(new File(currentFile));
        FileUtils.deleteQuietly(new File("." + currentFile + ".crc"));
        FileUtils.deleteQuietly(new File(currentFile + ".bck"));
        FileUtils.deleteQuietly(new File("tmp"));

        Path file = new Path(path);
        //check if currentFile exists or not
        if (fs.exists(file)) {
            fs.copyToLocalFile(file, new Path(currentFile));
            FileUtils.copyFile(new File(currentFile), new File(currentFile + ".bck"));
            BufferedWriter bufWriter = new BufferedWriter(new FileWriter("tmp"));
            BufferedReader br = new BufferedReader(new FileReader(currentFile));
            String line;
            boolean isInserted = false;
            while ((line = br.readLine()) != null) {
                bufWriter.write(line);
                bufWriter.write('\n');
                if (line.contains(putAfterString) && !isInserted) {
                    bufWriter.write(toBeInserted);
                    isInserted = true;
                }
            }
            br.close();
            bufWriter.close();
            FileUtils.deleteQuietly(new File(currentFile));
            FileUtils.copyFile(new File("tmp"), new File(currentFile));
            FileUtils.deleteQuietly(new File("tmp"));

            fs.delete(file, false);
            File crcFile = new File("." + currentFile + ".crc");
            if (crcFile.exists()) {
                LOGGER.info("Result of delete on crcFile" + crcFile + " : " + crcFile.delete());
            }
            fs.copyFromLocalFile(new Path(currentFile), file);
        } else {
            LOGGER.info("Nothing to do, " + currentFile + " does not exists");
        }
    }

    /**
     * Restore back the original file to HDFS that was edited by edit function.
     * @throws IOException
     */
    public void restore() throws IOException {
        for (int i = 0; i < paths.size(); i++) {
            fs.delete(new Path(paths.get(i)), false);
            FileUtils.deleteQuietly(new File(files.get(i)));
            FileUtils.copyFile(new File(files.get(i) + ".bck"),
                new File(files.get(i)));
            fs.copyFromLocalFile(new Path(files.get(i)), new Path(paths.get(i)));
            FileUtils.deleteQuietly(new File(files.get(i)));
            FileUtils.deleteQuietly(new File(files.get(i) + ".bck"));
        }
    }
}
