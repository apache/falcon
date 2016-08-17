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

package org.apache.falcon.metadata;

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONReader;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONWriter;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.falcon.FalconException;

import java.io.File;

/**
 * Utility class for graph operations.
 */
public final class GraphUpdateUtils {

    private static final String BANNER_MSG =
            "Before running this utility please make sure that Falcon startup properties "
                    + "has the right configuration settings for the graph database, "
                    + "Falcon server is stopped and no other access to the graph database is being performed.";

    private static final String IMPORT = "import";
    private static final String EXPORT = "export";
    private static final String INSTANCE_JSON_FILE = "instanceMetadata.json";

    private GraphUpdateUtils() {
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            usage();
            System.exit(1);
        }
        System.out.println(BANNER_MSG);
        String operation = args[0].toLowerCase();
        if (!(operation.equals(EXPORT) || operation.equals(IMPORT))) {
            usage();
            System.exit(1);
        }
        String utilsDir = args[1];
        File utilsDirFile = new File(utilsDir);
        if (!utilsDirFile.isDirectory()) {
            System.err.println(utilsDir + " is not a valid directory");
            System.exit(1);
        }
        String jsonFile = new File(utilsDirFile, INSTANCE_JSON_FILE).getAbsolutePath();
        try {
            Graph graph;
            if (operation.equals(EXPORT)) {
                graph = MetadataMappingService.initializeGraphDB();
                GraphSONWriter.outputGraph(graph, jsonFile);
                System.out.println("Exported instance metadata to " + jsonFile);
            } else {
                // Backup existing graphDB dir
                Configuration graphConfig = MetadataMappingService.getConfiguration();
                String graphStore = (String) graphConfig.getProperty("storage.directory");
                File graphStoreFile = new File(graphStore);
                File graphDirBackup = new File(graphStore + "_backup");
                if (graphDirBackup.exists()) {
                    FileUtils.deleteDirectory(graphDirBackup);
                }
                FileUtils.copyDirectory(graphStoreFile, graphDirBackup);

                // delete graph dir first and then init graphDB to ensure IMPORT happens into empty DB.
                FileUtils.deleteDirectory(graphStoreFile);
                graph = MetadataMappingService.initializeGraphDB();

                // Import, if there is an exception restore backup.
                try {
                    GraphSONReader.inputGraph(graph, jsonFile);
                    System.out.println("Imported instance metadata to " + jsonFile);
                } catch (Exception ex) {
                    String errorMsg = ex.getMessage();
                    if (graphStoreFile.exists()) {
                        FileUtils.deleteDirectory(graphStoreFile);
                    }
                    FileUtils.copyDirectory(graphDirBackup, graphStoreFile);
                    throw new FalconException(errorMsg);
                }
            }
        } catch (Exception e) {
            System.err.println("Error " + operation + "ing JSON data to " + jsonFile + ", " + e.getMessage());
            e.printStackTrace(System.out);
            System.exit(1);
        }
        System.exit(0);
    }

    public static void usage() {
        StringBuilder usageMessage = new StringBuilder(1024);
        usageMessage.append("usage: java ").append(GraphUpdateUtils.class.getName())
                .append(" {").append(EXPORT).append('|').append(IMPORT).append("} <directory>");
        System.err.println(usageMessage);
    }
}
