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

package org.apache.falcon.regression.testHelper;

import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.Config;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.OozieClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for test classes.
 */
public class BaseTestClass {
    private static String[] serverNames;
    private static final Logger LOGGER = Logger.getLogger(BaseTestClass.class);

    static {
        prepareProperties();
    }

    protected ColoHelper prism;
    protected List<ColoHelper> servers;
    protected List<FileSystem> serverFS;
    protected List<OozieClient> serverOC;
    protected String baseHDFSDir = "/tmp/falcon-regression";
    public static final String PRISM_PREFIX = "prism";
    protected Bundle[] bundles;
    public static final String MINUTE_DATE_PATTERN = "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";


    public BaseTestClass() {
        // loginFromKeytab as the current user
        prism = new ColoHelper(PRISM_PREFIX);
        servers = getServers();
        serverFS = new ArrayList<FileSystem>();
        serverOC = new ArrayList<OozieClient>();
        try {
            for (ColoHelper server : servers) {
                serverFS.add(server.getClusterHelper().getHadoopFS());
                serverOC.add(server.getClusterHelper().getOozieClient());
            }
            cleanTestsDirs();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        bundles = new Bundle[serverNames.length];
    }

    private static void prepareProperties() {

        serverNames = Config.getStringArray("servers");
        for (int i = 0; i < serverNames.length; i++) {
            serverNames[i] = serverNames[i].trim();
        }
    }

    private List<ColoHelper> getServers() {
        ArrayList<ColoHelper> returnList = new ArrayList<ColoHelper>();
        for (String serverName : serverNames) {
            returnList.add(new ColoHelper(serverName));
        }
        return returnList;
    }

    public void uploadDirToClusters(final String dstHdfsDir, final String localLocation)
        throws IOException {
        LOGGER.info(String.format("Uploading local dir: %s to all the clusters at: %s",
            localLocation, dstHdfsDir));
        for (FileSystem fs : serverFS) {
            HadoopUtil.uploadDir(fs, dstHdfsDir, localLocation);
        }
    }

    public final void removeBundles(Bundle... bundlesForDeletion) {
        for (Bundle bundle : bundlesForDeletion) {
            if (bundle != null) {
                bundle.deleteBundle(prism);
            }
        }
        if (bundlesForDeletion != this.bundles) {
            for (Bundle bundle : this.bundles) {
                if (bundle != null) {
                    bundle.deleteBundle(prism);
                }
            }
        }
    }

    public final void cleanTestsDirs() throws IOException {
        if (MerlinConstants.CLEAN_TESTS_DIR) {
            for (FileSystem fs : serverFS) {
                HadoopUtil.deleteDirIfExists(baseHDFSDir, fs);
            }
        }
    }
}
