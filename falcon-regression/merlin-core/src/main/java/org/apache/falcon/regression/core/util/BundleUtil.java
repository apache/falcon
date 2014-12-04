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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.falcon.entity.v0.cluster.Location;
import org.apache.falcon.entity.v0.cluster.Property;
import org.apache.falcon.regression.Entities.ClusterMerlin;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.testng.Assert;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * util methods related to bundle.
 */
public final class BundleUtil {
    private BundleUtil() {
        throw new AssertionError("Instantiating utility class...");
    }
    private static final Logger LOGGER = Logger.getLogger(BundleUtil.class);

    public static Bundle readFeedReplicaltionBundle() throws IOException {
        return readBundleFromFolder("FeedReplicaltionBundles");
    }

    public static Bundle readLateDataBundle() throws IOException {
        return readBundleFromFolder("LateDataBundles");
    }

    public static Bundle readRetryBundle() throws IOException {
        return readBundleFromFolder("RetryTests");
    }

    public static Bundle readRetentionBundle() throws IOException {
        return readBundleFromFolder("RetentionBundles");
    }

    public static Bundle readELBundle() throws IOException {
        return readBundleFromFolder("ELbundle");
    }

    public static Bundle readHCatBundle() throws IOException {
        return readBundleFromFolder("hcat");
    }

    public static Bundle readHCat2Bundle() throws IOException {
        return readBundleFromFolder("hcat_2");
    }

    public static Bundle readUpdateBundle() throws IOException {
        return readBundleFromFolder("updateBundle");
    }

    private static Bundle readBundleFromFolder(final String folderPath) throws IOException {
        LOGGER.info("Loading xmls from directory: " + folderPath);
        File directory = null;
        try {
            directory = new File(BundleUtil.class.getResource("/" + folderPath).toURI());
        } catch (URISyntaxException e) {
            Assert.fail("could not find dir: " + folderPath);
        }
        final Collection<File> list = FileUtils.listFiles(directory, new String[] {"xml"}, true);
        File[] files = list.toArray(new File[list.size()]);
        Arrays.sort(files);
        String clusterData = "";
        final List<String> dataSets = new ArrayList<String>();
        String processData = "";

        for (File file : files) {
            LOGGER.info("Loading data from path: " + file.getAbsolutePath());
            final String data = IOUtils.toString(file.toURI());

            if (data.contains("uri:falcon:cluster:0.1")) {
                LOGGER.info("data been added to cluster");
                ClusterMerlin clusterMerlin = new ClusterMerlin(data);
                //set ACL
                clusterMerlin.setACL(MerlinConstants.CURRENT_USER_NAME,
                        MerlinConstants.CURRENT_USER_GROUP, "*");
                //set staging and working locations
                clusterMerlin.getLocations().getLocations().clear();
                final Location staging = new Location();
                staging.setName("staging");
                staging.setPath(Config.getProperty("merlin.staging.location",
                        "/tmp/falcon-regression-staging"));
                clusterMerlin.getLocations().getLocations().add(staging);
                final Location working = new Location();
                working.setName("working");
                working.setPath(Config.getProperty("merlin.working.location",
                        "/tmp/falcon-regression-working"));
                clusterMerlin.getLocations().getLocations().add(working);
                final String protectionPropName = "hadoop.rpc.protection";
                final String protectionPropValue = Config.getProperty(protectionPropName);
                if (StringUtils.isNotEmpty(protectionPropValue)) {
                    final Property property = Util.getFalconClusterPropertyObject(
                            protectionPropName, protectionPropValue.trim());
                    clusterMerlin.getProperties().getProperties().add(property);
                }
                clusterData = clusterMerlin.toString();
            } else if (data.contains("uri:falcon:feed:0.1")) {
                LOGGER.info("data been added to feed");
                FeedMerlin feedMerlin = new FeedMerlin(data);
                feedMerlin.setACL(MerlinConstants.CURRENT_USER_NAME,
                        MerlinConstants.CURRENT_USER_GROUP, "*");
                dataSets.add(feedMerlin.toString());
            } else if (data.contains("uri:falcon:process:0.1")) {
                LOGGER.info("data been added to process");
                ProcessMerlin processMerlin = new ProcessMerlin(data);
                processMerlin.setACL(MerlinConstants.CURRENT_USER_NAME,
                        MerlinConstants.CURRENT_USER_GROUP, "*");
                processData = processMerlin.toString();
            }
        }
        Assert.assertNotNull(clusterData, "expecting cluster data to be non-empty");
        Assert.assertTrue(!dataSets.isEmpty(), "expecting feed data to be non-empty");
        return new Bundle(clusterData, dataSets, processData);
    }

    public static void submitAllClusters(ColoHelper prismHelper, Bundle... b)
        throws IOException, URISyntaxException, AuthenticationException, InterruptedException {
        for (Bundle aB : b) {
            ServiceResponse r = prismHelper.getClusterHelper().submitEntity(aB.getClusters().get(0));
            Assert.assertTrue(r.getMessage().contains("SUCCEEDED"));

        }
    }

}
