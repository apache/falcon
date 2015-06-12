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
import org.apache.falcon.entity.v0.cluster.ClusterLocationType;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
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

    public static Bundle readFeedReplicationBundle() throws IOException {
        return readBundleFromFolder("FeedReplicationBundles");
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

    public static Bundle readCombinedActionsBundle() throws IOException {
        return readBundleFromFolder("combinedActions");
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
        final List<String> dataSets = new ArrayList<>();
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
                staging.setName(ClusterLocationType.STAGING);
                staging.setPath(MerlinConstants.STAGING_LOCATION);
                clusterMerlin.getLocations().getLocations().add(staging);
                final Location working = new Location();
                working.setName(ClusterLocationType.WORKING);
                working.setPath(MerlinConstants.WORKING_LOCATION);
                clusterMerlin.getLocations().getLocations().add(working);
                final Location temp = new Location();
                temp.setName(ClusterLocationType.TEMP);
                temp.setPath(MerlinConstants.TEMP_LOCATION);
                clusterMerlin.getLocations().getLocations().add(temp);
                final String protectionPropName = "hadoop.rpc.protection";
                final String protectionPropValue = Config.getProperty(protectionPropName);
                if (StringUtils.isNotEmpty(protectionPropValue)) {
                    final Property property = getFalconClusterPropertyObject(
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

    /**
     * Configures cluster definition according to provided properties.
     * @param cluster cluster which should be configured
     * @param prefix current cluster prefix
     * @return modified cluster definition
     */
    public static ClusterMerlin getEnvClusterXML(String cluster, String prefix) {
        ClusterMerlin clusterObject = new ClusterMerlin(cluster);
        if ((null == prefix) || prefix.isEmpty()) {
            prefix = "";
        } else {
            prefix = prefix + ".";
        }
        String hcatEndpoint = Config.getProperty(prefix + "hcat_endpoint");

        //now read and set relevant values
        for (Interface iface : clusterObject.getInterfaces().getInterfaces()) {
            if (iface.getType() == Interfacetype.READONLY) {
                iface.setEndpoint(Config.getProperty(prefix + "cluster_readonly"));
            } else if (iface.getType() == Interfacetype.WRITE) {
                iface.setEndpoint(Config.getProperty(prefix + "cluster_write"));
            } else if (iface.getType() == Interfacetype.EXECUTE) {
                iface.setEndpoint(Config.getProperty(prefix + "cluster_execute"));
            } else if (iface.getType() == Interfacetype.WORKFLOW) {
                iface.setEndpoint(Config.getProperty(prefix + "oozie_url"));
            } else if (iface.getType() == Interfacetype.MESSAGING) {
                iface.setEndpoint(Config.getProperty(prefix + "activemq_url"));
            } else if (iface.getType() == Interfacetype.REGISTRY) {
                iface.setEndpoint(hcatEndpoint);
            }
        }
        //set colo name:
        clusterObject.setColo(Config.getProperty(prefix + "colo"));
        // get the properties object for the cluster
        org.apache.falcon.entity.v0.cluster.Properties clusterProperties =
            clusterObject.getProperties();
        // properties in the cluster needed when secure mode is on
        if (MerlinConstants.IS_SECURE) {
            // add the namenode principal to the properties object
            clusterProperties.getProperties().add(getFalconClusterPropertyObject(
                    "dfs.namenode.kerberos.principal",
                    Config.getProperty(prefix + "namenode.kerberos.principal", "none")));

            // add the hive meta store principal to the properties object
            clusterProperties.getProperties().add(getFalconClusterPropertyObject(
                    "hive.metastore.kerberos.principal",
                    Config.getProperty(prefix + "hive.metastore.kerberos.principal", "none")));

            // Until oozie has better integration with secure hive we need to send the properites to
            // falcon.
            // hive.metastore.sasl.enabled = true
            clusterProperties.getProperties()
                .add(getFalconClusterPropertyObject("hive.metastore.sasl.enabled", "true"));
            // Only set the metastore uri if its not empty or null.
        }
        String hiveMetastoreUris = Config.getProperty(prefix + "hive.metastore.uris");
        if (StringUtils.isNotBlank(hiveMetastoreUris)) {
            //hive.metastore.uris
            clusterProperties.getProperties()
                .add(getFalconClusterPropertyObject("hive.metastore.uris", hiveMetastoreUris));
        }
        String hiveServer2Uri = Config.getProperty(prefix + "hive.server2.uri");
        if (StringUtils.isNotBlank(hiveServer2Uri)) {
            //hive.metastore.uris
            clusterProperties.getProperties()
                .add(getFalconClusterPropertyObject("hive.server2.uri", hiveServer2Uri));
        }
        return clusterObject;
    }

    /**
     * Forms property object based on parameters.
     * @param name property name
     * @param value property value
     * @return property object
     */
    private static Property getFalconClusterPropertyObject(String name, String value) {
        Property property = new Property();
        property.setName(name);
        property.setValue(value);
        return property;
    }

    public static List<ClusterMerlin> getClustersFromStrings(List<String> clusterStrings) {
        List<ClusterMerlin> clusters = new ArrayList<>();
        for (String clusterString : clusterStrings) {
            clusters.add(new ClusterMerlin(clusterString));
        }
        return clusters;
    }
}
