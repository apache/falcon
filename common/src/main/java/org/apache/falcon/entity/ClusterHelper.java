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

package org.apache.falcon.entity;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.ClusterLocationType;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.cluster.Location;
import org.apache.falcon.entity.v0.cluster.Property;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.security.SecurityUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper to get end points relating to the cluster.
 */
public final class ClusterHelper {
    public static final String DEFAULT_BROKER_IMPL_CLASS = "org.apache.activemq.ActiveMQConnectionFactory";
    public static final String WORKINGDIR = "working";
    public static final String NO_USER_BROKER_URL = "NA";
    public static final String EMPTY_DIR_NAME = "EMPTY_DIR_DONT_DELETE";
    private static final Logger LOG = LoggerFactory.getLogger(ClusterHelper.class);

    private ClusterHelper() {
    }

    public static Cluster getCluster(String cluster) throws FalconException {
        return ConfigurationStore.get().get(EntityType.CLUSTER, cluster);
    }

    public static Configuration getConfiguration(Cluster cluster) {
        Configuration conf = new Configuration();

        final String storageUrl = getStorageUrl(cluster);
        conf.set(HadoopClientFactory.FS_DEFAULT_NAME_KEY, storageUrl);

        final String executeEndPoint = getMREndPoint(cluster);
        conf.set(HadoopClientFactory.MR_JT_ADDRESS_KEY, executeEndPoint);
        conf.set(HadoopClientFactory.YARN_RM_ADDRESS_KEY, executeEndPoint);

        if (cluster.getProperties() != null) {
            for (Property prop : cluster.getProperties().getProperties()) {
                conf.set(prop.getName(), prop.getValue());
            }
        }

        return conf;
    }

    public static Configuration getConfiguration(Configuration conf, String storageUrl,
                                                 String executeEndPoint, String kerberosPrincipal) {
        conf.set(HadoopClientFactory.FS_DEFAULT_NAME_KEY, storageUrl);
        conf.set(HadoopClientFactory.MR_JT_ADDRESS_KEY, executeEndPoint);
        conf.set(HadoopClientFactory.YARN_RM_ADDRESS_KEY, executeEndPoint);
        if (StringUtils.isNotBlank(kerberosPrincipal)) {
            conf.set(SecurityUtil.NN_PRINCIPAL, kerberosPrincipal);
        }
        return conf;
    }

    public static String getOozieUrl(Cluster cluster) {
        return getInterface(cluster, Interfacetype.WORKFLOW).getEndpoint();
    }

    public static String getStorageUrl(Cluster cluster) {
        return getNormalizedUrl(cluster, Interfacetype.WRITE);
    }

    public static String getReadOnlyStorageUrl(Cluster cluster) {
        return getNormalizedUrl(cluster, Interfacetype.READONLY);
    }

    public static String getMREndPoint(Cluster cluster) {
        return getInterface(cluster, Interfacetype.EXECUTE).getEndpoint();
    }

    public static String getRegistryEndPoint(Cluster cluster) {
        final Interface catalogInterface = getInterface(cluster, Interfacetype.REGISTRY);
        return catalogInterface == null ? null : catalogInterface.getEndpoint();
    }

    public static String getMessageBrokerUrl(Cluster cluster) {
        final Interface messageInterface = getInterface(cluster, Interfacetype.MESSAGING);
        return messageInterface == null ? NO_USER_BROKER_URL : messageInterface.getEndpoint();
    }

    public static String getSparkMasterEndPoint(Cluster cluster) {
        final Interface sparkInterface = getInterface(cluster, Interfacetype.SPARK);
        return sparkInterface == null ? null : sparkInterface.getEndpoint();
    }

    public static String getMessageBrokerImplClass(Cluster cluster) {
        if (cluster.getProperties() != null) {
            for (Property prop : cluster.getProperties().getProperties()) {
                if (prop.getName().equals("brokerImplClass")) {
                    return prop.getValue();
                }
            }
        }
        return DEFAULT_BROKER_IMPL_CLASS;
    }

    public static Interface getInterface(Cluster cluster, Interfacetype type) {
        if (cluster.getInterfaces() == null) {
            return null;
        }
        for (Interface interf : cluster.getInterfaces().getInterfaces()) {
            if (interf.getType() == type) {
                return interf;
            }
        }
        return null;
    }

    private static String getNormalizedUrl(Cluster cluster, Interfacetype type) {
        String normalizedUrl = getInterface(cluster, type).getEndpoint();
        if (normalizedUrl.endsWith("///")){
            return normalizedUrl;
        }
        String normalizedPath = new Path(normalizedUrl + "/").toString();
        return normalizedPath.substring(0, normalizedPath.length() - 1);
    }



    public static Location getLocation(Cluster cluster, ClusterLocationType clusterLocationType) {
        if (cluster.getLocations() == null) {
            return null;
        }
        for (Location loc : cluster.getLocations().getLocations()) {
            if (loc.getName().equals(clusterLocationType)) {
                return loc;
            }
        }
        //Mocking the working location FALCON-910
        if (clusterLocationType.equals(ClusterLocationType.WORKING)) {
            Location staging = getLocation(cluster, ClusterLocationType.STAGING);
            if (staging != null) {
                Location working = new Location();
                working.setName(ClusterLocationType.WORKING);
                working.setPath(staging.getPath().charAt(staging.getPath().length() - 1) == '/'
                        ?
                        staging.getPath().concat(WORKINGDIR)
                        :
                        staging.getPath().concat("/").concat(WORKINGDIR));
                return working;
            }
        }
        return null;
    }

    /**
     * Parsed the cluster object and checks for the working location.
     *
     * @param cluster
     * @return
     */
    public static boolean checkWorkingLocationExists(Cluster cluster) {
        for (Location loc : cluster.getLocations().getLocations()) {
            if (loc.getName().equals(ClusterLocationType.WORKING)) {
                return true;
            }
        }
        return false;
    }

    public static String getPropertyValue(Cluster cluster, String propName) {
        if (cluster.getProperties() != null) {
            for (Property prop : cluster.getProperties().getProperties()) {
                if (prop.getName().equals(propName)) {
                    return prop.getValue();
                }
            }
        }
        return null;
    }

    public static Map<String, String> getHiveProperties(Cluster cluster) {
        if (cluster.getProperties() != null) {
            List<Property> properties = cluster.getProperties().getProperties();
            if (properties != null && !properties.isEmpty()) {
                Map<String, String> hiveProperties = new HashMap<String, String>();
                for (Property prop : properties) {
                    if (prop.getName().startsWith("hive.")) {
                        hiveProperties.put(prop.getName(), prop.getValue());
                    }
                }
                return hiveProperties;
            }
        }
        return null;
    }

    public static String getEmptyDir(Cluster cluster) {
        return getStorageUrl(cluster) + getLocation(cluster, ClusterLocationType.STAGING).getPath()
                + "/" + EMPTY_DIR_NAME;
    }

    public static boolean matchInterface(final Cluster oldEntity, final Cluster newEntity,
                                         final Interfacetype interfaceType) {
        Interface oldInterface = getInterface(oldEntity, interfaceType);
        Interface newInterface = getInterface(newEntity, interfaceType);
        String oldEndpoint = (oldInterface == null) ? null : oldInterface.getEndpoint();
        String newEndpoint = (newInterface == null) ? null : newInterface.getEndpoint();
        LOG.debug("Verifying if Interfaces match for cluster {} : Old - {}, New - {}",
                interfaceType.name(), oldEndpoint, newEndpoint);
        return StringUtils.isBlank(oldEndpoint) && StringUtils.isBlank(newEndpoint)
                || StringUtils.isNotBlank(oldEndpoint) && oldEndpoint.equalsIgnoreCase(newEndpoint);
    }

    public static boolean matchLocations(final Cluster oldEntity, final Cluster newEntity,
                                         final ClusterLocationType locationType) {
        Location oldLocation = getLocation(oldEntity, locationType);
        Location newLocation = getLocation(newEntity, locationType);
        String oldLocationPath = (oldLocation == null) ? null : oldLocation.getPath();
        String newLocationPath = (newLocation == null) ? null : newLocation.getPath();
        LOG.debug("Verifying if Locations match {} : Old - {}, New - {}",
                locationType.name(), oldLocationPath, newLocationPath);
        return  StringUtils.isBlank(oldLocationPath) && StringUtils.isBlank(newLocationPath)
                || StringUtils.isNotBlank(oldLocationPath) && oldLocationPath.equalsIgnoreCase(newLocationPath);
    }

    public static boolean matchProperties(final Cluster oldEntity, final Cluster newEntity) {
        Map<String, String> oldProps = getClusterProperties(oldEntity);
        Map<String, String> newProps = getClusterProperties(newEntity);
        return oldProps.equals(newProps);
    }

    private static Map<String, String> getClusterProperties(final Cluster cluster) {
        Map<String, String> returnProps = new HashMap<String, String>();
        if (cluster.getProperties() != null) {
            for (Property prop : cluster.getProperties().getProperties()) {
                returnProps.put(prop.getName(), prop.getValue());
            }
        }
        return returnProps;
    }
}
