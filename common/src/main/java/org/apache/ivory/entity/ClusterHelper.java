/*
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

package org.apache.ivory.entity;

import org.apache.hadoop.conf.Configuration;
import org.apache.ivory.entity.v0.cluster.*;

import java.util.Map;

public final class ClusterHelper {

    private ClusterHelper() {}

    public static Configuration getConfiguration(Cluster cluster) {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", getHdfsUrl(cluster));
        conf.set("mapred.job.tracker", getMREndPoint(cluster));
        for (Map.Entry<String, Property> property : cluster.
                getProperties().entrySet()) {
            conf.set(property.getKey(), property.getValue().getValue());
        }
        return conf;
    }

    public static String getOozieUrl(Cluster cluster) {
        return getInterfaceFor(cluster, Interfacetype.WORKFLOW);
    }

    public static String getHdfsUrl(Cluster cluster) {
        return getInterfaceFor(cluster, Interfacetype.WRITE);
    }

    public static String getReadOnlyHdfsUrl(Cluster cluster) {
        return getInterfaceFor(cluster, Interfacetype.READONLY);
    }

    public static String getMREndPoint(Cluster cluster) {
        return getInterfaceFor(cluster, Interfacetype.EXECUTE);
    }

    private static String getInterfaceFor(Cluster cluster, Interfacetype type) {
        assert cluster != null : "Cluster object can't be null";
        Map<Interfacetype,Interface> interfaces = cluster.getInterfaces();
        assert interfaces != null : "No interfaces for cluster " + cluster.getName() ;
        Interface interfaceRef = interfaces.get(type);
        assert interfaceRef != null : type + " interface not set for cluster " +
                cluster.getName();

        return interfaceRef.getEndpoint();
    }

    public static String getLocation(Cluster cluster, String locationKey) {
        assert cluster != null : "Cluster object can't be null";
        Map<String, Location> locations = cluster.getLocations();
        assert locations != null : "No locations configured for cluster " +
                cluster.getName() ;
        Location location = locations.get(locationKey);
        assert location != null : "Location " + locationKey +
                " not configured for cluster " + cluster.getName() ;
        return getHdfsUrl(cluster) + "/" + location.getPath();
    }
}
