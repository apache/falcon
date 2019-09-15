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
package org.apache.falcon.notification.service.impl;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.notification.service.request.DataNotificationRequest;
import org.apache.falcon.notification.service.request.LocationBasedDataNotificationRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Handles Path based Data requests for ABSOLUTE El expressions.
 */
public class PathServiceHandler implements DataServiceHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PathServiceHandler.class);

    @Override
    public boolean handleRequest(DataNotificationRequest request) {
        LocationBasedDataNotificationRequest dataNotificationRequest = null;
        if (request instanceof LocationBasedDataNotificationRequest) {
            dataNotificationRequest = (LocationBasedDataNotificationRequest)request;
        } else {
            throw new IllegalArgumentException("Request should be LocationBased for to be processed");
        }
        try {
            Entity entity = EntityUtil.getEntity(EntityType.CLUSTER, dataNotificationRequest.getCluster());
            Cluster clusterEntity = (Cluster) entity;
            Configuration conf = ClusterHelper.getConfiguration(clusterEntity);
            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(conf);
            Map<Path, Boolean> locations = dataNotificationRequest.getLocationMap();
            List<Path> nonAvailablePaths = getUnAvailablePaths(locations);
            updatePathsAvailability(nonAvailablePaths, fs, locations);
            if (allPathsExist(locations)) {
                return true;
            }
        } catch (FalconException e) {
            LOG.error("Retrieving the Cluster Entity " + e);
        } catch (IOException e) {
            LOG.error("Unable to connect to FileSystem " + e);
        }
        return false;
    }

    private void updatePathsAvailability(List<Path> unAvailablePaths, FileSystem fs,
                                         Map<Path, Boolean> locations) throws IOException {
        for (Path path : unAvailablePaths) {
            if (fs.exists(path)) {
                if (locations.containsKey(path)) {
                    locations.put(path, true);
                } else {
                    locations.put(new Path(path.toUri().getPath()), true);
                }
            }
        }
    }

    private List<Path> getUnAvailablePaths(Map<Path, Boolean> locations) {
        List<Path> paths = new ArrayList<>();
        for (Map.Entry<Path, Boolean> pathInfo : locations.entrySet()) {
            if (!pathInfo.getValue()) {
                paths.add(pathInfo.getKey());
            }
        }
        return paths;
    }

    private boolean allPathsExist(Map<Path, Boolean> locations) {
        if (locations.containsValue(Boolean.FALSE)) {
            return false;
        }
        return true;
    }
}
