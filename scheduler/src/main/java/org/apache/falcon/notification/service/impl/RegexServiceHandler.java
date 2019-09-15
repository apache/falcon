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
import org.apache.falcon.execution.SchedulerUtil;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.notification.service.request.DataNotificationRequest;
import org.apache.falcon.notification.service.request.RegexBasedDataNotificationRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


/**
 * Handler interface for DataNotificationRequests for Latest and Future EL Exps.
 */
public class RegexServiceHandler implements DataServiceHandler {
    private static final Logger LOG = LoggerFactory.getLogger(RegexServiceHandler.class);

    @Override
    public boolean handleRequest(DataNotificationRequest request) {
        RegexBasedDataNotificationRequest regexBasedDataNotificationRequest;
        if (request instanceof RegexBasedDataNotificationRequest) {
            regexBasedDataNotificationRequest = (RegexBasedDataNotificationRequest)request;
        } else {
            throw new IllegalArgumentException("Request should be Regex based to be processed");
        }
        try {
            Entity entity = EntityUtil.getEntity(EntityType.CLUSTER, regexBasedDataNotificationRequest.getCluster());
            Cluster clusterEntity = (Cluster) entity;
            Configuration conf = ClusterHelper.getConfiguration(clusterEntity);
            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(conf);
            return checkPaths(fs, regexBasedDataNotificationRequest);
        } catch (FalconException e) {
            LOG.error("Retrieving the Cluster Entity " + e);
        } catch (IOException e) {
            LOG.error("unable to get path");
        }
        return false;
    }

    private boolean checkPaths(FileSystem fs, RegexBasedDataNotificationRequest regexBasedDataNotificationRequest)
        throws IOException {
        Date startTime = new Date(regexBasedDataNotificationRequest.getStartTimeinMillis());
        Date endTime = new Date(regexBasedDataNotificationRequest.getEndTimeInMillis());
        Map<Path, Boolean> locations = regexBasedDataNotificationRequest.getLocationMap();
        if (locations == null) {
            locations = new HashMap<>();
        }
        String basePath = regexBasedDataNotificationRequest.getBasePath();
        int endInstance = regexBasedDataNotificationRequest.getEndInstance();
        int startInstance = regexBasedDataNotificationRequest.getStartInstance();
        int requiredPaths = Math.abs(endInstance - startInstance) + 1;
        int availablePaths = 0;
        int pathsAdded = 0;
        while (shouldRun(startTime, endTime, regexBasedDataNotificationRequest.getExpType())
                && pathsAdded < requiredPaths) {
            String pathString = EntityUtil.evaluateDependentPath(basePath, startTime);
            Path path = new Path(pathString);
            if (fs.exists(path)) {
                if (shouldAdd(availablePaths, startInstance, endInstance,
                        regexBasedDataNotificationRequest.getExpType())) {
                    if (locations.containsKey(path)) {
                        locations.put(path, true);
                    } else {
                        locations.put(new Path(path.toUri().getPath()), true);
                    }
                    pathsAdded++;
                }
                availablePaths++;
            }
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(startTime);
            int mul = getMultipleConstant(regexBasedDataNotificationRequest.getExpType());
            calendar.add(Calendar.MILLISECOND, (int) (mul * regexBasedDataNotificationRequest.getFrequencyInMillis()));
            startTime = calendar.getTime();
        }
        regexBasedDataNotificationRequest.setLocationMap(locations);
        if (pathsAdded ==  requiredPaths) {
            return true;
        }
        return false;
    }

    private boolean shouldAdd(int availablePaths, int startInstance, int endInstance, SchedulerUtil.EXPTYPE expType) {
        if (expType == SchedulerUtil.EXPTYPE.LATEST) {
            return (availablePaths >= endInstance && availablePaths <= startInstance);
        }
        return (availablePaths >= startInstance && availablePaths <= endInstance);
    }

    private boolean shouldRun(Date startTime, Date endTime, SchedulerUtil.EXPTYPE exptype) {
        if (exptype == SchedulerUtil.EXPTYPE.LATEST) {
            return startTime.compareTo(endTime) > 0;
        }
        return startTime.compareTo(endTime) < 0;
    }

    private int getMultipleConstant(SchedulerUtil.EXPTYPE expType) {
        if (expType == SchedulerUtil.EXPTYPE.LATEST) {
            return -1;
        }
        return 1;
    }

}
