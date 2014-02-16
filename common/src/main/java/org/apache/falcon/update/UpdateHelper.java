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

package org.apache.falcon.update;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Partition;
import org.apache.falcon.entity.v0.feed.Partitions;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Inputs;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Helper methods to facilitate entity updates.
 */
public final class UpdateHelper {
    private static final Logger LOG = Logger.getLogger(UpdateHelper.class);

    private static final String[] FEED_FIELDS = new String[]{"partitions", "groups", "lateArrival.cutOff",
                                                             "schema.location", "schema.provider",
                                                             "ACL.group", "ACL.owner", "ACL.permission", };
    private static final String[] PROCESS_FIELDS = new String[]{"retry.policy", "retry.delay", "retry.attempts",
                                                                "lateProcess.policy", "lateProcess.delay",
                                                                "lateProcess.lateInputs[\\d+].input",
                                                                "lateProcess.lateInputs[\\d+].workflowPath", };

    private UpdateHelper() {}

    public static boolean isEntityUpdated(Entity oldEntity, Entity newEntity, String cluster) throws FalconException {
        Entity oldView = EntityUtil.getClusterView(oldEntity, cluster);
        Entity newView = EntityUtil.getClusterView(newEntity, cluster);
        switch (oldEntity.getEntityType()) {
        case FEED:
            return !EntityUtil.equals(oldView, newView, FEED_FIELDS);

        case PROCESS:
            return !EntityUtil.equals(oldView, newView, PROCESS_FIELDS);

        default:
        }
        throw new IllegalArgumentException("Unhandled entity type " + oldEntity.getEntityType());
    }

    //Read checksum file
    private static Map<String, String> readChecksums(FileSystem fs, Path path) throws FalconException {
        try {
            Map<String, String> checksums = new HashMap<String, String>();
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("=");
                    checksums.put(parts[0], parts[1]);
                }
            } finally {
                reader.close();
            }
            return checksums;
        } catch (IOException e) {
            throw new FalconException(e);
        }
    }

    //Checks if the user workflow or lib is updated
    public static boolean isWorkflowUpdated(String cluster, Entity entity) throws FalconException {
        if (entity.getEntityType() != EntityType.PROCESS) {
            return false;
        }

        try {
            Process process = (Process) entity;
            org.apache.falcon.entity.v0.cluster.Cluster clusterEntity =
                    ConfigurationStore.get().get(EntityType.CLUSTER, cluster);
            Path bundlePath = EntityUtil.getLastCommittedStagingPath(clusterEntity, process);
            if (bundlePath == null) {
                return true;
            }

            Path checksum = new Path(bundlePath, EntityUtil.PROCESS_CHECKSUM_FILE);
            Configuration conf = ClusterHelper.getConfiguration(clusterEntity);
            FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(conf);
            if (!fs.exists(checksum)) {
                //Update if there is no checksum file(for migration)
                return true;
            }
            Map<String, String> checksums = readChecksums(fs, checksum);

            //Get checksum from user wf/lib
            Map<String, String> wfPaths = checksumAndCopy(fs, new Path(process.getWorkflow().getPath()), null);
            if (process.getWorkflow().getLib() != null) {
                wfPaths.putAll(checksumAndCopy(fs, new Path(process.getWorkflow().getLib()), null));
            }

            //Update if the user wf/lib is updated i.e., if checksums are different
            return !wfPaths.equals(checksums);
        } catch (IOException e) {
            throw new FalconException(e);
        }
    }

    /**
     * Recursively traverses each file and tracks checksum. If dest != null, each traversed file is copied to dest
     * @param fs FileSystem
     * @param src file/directory
     * @param dest directory always
     * @return checksums
     * @throws FalconException
     */
    public static Map<String, String> checksumAndCopy(FileSystem fs, Path src, Path dest) throws FalconException {
        try {
            Configuration conf = new Configuration();
            Map<String, String> paths = new HashMap<String, String>();
            if (dest != null && !fs.exists(dest) && !fs.mkdirs(dest)) {
                throw new FalconException("mkdir failed on " + dest);
            }

            if (fs.isFile(src)) {
                paths.put(src.toString(), fs.getFileChecksum(src).toString());
                if (dest != null) {
                    Path target = new Path(dest, src.getName());
                    FileUtil.copy(fs, src, fs, target, false, conf);
                    LOG.debug("Copied " + src + " to " + target);
                }
            } else {
                FileStatus[] files = fs.listStatus(src);
                if (files != null) {
                    for (FileStatus file : files) {
                        if (fs.isFile(file.getPath())) {
                            paths.putAll(checksumAndCopy(fs, file.getPath(), dest));
                        } else {
                            paths.putAll(checksumAndCopy(fs, file.getPath(),
                                    ((dest == null) ? null : new Path(dest, file.getPath().getName()))));
                        }
                    }
                }
            }
            return paths;
        } catch(IOException e) {
            throw new FalconException(e);
        }
    }

    public static boolean shouldUpdate(Entity oldEntity, Entity newEntity, Entity affectedEntity)
        throws FalconException {
        if (oldEntity.getEntityType() == EntityType.FEED && affectedEntity.getEntityType() == EntityType.PROCESS) {
            return shouldUpdate((Feed) oldEntity, (Feed) newEntity, (Process) affectedEntity);
        } else {
            LOG.debug(newEntity.toShortString());
            LOG.debug(affectedEntity.toShortString());
            throw new FalconException("Don't know what to do. Unexpected scenario");
        }
    }

    public static boolean shouldUpdate(Feed oldFeed, Feed newFeed, Process affectedProcess)
        throws FalconException {
        Storage oldFeedStorage = FeedHelper.createStorage(oldFeed);
        Storage newFeedStorage = FeedHelper.createStorage(newFeed);

        if (!oldFeedStorage.isIdentical(newFeedStorage)) {
            return true;
        }
        LOG.debug(oldFeed.toShortString() + ": Storage identical. Ignoring...");

        if (!oldFeed.getFrequency().equals(newFeed.getFrequency())) {
            return true;
        }
        LOG.debug(oldFeed.toShortString() + ": Frequency identical. Ignoring...");

        // it is not possible to have oldFeed partitions as non empty and
        // new being empty. validator should have gated this.
        // Also if new partitions are added and old is empty, then there is
        // nothing
        // to update in process
        boolean partitionApplicable = false;
        Inputs affectedInputs = affectedProcess.getInputs();
        if (affectedInputs != null && affectedInputs.getInputs() != null) {
            for (Input input : affectedInputs.getInputs()) {
                if (input.getFeed().equals(oldFeed.getName())) {
                    if (input.getPartition() != null && !input.getPartition().isEmpty()) {
                        partitionApplicable = true;
                    }
                }
            }
            if (partitionApplicable) {
                LOG.debug("Partitions are applicable. Checking ...");
                if (newFeed.getPartitions() != null && oldFeed.getPartitions() != null) {
                    List<String> newParts = getPartitions(newFeed.getPartitions());
                    List<String> oldParts = getPartitions(oldFeed.getPartitions());
                    if (newParts.size() != oldParts.size()) {
                        return true;
                    }
                    if (!newParts.containsAll(oldParts)) {
                        return true;
                    }
                }
                LOG.debug(oldFeed.toShortString() + ": Partitions identical. Ignoring...");
            }
        }

        for (Cluster cluster : affectedProcess.getClusters().getClusters()) {
            oldFeedStorage = FeedHelper.createStorage(cluster.getName(), oldFeed);
            newFeedStorage = FeedHelper.createStorage(cluster.getName(), newFeed);

            if (!FeedHelper.getCluster(oldFeed, cluster.getName()).getValidity().getStart()
                    .equals(FeedHelper.getCluster(newFeed, cluster.getName()).getValidity().getStart())
                    || !oldFeedStorage.isIdentical(newFeedStorage)) {
                return true;
            }
            LOG.debug(oldFeed.toShortString() + ": Feed on cluster" + cluster.getName() + " identical. Ignoring...");
        }

        return false;
    }

    private static List<String> getPartitions(Partitions partitions) {
        List<String> parts = new ArrayList<String>();
        for (Partition partition : partitions.getPartitions()) {
            parts.add(partition.getName());
        }
        return parts;
    }
}
