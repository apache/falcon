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
package org.apache.falcon.cleanup;

import org.apache.falcon.FalconException;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.CatalogStorage;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Collection;

/**
 * Cleanup files relating to feed management workflows.
 */
public class FeedCleanupHandler extends AbstractCleanupHandler {

    @Override
    public void cleanup() throws FalconException {
        Collection<String> feeds = STORE.getEntities(EntityType.FEED);
        for (String feedName : feeds) {
            Feed feed;
            feed = STORE.get(EntityType.FEED, feedName);
            long retention = getRetention(feed, feed.getFrequency()
                    .getTimeUnit());
            for (org.apache.falcon.entity.v0.feed.Cluster cluster : feed
                    .getClusters().getClusters()) {
                Cluster currentCluster = STORE.get(EntityType.CLUSTER,
                        cluster.getName());
                if (currentCluster.getColo().equals(getCurrentColo())) {
                    LOG.info("Cleaning up logs & staged data for feed:" + feedName
                            + " in  cluster: " + cluster.getName() + " with retention: " + retention);
                    delete(currentCluster, feed, retention);
                    deleteStagedData(currentCluster, feed, retention);
                } else {
                    LOG.info("Ignoring cleanup for feed:" + feedName
                            + " in  cluster: " + cluster.getName() + " as this does not belong to current colo");
                }
            }

        }
    }

    /**
     * Delete the staging area used for replicating tables.
     *
     * @param cluster cluster hosting the staged data
     * @param feed feed entity
     * @param retention retention limit
     * @throws FalconException
     */
    private void deleteStagedData(Cluster cluster, Feed feed, long retention)
        throws FalconException {
        Storage storage = FeedHelper.createStorage(cluster, feed);
        if (storage.getType() == Storage.TYPE.FILESYSTEM) {  // FS does NOT use staging dirs
            return;
        }

        final CatalogStorage tableStorage = (CatalogStorage) storage;
        String stagingDir = FeedHelper.getStagingDir(cluster, feed, tableStorage, Tag.REPLICATION);
        Path stagingPath = new Path(stagingDir + "/*/*/*");  // stagingDir/dataOutPartitionValue/nominal-time/data
        FileSystem fs = getFileSystem(cluster);
        try {
            FileStatus[] paths = fs.globStatus(stagingPath);
            delete(cluster, feed, retention, paths);
        } catch (IOException e) {
            throw new FalconException(e);
        }
    }

    @Override
    protected Path getLogPath(Entity entity, String stagingPath) {
        return new Path(stagingPath, "falcon/workflows/feed/"
                + entity.getName() + "/logs/job-*/*/*");
    }
}
