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

package org.apache.falcon.oozie.feed;

import org.apache.falcon.FalconException;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.oozie.OozieBundleBuilder;
import org.apache.falcon.oozie.OozieCoordinatorBuilder;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Builds oozie bundle for the feed.
 */
public class FeedBundleBuilder extends OozieBundleBuilder<Feed> {
    public FeedBundleBuilder(Feed entity) {
        super(entity);
    }

    @Override protected Path getLibPath(Cluster cluster, Path buildPath) {
        return new Path(buildPath, "lib");
    }

    @Override protected List<Properties> doBuild(Cluster cluster, Path buildPath) throws FalconException {
        List<Properties> props = new ArrayList<Properties>();
        List<Properties> evictionProps =
            OozieCoordinatorBuilder.get(entity, Tag.RETENTION).buildCoords(cluster, buildPath);
        if (evictionProps != null) {
            props.addAll(evictionProps);
        }

        List<Properties> replicationProps = OozieCoordinatorBuilder.get(entity, Tag.REPLICATION).buildCoords(cluster,
            buildPath);
        if (replicationProps != null) {
            props.addAll(replicationProps);
        }

        if (!props.isEmpty()) {
            copySharedLibs(cluster, getLibPath(cluster, buildPath));
        }

        return props;
    }
}
