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
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Lifecycle;
import org.apache.falcon.lifecycle.LifecyclePolicy;
import org.apache.falcon.oozie.OozieBundleBuilder;
import org.apache.falcon.oozie.OozieCoordinatorBuilder;
import org.apache.falcon.service.LifecyclePolicyMap;
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

    @Override
    protected List<Properties> buildCoords(Cluster cluster, Path buildPath) throws FalconException {
        // if feed has lifecycle defined - then use it to create coordinator and wf else fall back
        List<Properties> props = new ArrayList<>();
        Lifecycle lifecycle = this.entity.getLifecycle();
        if (lifecycle != null) {
            for (String name : FeedHelper.getPolicies(this.entity, cluster.getName())) {
                LifecyclePolicy policy = LifecyclePolicyMap.get().get(name);
                if (policy == null) {
                    LOG.error("Couldn't find lifecycle policy for name:{}", name);
                    throw new FalconException("Invalid policy name " + name);
                }
                Properties appProps = policy.build(cluster, buildPath, this.entity);
                if (appProps != null) {
                    props.add(appProps);
                }
            }
        } else {
            List<Properties> evictionProps =
                    OozieCoordinatorBuilder.get(entity, Tag.RETENTION).buildCoords(cluster, buildPath);
            if (evictionProps != null) {
                props.addAll(evictionProps);
            }
        }
        List<Properties> replicationProps = OozieCoordinatorBuilder.get(entity, Tag.REPLICATION)
                .buildCoords(cluster, buildPath);
        if (replicationProps != null) {
            props.addAll(replicationProps);
        }

        List<Properties> importProps = OozieCoordinatorBuilder.get(entity, Tag.IMPORT).buildCoords(cluster, buildPath);
        if (importProps != null) {
            props.addAll(importProps);
        }

        if (!props.isEmpty()) {
            copySharedLibs(cluster, new Path(getLibPath(buildPath)));
        }
        return props;
    }

    @Override
    public String getLibPath(Path buildPath) {
        return new Path(buildPath, "lib").toString();
    }
}
