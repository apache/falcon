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

package org.apache.falcon.lifecycle;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.hadoop.fs.Path;

import java.util.Properties;

/**
 * Interface for all policies in feed lifecycle.
 */
public interface LifecyclePolicy {

    /**
     * Returns the name of the policy. Name of policy must be unique as it is used as an identifier.
     * @return name of the policy
     */
    String getName();

    /**
     * Returns the stage to which the policy belongs.
     * @return stage to which the policy belongs.
     */
    FeedLifecycleStage getStage();

    /**
     * Validates the configurations as per this policy.
     * @param feed Parent feed for which the policy is configured.
     * @param clusterName cluster to be used as context for validation.
     * @throws FalconException
     */
    void validate(Feed feed, String clusterName) throws FalconException;

    /**
     * Builds workflow engine artifacts.
     * @param cluster cluster to be used as context
     * @param buildPath base path to be used for storing the artifacts.
     * @param feed Parent feed.
     * @return Properties to be passed to the caller e.g. bundle in case of oozie workflow engine.
     * @throws FalconException
     */
    Properties build(Cluster cluster, Path buildPath, Feed feed) throws FalconException;

}
