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

package org.apache.falcon.lifecycle.retention;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.lifecycle.AbstractPolicyBuilderFactory;
import org.apache.falcon.lifecycle.FeedLifecycleStage;
import org.apache.falcon.lifecycle.LifecyclePolicy;
import org.apache.falcon.lifecycle.PolicyBuilder;
import org.apache.falcon.workflow.WorkflowEngineFactory;
import org.apache.hadoop.fs.Path;

import java.util.Properties;

/**
 * All retention policies must implement this interface.
 */
public abstract class RetentionPolicy implements LifecyclePolicy {

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public FeedLifecycleStage getStage() {
        return FeedLifecycleStage.RETENTION;
    }

    @Override
    public Properties build(Cluster cluster, Path buildPath, Feed feed) throws FalconException {
        AbstractPolicyBuilderFactory factory = WorkflowEngineFactory.getLifecycleEngine();
        PolicyBuilder builder = factory.getPolicyBuilder(getName());
        return builder.build(cluster, buildPath, feed);
    }
}
