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

package org.apache.falcon.service;

import org.apache.falcon.FalconException;
import org.apache.falcon.lifecycle.FeedLifecycleStage;
import org.apache.falcon.lifecycle.LifecyclePolicy;
import org.apache.falcon.util.ReflectionUtils;
import org.apache.falcon.util.StartupProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Stores all internal and external feed lifecycle policies.
 */
public final class LifecyclePolicyMap implements FalconService {
    private static final Logger LOG = LoggerFactory.getLogger(LifecyclePolicyMap.class);
    private static final LifecyclePolicyMap STORE = new LifecyclePolicyMap();

    private final Map<String, LifecyclePolicy> policyMap = new HashMap<>();

    private LifecyclePolicyMap() {}

    public static LifecyclePolicyMap get() {
        return STORE;
    }

    public LifecyclePolicy get(String policyName) {
        return policyMap.get(policyName);
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    public void init() throws FalconException {
        String[] policyNames = StartupProperties.get().getProperty("falcon.feed.lifecycle.policies").split(",");
        for (String name : policyNames) {
            LifecyclePolicy policy = ReflectionUtils.getInstanceByClassName(name);
            LOG.debug("Loaded policy : {} for stage : {}", policy.getName(), policy.getStage());
            policyMap.put(policy.getName(), policy);
        }
        validate();
    }

    @Override
    public void destroy() throws FalconException {
        policyMap.clear();
    }

    // validate that default policy for each stage is available
    private void validate() throws FalconException {
        for (FeedLifecycleStage stage : FeedLifecycleStage.values()) {
            if (!policyMap.containsKey(stage.getDefaultPolicyName())) {
                throw new FalconException("Default Policy: " + stage.getDefaultPolicyName()
                        + " for stage: " + stage.name() + "was not found.");
            }
        }
    }
}
