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

package org.apache.falcon.lifecycle.engine.oozie;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.lifecycle.AbstractPolicyBuilderFactory;
import org.apache.falcon.lifecycle.PolicyBuilder;
import org.apache.falcon.util.ReflectionUtils;
import org.apache.falcon.util.StartupProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Builds feed lifecycle policies for Oozie workflow engine.
 */
public class OoziePolicyBuilderFactory extends AbstractPolicyBuilderFactory {

    private static Map<String, PolicyBuilder> registry = new HashMap<>();

    static {
        String builders = StartupProperties.get().getProperty("falcon.feed.lifecycle.policy.builders", "");
        if (StringUtils.isNotBlank(builders)) {
            for (String builder : builders.split(",")) {
                try {
                    PolicyBuilder policyBuilder = ReflectionUtils.getInstanceByClassName(builder);
                    registry.put(policyBuilder.getPolicyName(), policyBuilder);
                } catch (FalconException e) {
                    throw new RuntimeException("Couldn't load builder for " + builder.getClass().getSimpleName(), e);
                }
            }
        }
    }

    @Override
    public PolicyBuilder getPolicyBuilder(String policyName) throws FalconException {
        if (registry.containsKey(policyName)) {
            return registry.get(policyName);
        }
        throw new FalconException("Couldn't find builder for policy " + policyName);
    }
}
