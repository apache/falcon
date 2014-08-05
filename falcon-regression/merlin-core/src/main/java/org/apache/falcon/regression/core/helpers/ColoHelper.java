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

package org.apache.falcon.regression.core.helpers;

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.regression.core.interfaces.EntityHelperFactory;
import org.apache.falcon.regression.core.interfaces.IEntityManagerHelper;

/** Helper class to work with a colo. */
public class ColoHelper {
    private final IEntityManagerHelper clusterHelper;
    private final IEntityManagerHelper processHelper;
    private final IEntityManagerHelper feedHelper;
    private String prefix;

    public ColoHelper(String prefix) {
        this.prefix = prefix;
        clusterHelper = EntityHelperFactory.getEntityHelper(EntityType.CLUSTER, prefix);
        processHelper = EntityHelperFactory.getEntityHelper(EntityType.PROCESS, prefix);
        feedHelper = EntityHelperFactory.getEntityHelper(EntityType.FEED, prefix);
    }

    public IEntityManagerHelper getClusterHelper() {
        return clusterHelper;
    }

    public IEntityManagerHelper getFeedHelper() {
        return feedHelper;
    }

    public IEntityManagerHelper getProcessHelper() {
        return processHelper;
    }

    public String getPrefix() {
        return prefix;
    }
}
