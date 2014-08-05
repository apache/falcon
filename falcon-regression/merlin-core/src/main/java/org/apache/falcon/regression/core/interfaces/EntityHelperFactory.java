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

package org.apache.falcon.regression.core.interfaces;

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.regression.core.helpers.ClusterEntityHelperImpl;
import org.apache.falcon.regression.core.helpers.DataEntityHelperImpl;
import org.apache.falcon.regression.core.helpers.ProcessEntityHelperImpl;

/** Factory class to create helper objects. */
public final class EntityHelperFactory {
    private EntityHelperFactory() {
    }

    public static IEntityManagerHelper getEntityHelper(EntityType type, String prefix) {
        switch (type) {
        case FEED:
            return new DataEntityHelperImpl(prefix);
        case CLUSTER:
            return new ClusterEntityHelperImpl(prefix);
        case PROCESS:
            return new ProcessEntityHelperImpl(prefix);
        default:
            return null;
        }
    }
}
