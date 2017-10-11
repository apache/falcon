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

package org.apache.falcon.entity.v0;

import org.apache.falcon.FalconException;
import org.apache.falcon.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Helper methods to check integrity of entity.
 */
public final class EntityIntegrityChecker {

    private EntityIntegrityChecker() {}

    public static Pair<String, EntityType>[] referencedBy(Entity entity) throws FalconException {
        Set<Entity> deps = EntityGraph.get().getDependents(entity);
        switch (entity.getEntityType()) {
        case CLUSTER:
            return filter(deps, EntityType.FEED, EntityType.PROCESS);

        case FEED:
            return filter(deps, EntityType.PROCESS);

        case DATASOURCE:
            return filter(deps, EntityType.FEED);

        default:
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private static Pair<String, EntityType>[] filter(Set<Entity> deps, EntityType... types) {
        List<Pair<String, EntityType>> filteredSet = new ArrayList<Pair<String, EntityType>>();
        List<EntityType> validTypes = Arrays.asList(types);
        for (Entity dep : deps) {
            if (validTypes.contains(dep.getEntityType())) {
                filteredSet.add(Pair.of(dep.getName(), dep.getEntityType()));
            }
        }
        return filteredSet.toArray(new Pair[0]);
    }
}
