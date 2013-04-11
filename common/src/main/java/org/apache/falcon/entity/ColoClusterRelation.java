/*
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

package org.apache.falcon.entity;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.service.ConfigurationChangeListener;

public class ColoClusterRelation implements ConfigurationChangeListener {
    private static final ConcurrentHashMap<String, Set<String>> coloClusterMap = new ConcurrentHashMap<String, Set<String>>();
    private static final ColoClusterRelation instance = new ColoClusterRelation();

    private ColoClusterRelation() {
    }

    public static ColoClusterRelation get() {
        return instance;
    }

    public Set<String> getClusters(String colo) {
        if (coloClusterMap.containsKey(colo))
            return coloClusterMap.get(colo);
        return new HashSet<String>();
    }

    @Override
    public void onAdd(Entity entity) {
        if (entity.getEntityType() != EntityType.CLUSTER)
            return;

        Cluster cluster = (Cluster) entity;
        coloClusterMap.putIfAbsent(cluster.getColo(), new HashSet<String>());
        coloClusterMap.get(cluster.getColo()).add(cluster.getName());
    }

    @Override
    public void onRemove(Entity entity) {
        if (entity.getEntityType() != EntityType.CLUSTER)
            return;

        Cluster cluster = (Cluster) entity;
        coloClusterMap.get(cluster.getColo()).remove(cluster.getName());
        if (coloClusterMap.get(cluster.getColo()).isEmpty())
            coloClusterMap.remove(cluster.getColo());
    }

    @Override
    public void onChange(Entity oldEntity, Entity newEntity) throws FalconException {
        if (oldEntity.getEntityType() != EntityType.CLUSTER)
            return;
        throw new FalconException("change shouldn't be supported on cluster!");
    }
}
