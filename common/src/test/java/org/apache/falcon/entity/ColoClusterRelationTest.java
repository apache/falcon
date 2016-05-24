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

package org.apache.falcon.entity;

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Set;

/**
 * Tests for validating relationship between cluster to data center/co-location.
 */
@Test
public class ColoClusterRelationTest extends AbstractTestBase {

    private Cluster newCluster(String name, String colo) {
        Cluster cluster = new Cluster();
        cluster.setName(name);
        cluster.setColo(colo);
        return cluster;
    }

    @Test
    public void testMapping() throws Exception {
        Cluster cluster1 = newCluster("cluster1", "colo1");
        Cluster cluster2 = newCluster("cluster2", "colo1");
        Cluster cluster3 = newCluster("cluster3", "colo2");
        getStore().publish(EntityType.CLUSTER, cluster1);
        getStore().publish(EntityType.CLUSTER, cluster2);
        getStore().publish(EntityType.CLUSTER, cluster3);

        ColoClusterRelation relation = ColoClusterRelation.get();
        Set<String> clusters = relation.getClusters("colo1");
        Assert.assertNotNull(clusters);
        Assert.assertEquals(2, clusters.size());
        Assert.assertTrue(clusters.contains(cluster1.getName()));
        Assert.assertTrue(clusters.contains(cluster2.getName()));

        clusters = relation.getClusters("colo2");
        Assert.assertNotNull(clusters);
        Assert.assertEquals(1, clusters.size());
        Assert.assertTrue(clusters.contains(cluster3.getName()));

        getStore().remove(EntityType.CLUSTER, cluster1.getName());
        clusters = relation.getClusters("colo1");
        Assert.assertNotNull(clusters);
        Assert.assertEquals(1, clusters.size());
        Assert.assertTrue(clusters.contains(cluster2.getName()));

        getStore().remove(EntityType.CLUSTER, cluster2.getName());
        clusters = relation.getClusters("colo1");
        Assert.assertNotNull(clusters);
        Assert.assertEquals(0, clusters.size());

        Cluster updatedCluster3 = new Cluster();
        updatedCluster3.setName(cluster3.getName());
        updatedCluster3.setColo("colo3");
        try {
            getStore().initiateUpdate(updatedCluster3);
            getStore().update(EntityType.CLUSTER, updatedCluster3);
        } finally {
            getStore().cleanupUpdateInit();
        }

        relation = ColoClusterRelation.get();
        clusters = relation.getClusters("colo3");
        Assert.assertNotNull(clusters);
        Assert.assertEquals(1, clusters.size());
        Assert.assertTrue(clusters.contains(updatedCluster3.getName()));

        clusters = relation.getClusters("colo2");
        Assert.assertNotNull(clusters);
        Assert.assertEquals(0, clusters.size());
    }
}
