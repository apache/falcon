package org.apache.ivory.entity;

import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.Set;

import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;

@Test
public class ColoClusterRelationTest extends AbstractTestBase{
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
        ConfigurationStore store = ConfigurationStore.get();
        store.publish(EntityType.CLUSTER, cluster1);
        store.publish(EntityType.CLUSTER, cluster2);
        store.publish(EntityType.CLUSTER, cluster3);
        
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

        store.remove(EntityType.CLUSTER, cluster1.getName());
        clusters = relation.getClusters("colo1");
        Assert.assertNotNull(clusters);
        Assert.assertEquals(1, clusters.size());
        Assert.assertTrue(clusters.contains(cluster2.getName()));
        
        store.remove(EntityType.CLUSTER, cluster2.getName());
        clusters = relation.getClusters("colo1");
        Assert.assertNotNull(clusters);
        Assert.assertEquals(0, clusters.size());
    }
}
