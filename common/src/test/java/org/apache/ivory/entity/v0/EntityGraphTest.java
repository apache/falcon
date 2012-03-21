package org.apache.ivory.entity.v0;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.feed.Clusters;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.process.*;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.service.Services;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Set;

/**
 * User: sriksun
 * Date: 19/03/12
 */
public class EntityGraphTest {

    private ConfigurationStore store;

    private EntityGraph graph;

    @BeforeClass
    @AfterClass
    public void cleanupStore() throws Exception {
        graph = EntityGraph.get();
        store = ConfigurationStore.get();
        for (EntityType type : EntityType.values()) {
            for (String name : store.getEntities(type)) {
                store.remove(type, name);
            }
        }
    }

    @Test
    public void testOnAdd() throws Exception {

        Process process = new Process();
        process.setName("p1");
        Cluster cluster = new Cluster();
        cluster.setName("c1");
        Feed f1 = addInput(process, "f1", cluster);
        Feed f2 = addInput(process, "f2", cluster);
        Feed f3 = addOutput(process, "f3", cluster);
        Feed f4 = addOutput(process, "f4", cluster);
        process.setCluster(new org.apache.ivory.entity.v0.process.Cluster());
        process.getCluster().setName("c1");

        store.publish(EntityType.CLUSTER, cluster);
        store.publish(EntityType.FEED, f1);
        store.publish(EntityType.FEED, f2);
        store.publish(EntityType.FEED, f3);
        store.publish(EntityType.FEED, f4);
        store.publish(EntityType.PROCESS, process);

        Set<Entity> entities = graph.getDependents(process);
        Assert.assertEquals(entities.size(), 5);
        Assert.assertTrue(entities.contains(cluster));
        Assert.assertTrue(entities.contains(f1));
        Assert.assertTrue(entities.contains(f2));
        Assert.assertTrue(entities.contains(f3));
        Assert.assertTrue(entities.contains(f4));

        entities = graph.getDependents(f1);
        Assert.assertEquals(entities.size(), 2);
        Assert.assertTrue(entities.contains(process));
        Assert.assertTrue(entities.contains(cluster));

        entities = graph.getDependents(f2);
        Assert.assertEquals(entities.size(), 2);
        Assert.assertTrue(entities.contains(process));
        Assert.assertTrue(entities.contains(cluster));

        entities = graph.getDependents(f3);
        Assert.assertEquals(entities.size(), 2);
        Assert.assertTrue(entities.contains(process));
        Assert.assertTrue(entities.contains(cluster));

        entities = graph.getDependents(f4);
        Assert.assertEquals(entities.size(), 2);
        Assert.assertTrue(entities.contains(process));
        Assert.assertTrue(entities.contains(cluster));

        entities = graph.getDependents(cluster);
        Assert.assertEquals(entities.size(), 5);
        Assert.assertTrue(entities.contains(process));
        Assert.assertTrue(entities.contains(f1));
        Assert.assertTrue(entities.contains(f2));
        Assert.assertTrue(entities.contains(f3));
        Assert.assertTrue(entities.contains(f4));
    }

    private Feed addInput(Process process, String feed, Cluster cluster) {
        if (process.getInputs() == null) process.setInputs(new Inputs());
        Inputs inputs = process.getInputs();
        Input input = new Input();
        input.setFeed(feed);
        inputs.getInput().add(input);
        Feed f1 = new Feed();
        f1.setName(feed);
        Clusters clusters = new Clusters();
        f1.setClusters(clusters);
        org.apache.ivory.entity.v0.feed.Cluster feedCluster =
                new org.apache.ivory.entity.v0.feed.Cluster();
        feedCluster.setName(cluster.getName());
        clusters.getCluster().add(feedCluster);
        return f1; 
    }

    private void attachInput(Process process, Feed feed) {
        if (process.getInputs() == null) process.setInputs(new Inputs());
        Inputs inputs = process.getInputs();
        Input input = new Input();
        input.setFeed(feed.getName());
        inputs.getInput().add(input);
    }

    private Feed addOutput(Process process, String feed, Cluster cluster) {
        if (process.getOutputs() == null) process.setOutputs(new Outputs());
        Outputs Outputs = process.getOutputs();
        Output Output = new Output();
        Output.setFeed(feed);
        Outputs.getOutput().add(Output);
        Feed f1 = new Feed();
        f1.setName(feed);
        Clusters clusters = new Clusters();
        f1.setClusters(clusters);
        org.apache.ivory.entity.v0.feed.Cluster feedCluster =
                new org.apache.ivory.entity.v0.feed.Cluster();
        feedCluster.setName(cluster.getName());
        clusters.getCluster().add(feedCluster);
        return f1;
    }

    @Test
    public void testOnRemove() throws Exception {
        Process process = new Process();
        process.setName("rp1");
        Cluster cluster = new Cluster();
        cluster.setName("rc1");
        process.setCluster(new org.apache.ivory.entity.v0.process.Cluster());
        process.getCluster().setName("rc1");

        store.publish(EntityType.CLUSTER, cluster);
        store.publish(EntityType.PROCESS, process);

        Set<Entity> entities = graph.getDependents(process);
        Assert.assertEquals(entities.size(), 1);
        Assert.assertTrue(entities.contains(cluster));

        entities = graph.getDependents(cluster);
        Assert.assertEquals(entities.size(), 1);
        Assert.assertTrue(entities.contains(process));

        store.remove(EntityType.PROCESS, process.getName());
        entities = graph.getDependents(cluster);
        Assert.assertTrue(entities == null);

        entities = graph.getDependents(process);
        Assert.assertTrue(entities == null);
    }

    @Test
    public void testOnRemove2() throws Exception {

        Process p1 = new Process();
        p1.setName("ap1");
        Process p2 = new Process();
        p2.setName("ap2");
        Cluster cluster = new Cluster();
        cluster.setName("ac1");
        Feed f1 = addInput(p1, "af1", cluster);
        Feed f3 = addOutput(p1, "af3", cluster);
        Feed f2 = addOutput(p2, "af2", cluster);
        attachInput(p2, f3);
        p1.setCluster(new org.apache.ivory.entity.v0.process.Cluster());
        p1.getCluster().setName("ac1");
        p2.setCluster(new org.apache.ivory.entity.v0.process.Cluster());
        p2.getCluster().setName("ac1");

        store.publish(EntityType.CLUSTER, cluster);
        store.publish(EntityType.FEED, f1);
        store.publish(EntityType.FEED, f2);
        store.publish(EntityType.FEED, f3);
        store.publish(EntityType.PROCESS, p1);
        store.publish(EntityType.PROCESS, p2);

        Set<Entity> entities = graph.getDependents(p1);
        Assert.assertEquals(entities.size(), 3);
        Assert.assertTrue(entities.contains(cluster));
        Assert.assertTrue(entities.contains(f1));
        Assert.assertTrue(entities.contains(f3));

        entities = graph.getDependents(p2);
        Assert.assertEquals(entities.size(), 3);
        Assert.assertTrue(entities.contains(cluster));
        Assert.assertTrue(entities.contains(f2));
        Assert.assertTrue(entities.contains(f3));

        entities = graph.getDependents(f1);
        Assert.assertEquals(entities.size(), 2);
        Assert.assertTrue(entities.contains(p1));
        Assert.assertTrue(entities.contains(cluster));

        entities = graph.getDependents(f2);
        Assert.assertEquals(entities.size(), 2);
        Assert.assertTrue(entities.contains(p2));
        Assert.assertTrue(entities.contains(cluster));

        entities = graph.getDependents(f3);
        Assert.assertEquals(entities.size(), 3);
        Assert.assertTrue(entities.contains(p2));
        Assert.assertTrue(entities.contains(p1));
        Assert.assertTrue(entities.contains(cluster));

        entities = graph.getDependents(cluster);
        Assert.assertEquals(entities.size(), 5);
        Assert.assertTrue(entities.contains(p1));
        Assert.assertTrue(entities.contains(p2));
        Assert.assertTrue(entities.contains(f1));
        Assert.assertTrue(entities.contains(f2));
        Assert.assertTrue(entities.contains(f3));

        store.remove(EntityType.PROCESS, p2.getName());
        store.remove(EntityType.FEED, f2.getName());

        entities = graph.getDependents(p1);
        Assert.assertEquals(entities.size(), 3);
        Assert.assertTrue(entities.contains(cluster));
        Assert.assertTrue(entities.contains(f1));
        Assert.assertTrue(entities.contains(f3));

        entities = graph.getDependents(p2);
        Assert.assertTrue(entities == null);

        entities = graph.getDependents(f1);
        Assert.assertEquals(entities.size(), 2);
        Assert.assertTrue(entities.contains(p1));
        Assert.assertTrue(entities.contains(cluster));

        entities = graph.getDependents(f2);
        Assert.assertTrue(entities == null);

        entities = graph.getDependents(f3);
        Assert.assertEquals(entities.size(), 2);
        Assert.assertTrue(entities.contains(p1));
        Assert.assertTrue(entities.contains(cluster));

        entities = graph.getDependents(cluster);
        Assert.assertEquals(entities.size(), 3);
        Assert.assertTrue(entities.contains(p1));
        Assert.assertTrue(entities.contains(f1));
        Assert.assertTrue(entities.contains(f3));
    }

    @Test
    public void testOnChange() throws Exception {
    }
}
