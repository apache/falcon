package org.apache.ivory.update;

import java.util.ArrayList;
import java.util.List;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.FeedHelper;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.feed.LocationType;
import org.apache.ivory.entity.v0.feed.Partition;
import org.apache.ivory.entity.v0.feed.Partitions;
import org.apache.ivory.entity.v0.process.Cluster;
import org.apache.ivory.entity.v0.process.Input;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.log4j.Logger;

public final class UpdateHelper {
    private static final Logger LOG = Logger.getLogger(UpdateHelper.class);
    private static final String[] FEED_FIELDS = new String[] { "partitions", "groups", "lateArrival.cutOff", "schema.location", "schema.provider",
        "ACL.group", "ACL.owner", "ACL.permission"};
    private static final String[] PROCESS_FIELDS = new String[] { "retry.policy", "retry.delay", "retry.attempts", 
        "lateProcess.policy", "lateProcess.delay", "lateProcess.lateInputs[\\d+].input", "lateProcess.lateInputs[\\d+].workflowPath"};
    
    public static boolean shouldUpdate(Entity oldEntity, Entity newEntity, String cluster) throws IvoryException {
        Entity oldView = EntityUtil.getClusterView(oldEntity, cluster);
        Entity newView = EntityUtil.getClusterView(newEntity, cluster);
        switch(oldEntity.getEntityType()) {
            case FEED:
                if(EntityUtil.equals(oldView, newView, FEED_FIELDS))
                    return false;
                return true;
                
            case PROCESS:
                if(EntityUtil.equals(oldView, newView, PROCESS_FIELDS))
                    return false;
                return true;
        }
        throw new IllegalArgumentException("Unhandled entity type " + oldEntity.getEntityType());
    }

    public static boolean shouldUpdate(Entity oldEntity, Entity newEntity, Entity affectedEntity) throws IvoryException {
        if (oldEntity.getEntityType() == EntityType.FEED && affectedEntity.getEntityType() == EntityType.PROCESS) {
            return shouldUpdate((Feed) oldEntity, (Feed) newEntity, (Process) affectedEntity);
        } else {
            LOG.debug(newEntity.toShortString());
            LOG.debug(affectedEntity.toShortString());
            throw new IvoryException("Don't know what to do. Unexpected scenario");
        }
    }

    public static boolean shouldUpdate(Feed oldFeed, Feed newFeed, Process affectedProcess) {
        if (!FeedHelper.getLocation(oldFeed, LocationType.DATA).getPath()
                .equals(FeedHelper.getLocation(newFeed, LocationType.DATA).getPath()))
            return true;
        LOG.debug(oldFeed.toShortString() + ": Location identical. Ignoring...");

        if (!oldFeed.getFrequency().equals(newFeed.getFrequency()))
            return true;
        LOG.debug(oldFeed.toShortString() + ": Frequency identical. Ignoring...");

        // it is not possible to have oldFeed partitions as non empty and
        // new being empty. validator should have gated this.
        // Also if new partitions are added and old is empty, then there is
        // nothing
        // to update in process
        boolean partitionApplicable = false;
        for (Input input : affectedProcess.getInputs().getInputs()) {
            if (input.getFeed().equals(oldFeed.getName())) {
                if (input.getPartition() != null && !input.getPartition().isEmpty()) {
                    partitionApplicable = true;
                }
            }
        }
        if (partitionApplicable) {
            LOG.debug("Partitions are applicable. Checking ...");
            if (newFeed.getPartitions() != null && oldFeed.getPartitions() != null) {
                List<String> newParts = getPartitions(newFeed.getPartitions());
                List<String> oldParts = getPartitions(oldFeed.getPartitions());
                if (newParts.size() != oldParts.size())
                    return true;
                if (!newParts.containsAll(oldParts))
                    return true;
            }
            LOG.debug(oldFeed.toShortString() + ": Partitions identical. Ignoring...");
        }

        for (Cluster cluster : affectedProcess.getClusters().getClusters()) {
            if (!FeedHelper.getCluster(oldFeed, cluster.getName()).getValidity().getStart()
                    .equals(FeedHelper.getCluster(newFeed, cluster.getName()).getValidity().getStart()))
                return true;
            LOG.debug(oldFeed.toShortString() + ": Feed start on cluster" + cluster.getName() + " identical. Ignoring...");
        }

        return false;
    }

    private static List<String> getPartitions(Partitions partitions) {
        List<String> parts = new ArrayList<String>();
        for (Partition partition : partitions.getPartitions()) {
            parts.add(partition.getName());
        }
        return parts;
    }
}
