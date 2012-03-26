package org.apache.ivory.update;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.feed.*;
import org.apache.ivory.entity.v0.process.Input;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class UpdateHelper {
    private static final Logger LOG = Logger.getLogger(UpdateHelper.class);

    public static boolean shouldUpdate(Entity oldEntity, Entity newEntity,
                                       Entity affectedEntity) throws IvoryException {
        if (oldEntity.getEntityType() == EntityType.FEED &&
                affectedEntity.getEntityType() == EntityType.PROCESS) {
            return shouldUpdate((Feed)oldEntity, (Feed)newEntity, (Process)affectedEntity);
        } else {
            LOG.debug(newEntity.toShortString());
            LOG.debug(affectedEntity.toShortString());
            throw new IvoryException("Don't know what to do. Unexpected scenario");
        }
    }

    public static boolean shouldUpdate(Feed oldFeed, Feed newFeed,
                                       Process affectedProcess) throws IvoryException {

        if (!oldFeed.getLateArrival().getCutOff().
                equals(newFeed.getLateArrival().getCutOff())) return true;
        LOG.debug(oldFeed.toShortString() + ": late-cutoff identical. Ignoring...");

        if (!oldFeed.getLocations().get(LocationType.DATA).getPath().
                equals(newFeed.getLocations().get(LocationType.DATA).getPath())) return true;
        LOG.debug(oldFeed.toShortString() + ": Location identical. Ignoring...");

        if (!oldFeed.getFrequency().equals(newFeed.getFrequency())) return true;
        LOG.debug(oldFeed.toShortString() + ": Frequency identical. Ignoring...");

        if (oldFeed.getPeriodicity() != newFeed.getPeriodicity()) return true;
        LOG.debug(oldFeed.toShortString() + ": Periodicity identical. Ignoring...");

        //it is not possible to have oldFeed partitions as non empty and
        //new being empty. validator should have gated this.
        //Also if new partitions are added and old is empty, then there is nothing
        //to update in process
        boolean partitionApplicable = false;
        for (Input input : affectedProcess.getInputs().getInput()) {
            if (input.getFeed().equals(oldFeed.getName())) {
                if (input.getPartition() != null && !input.getPartition().isEmpty()) {
                    partitionApplicable = true;
                }
            }
        }
        if (partitionApplicable) {
            LOG.debug("Partitions are applicable. Checking ...");
            if (newFeed.getPartitions() != null && oldFeed.getPartitions() != null) {
                List<String> newParts = getParts(newFeed.getPartitions());
                List<String> oldParts = getParts(oldFeed.getPartitions());
                if (newParts.size() != oldParts.size()) return true;
                if (!newParts.containsAll(oldParts)) return true;
            }
            LOG.debug(oldFeed.toShortString() + ": Partitions identical. Ignoring...");
        }

        Map<String, String> oldProps = getProperties(oldFeed);
        Map<String, String> newProps = getProperties(newFeed);
        if (oldProps.size() != newProps.size()) return true;
        for (Map.Entry<String, String> entry : oldProps.entrySet()) {
            if (!newProps.containsKey(entry.getKey()) ||
                    !newProps.get(entry.getKey()).equals(entry.getValue())) return true;
        }
        LOG.debug(oldFeed.toShortString() + ": Properties identical. Ignoring...");

        String clusterName = affectedProcess.getCluster().getName();
        if (!oldFeed.getCluster(clusterName).getValidity().getStart().
                equals(newFeed.getCluster(clusterName).getValidity().getStart())) return true;
        LOG.debug(oldFeed.toShortString() + ": Feed start on cluster" + clusterName +
                " identical. Ignoring...");

        return false;
    }

    private static Map<String, String> getProperties(Feed feed) {
        Map<String, String> props = new HashMap<String, String>();
        if (feed.getProperties() == null) return props;
        for (Map.Entry<String, Property> prop : feed.getProperties().entrySet()) {
            props.put(prop.getKey(), prop.getValue().getValue());
        }
        return props;
    }

    private static List<String> getParts(Partitions partitions) {
        List<String> parts = new ArrayList<String>();
        for (Partition partition : partitions.getPartition()) {
            parts.add(partition.getName());
        }
        return parts;
    }
}
