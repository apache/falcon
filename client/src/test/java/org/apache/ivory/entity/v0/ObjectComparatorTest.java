package org.apache.ivory.entity.v0;

import org.apache.ivory.entity.v0.feed.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.TimeZone;

public class ObjectComparatorTest {

    @Test
    public void testEquals() throws Exception {

        Feed feed = new Feed();
        feed.setName("myfeed");
        feed.setAvailabilityFlag("yes");
        feed.setDescription("desc");
        feed.setGroups("grp");
        Partitions partitions = new Partitions();
        Partition partition = new Partition();
        partition.setName("p1");
        partitions.getPartitions().add(partition);
        feed.setPartitions(partitions);
        Locations locations = new Locations();
        Location location = new Location();
        locations.getLocations().add(location);
        location.setPath("path");
        location.setType(LocationType.DATA);
        feed.setLocations(locations);
        feed.setFrequency(Frequency.fromString("hours(1)"));

        Clusters clusters = new Clusters();
        Cluster cluster = new Cluster();
        clusters.getClusters().add(cluster);
        cluster.setName("c1");
        cluster.setPartition("*");
        cluster.setType(ClusterType.SOURCE);
        Validity validity = new Validity();
        validity.setStart("2012-01-01T01:00Z");
        validity.setEnd("2012-01-01T01:00Z");
        cluster.setValidity(validity);
        Retention retention = new Retention();
        retention.setAction(ActionType.DELETE);
        retention.setLimit(Frequency.fromString("days(1)"));
        retention.setType(RetentionType.INSTANCE);
        cluster.setRetention(retention);
        feed.setClusters(clusters);
        feed.setTimezone(TimeZone.getDefault());
        LateArrival late = new LateArrival();
        late.setCutOff(Frequency.fromString("days(1)"));
        feed.setLateArrival(late);

        ACL acl = new ACL();
        acl.setPermission("rwx");
        acl.setOwner("user");
        acl.setGroup("group");
        feed.setACL(acl);
        Schema schema = new Schema();
        schema.setLocation("lcoation");
        schema.setProvider("provider");
        feed.setSchema(schema);

        Feed feed1 = (Feed) feed.clone();

        Assert.assertTrue(ObjectComparator.equals(feed, feed1));
        feed1.setGroups("g1");
        Assert.assertFalse(ObjectComparator.equals(feed, feed1));
    }
}
