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

package org.apache.falcon.service;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import org.apache.falcon.FalconException;
import org.apache.falcon.Pair;
import org.apache.falcon.entity.AbstractTestBase;
import org.apache.falcon.entity.EntityNotRegisteredException;
import org.apache.falcon.entity.parser.ValidationException;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.entity.v0.feed.Clusters;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Sla;
import org.apache.falcon.entity.v0.feed.Validity;
import org.apache.falcon.resource.AbstractSchedulableEntityManager;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests for FeedSLAMonitoring Service.
 */
public class FeedSLAMonitoringTest extends AbstractTestBase {
    private static final String CLUSTER_NAME = "testCluster";
    private static final String FEED_NAME = "testFeed";
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    private static final String TAG_CRITICAL = FeedSLAMonitoringService.get().TAG_CRITICAL;

    @Test
    public void testSLAStatus() throws FalconException {
        // sla, start, end, missingInstances
        Sla sla = new Sla();
        sla.setSlaLow(new Frequency("days(1)"));
        sla.setSlaHigh(new Frequency("days(2)"));

        Date start = SchemaHelper.parseDateUTC("2014-05-05T00:00Z");
        Date end = SchemaHelper.parseDateUTC("2015-05-05T00:00Z");

        List<Date> missingInstances = new ArrayList<>();
        missingInstances.add(SchemaHelper.parseDateUTC("2013-05-05T00:00Z")); // before start time
        missingInstances.add(SchemaHelper.parseDateUTC("2014-05-05T00:00Z")); // equal to start time
        missingInstances.add(SchemaHelper.parseDateUTC("2014-05-06T00:00Z")); // in between
        missingInstances.add(SchemaHelper.parseDateUTC("2014-05-07T00:00Z"));
        missingInstances.add(SchemaHelper.parseDateUTC("2015-05-05T00:00Z")); // equal to end time
        missingInstances.add(SchemaHelper.parseDateUTC("2015-05-06T00:00Z")); // after end time

        Set<Pair<Date, String>> result = FeedSLAMonitoringService.get().getSLAStatus(sla, start, end, missingInstances);
        Set<Pair<Date, String>> expected = new HashSet<>();
        expected.add(new Pair<>(SchemaHelper.parseDateUTC("2014-05-05T00:00Z"), TAG_CRITICAL));
        expected.add(new Pair<>(SchemaHelper.parseDateUTC("2014-05-06T00:00Z"), TAG_CRITICAL));
        expected.add(new Pair<>(SchemaHelper.parseDateUTC("2014-05-07T00:00Z"), TAG_CRITICAL));
        expected.add(new Pair<>(SchemaHelper.parseDateUTC("2015-05-05T00:00Z"), TAG_CRITICAL));
        Assert.assertEquals(result, expected);
    }

    @Test(expectedExceptions = ValidationException.class,
            expectedExceptionsMessageRegExp = "SLA monitoring is not supported for: PROCESS")
    public void testInvalidType() throws FalconException {
        AbstractSchedulableEntityManager.validateSlaParams("process",
                "in", "2015-05-05T00:00Z", "2015-05-05T00:00Z", "*");
    }

    @Test(expectedExceptions = EntityNotRegisteredException.class,
            expectedExceptionsMessageRegExp = ".*\\(FEED\\) not found.*")
    public void testInvalidName() throws FalconException {
        AbstractSchedulableEntityManager.validateSlaParams("feed",
                "non-existent", "2015-05-05T00:00Z", "2015-05-05T00:00Z", "*");
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "2015-05-00T00:00Z is not a valid UTC string")
    public void testInvalidStart() throws FalconException {
        AbstractSchedulableEntityManager.validateSlaParams("feed", null, "2015-05-00T00:00Z", "2015-05-05T00:00Z", "*");
    }

    @Test(expectedExceptions = ValidationException.class,
            expectedExceptionsMessageRegExp = "start can not be after end")
    public void testInvalidRange() throws FalconException {
        AbstractSchedulableEntityManager.validateSlaParams("feed",
                null, "2015-05-05T00:00Z", "2014-05-05T00:00Z", "*");
    }

    @Test
    public void testOptionalName() throws FalconException {
        AbstractSchedulableEntityManager.validateSlaParams("feed", null, "2015-05-05T00:00Z", "2015-05-05T00:00Z", "*");
        AbstractSchedulableEntityManager.validateSlaParams("feed", "", "2015-05-05T00:00Z", "2015-05-05T00:00Z", "*");
    }

    @Test
    public void testOptionalEnd() throws FalconException {
        AbstractSchedulableEntityManager.validateSlaParams("feed", null, "2015-05-05T00:00Z", "", "*");
        AbstractSchedulableEntityManager.validateSlaParams("feed", null, "2015-05-05T00:00Z", null, "*");
    }

    private Cluster publishCluster() throws FalconException {
        Cluster cluster = new Cluster();
        cluster.setName(CLUSTER_NAME);
        cluster.setColo("default");
        getStore().publish(EntityType.CLUSTER, cluster);
        return cluster;

    }

    private Feed publishFeed(Cluster cluster, String frequency, String start, String end)
        throws FalconException, ParseException {
        Feed feed = new Feed();
        feed.setName(FEED_NAME);
        Frequency f = new Frequency(frequency);
        feed.setFrequency(f);
        feed.setTimezone(UTC);
        Clusters fClusters = new Clusters();
        org.apache.falcon.entity.v0.feed.Cluster fCluster = new org.apache.falcon.entity.v0.feed.Cluster();
        fCluster.setType(ClusterType.SOURCE);
        fCluster.setName(cluster.getName());
        fCluster.setValidity(getFeedValidity(start, end));
        fClusters.getClusters().add(fCluster);
        feed.setClusters(fClusters);
        getStore().publish(EntityType.FEED, feed);
        return feed;
    }

    private Validity getFeedValidity(String start, String end) throws ParseException {
        Validity validity = new Validity();
        validity.setStart(getDate(start));
        validity.setEnd(getDate(end));
        return validity;
    }

    private Date getDate(String dateString) throws ParseException {
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm Z");
        return format.parse(dateString);
    }
}
