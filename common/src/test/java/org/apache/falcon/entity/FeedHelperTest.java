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

import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Properties;
import org.apache.falcon.entity.v0.cluster.Property;
import org.apache.falcon.entity.v0.feed.*;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.TimeZone;

/**
 * Test for feed helper methods.
 */
public class FeedHelperTest {
    public static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    @Test
    public void testPartitionExpression() {
        Assert.assertEquals(FeedHelper.normalizePartitionExpression(" /a// ", "  /b// "), "a/b");
        Assert.assertEquals(FeedHelper.normalizePartitionExpression(null, "  /b// "), "b");
        Assert.assertEquals(FeedHelper.normalizePartitionExpression(null, null), "");
    }

    @Test
    public void testEvaluateExpression() throws Exception {
        Cluster cluster = new Cluster();
        cluster.setName("name");
        cluster.setColo("colo");
        cluster.setProperties(new Properties());
        Property prop = new Property();
        prop.setName("pname");
        prop.setValue("pvalue");
        cluster.getProperties().getProperties().add(prop);

        Assert.assertEquals(FeedHelper.evaluateClusterExp(cluster, "${cluster.colo}/*/US"), "colo/*/US");
        Assert.assertEquals(FeedHelper.evaluateClusterExp(cluster, "${cluster.name}/*/${cluster.pname}"),
                "name/*/pvalue");
        Assert.assertEquals(FeedHelper.evaluateClusterExp(cluster, "IN"), "IN");
    }

    @DataProvider(name = "fsPathsforDate")
    public Object[][] createPathsForGetDate() {
        return new Object[][] {
            {"/data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}", "/data/2015/01/01/00/30", "2015-01-01T00:30Z"},
            {"/data/${YEAR}-${MONTH}-${DAY}-${HOUR}-${MINUTE}", "/data/2015-01-01-01-00", "2015-01-01T01:00Z"},
            {"/data/${YEAR}/${MONTH}/${DAY}", "/data/2015/01/01", "2015-01-01T00:00Z"},
            {"/data/${YEAR}/${MONTH}/${DAY}/data", "/data/2015/01/01/data", "2015-01-01T00:00Z"},
            {"/data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}", "/data/2015-01-01/00/30", null},
            {"/data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/data", "/data/2015-01-01/00/30", null},
            {"/data/${YEAR}/${MONTH}/${DAY}/${HOUR}/data", "/data/2015/05/25/00/data/{p1}/p2", "2015-05-25T00:00Z"},
            {"/data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}/data", "/data/2015/05/25/00/00/{p1}/p2", null},
        };
    }

    @Test(dataProvider = "fsPathsforDate")
    public void testGetDateFromPath(String template, String path, String expectedDate) throws Exception {
        Date date = FeedHelper.getDate(template, new Path(path), UTC);
        Assert.assertEquals(SchemaHelper.formatDateUTC(date), expectedDate);
    }

    @Test
    public void testGetLocations() {
        Cluster cluster = new Cluster();
        cluster.setName("name");
        Feed feed = new Feed();
        Location location1 = new Location();
        location1.setType(LocationType.META);
        Locations locations = new Locations();
        locations.getLocations().add(location1);

        Location location2 = new Location();
        location2.setType(LocationType.DATA);
        locations.getLocations().add(location2);

        org.apache.falcon.entity.v0.feed.Cluster feedCluster = new org.apache.falcon.entity.v0.feed.Cluster();
        feedCluster.setName("name");

        feed.setLocations(locations);
        Clusters clusters = new Clusters();
        feed.setClusters(clusters);
        feed.getClusters().getClusters().add(feedCluster);

        Assert.assertEquals(FeedHelper.getLocations(feedCluster, feed),
                locations.getLocations());
        Assert.assertEquals(FeedHelper.getLocation(feed, cluster, LocationType.DATA), location2);
    }
}
