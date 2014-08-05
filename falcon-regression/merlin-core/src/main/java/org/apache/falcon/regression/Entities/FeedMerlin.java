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

package org.apache.falcon.regression.Entities;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.ActionType;
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.entity.v0.feed.Clusters;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.entity.v0.feed.Property;
import org.apache.falcon.entity.v0.feed.Retention;
import org.apache.falcon.entity.v0.feed.RetentionType;
import org.apache.falcon.entity.v0.feed.Validity;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.log4j.Logger;
import org.testng.Assert;

import javax.xml.bind.JAXBException;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Class for representing a feed xml. */
public class FeedMerlin extends Feed {

    private static final Logger LOGGER = Logger.getLogger(FeedMerlin.class);

    public FeedMerlin(String feedData) {
        this((Feed) fromString(EntityType.FEED, feedData));
    }

    public FeedMerlin(final Feed feed) {
        try {
            PropertyUtils.copyProperties(this, feed);
        } catch (IllegalAccessException e) {
            Assert.fail("Can't create ClusterMerlin: " + ExceptionUtils.getStackTrace(e));
        } catch (InvocationTargetException e) {
            Assert.fail("Can't create ClusterMerlin: " + ExceptionUtils.getStackTrace(e));
        } catch (NoSuchMethodException e) {
            Assert.fail("Can't create ClusterMerlin: " + ExceptionUtils.getStackTrace(e));
        }
    }

    public static List<FeedMerlin> fromString(List<String> feedStrings) {
        List<FeedMerlin> feeds = new ArrayList<FeedMerlin>();
        for (String feedString : feedStrings) {
            feeds.add(new FeedMerlin(feedString));
        }
        return feeds;
    }

    /**
     * Method sets a number of clusters to feed definition.
     *
     * @param newClusters list of definitions of clusters which are to be set to feed
     * @param location location of data on every cluster
     * @param startTime start of feed validity on every cluster
     * @param endTime end of feed validity on every cluster
     */
    public void setFeedClusters(List<String> newClusters, String location, String startTime,
                                String endTime) {
        Clusters cs = new Clusters();
        setFrequency(new Frequency("" + 5, Frequency.TimeUnit.minutes));

        for (String newCluster : newClusters) {
            Cluster c = new Cluster();
            c.setName(new ClusterMerlin(newCluster).getName());
            Location l = new Location();
            l.setType(LocationType.DATA);
            l.setPath(location + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}");
            Locations ls = new Locations();
            ls.getLocations().add(l);
            c.setLocations(ls);
            Validity v = new Validity();
            startTime = TimeUtil.addMinsToTime(startTime, -180);
            endTime = TimeUtil.addMinsToTime(endTime, 180);
            v.setStart(TimeUtil.oozieDateToDate(startTime).toDate());
            v.setEnd(TimeUtil.oozieDateToDate(endTime).toDate());
            c.setValidity(v);
            Retention r = new Retention();
            r.setAction(ActionType.DELETE);
            Frequency f1 = new Frequency("" + 20, Frequency.TimeUnit.hours);
            r.setLimit(f1);
            r.setType(RetentionType.INSTANCE);
            c.setRetention(r);
            cs.getClusters().add(c);
        }
        setClusters(cs);
    }

    public void setRetentionValue(String retentionValue) {
        for (org.apache.falcon.entity.v0.feed.Cluster cluster : getClusters().getClusters()) {
            cluster.getRetention().setLimit(new Frequency(retentionValue));
        }
    }

    public void setTableValue(String dBName, String tableName, String pathValue) {
        getTable().setUri("catalog:" + dBName + ":" + tableName + "#" + pathValue);
    }

    @Override
    public String toString() {
        try {
            StringWriter sw = new StringWriter();
            EntityType.FEED.getMarshaller().marshal(this, sw);
            return sw.toString();
        } catch (JAXBException e) {
            throw new RuntimeException(e);
        }
    }

    public void setLocation(LocationType locationType, String feedInputPath) {
        for (Location location : getLocations().getLocations()) {
            if (location.getType() == locationType) {
                location.setPath(feedInputPath);
            }
        }
    }

    public void addProperty(String someProp, String someVal) {
        Property property = new Property();
        property.setName(someProp);
        property.setValue(someVal);
        this.getProperties().getProperties().add(property);
    }

    /**
     * Sets unique names for the feed.
     * @return mapping of old name to new name
     */
    public Map<? extends String, ? extends String> setUniqueName() {
        final String oldName = getName();
        final String newName =  oldName + Util.getUniqueString();
        setName(newName);
        final HashMap<String, String> nameMap = new HashMap<String, String>(1);
        nameMap.put(oldName, newName);
        return nameMap;
    }

    public void renameClusters(Map<String, String> clusterNameMap) {
        for (Cluster cluster : getClusters().getClusters()) {
            final String oldName = cluster.getName();
            final String newName = clusterNameMap.get(oldName);
            if (!StringUtils.isEmpty(newName)) {
                cluster.setName(newName);
            }
        }
    }
}
