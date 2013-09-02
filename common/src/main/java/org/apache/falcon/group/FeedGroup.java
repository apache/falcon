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
package org.apache.falcon.group;

import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.common.FeedDataPath;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.LocationType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;

/**
 * Group, which represents a logical group of feeds which can belong to this
 * group.
 */
public class FeedGroup {

    public FeedGroup(String group, Frequency frequency, String path) {
        this.name = group;
        this.frequency = frequency;
        this.datePattern = getDatePattern(path);
        this.feeds = Collections
                .newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    }

    public static String getDatePattern(String path) {
        Matcher matcher = FeedDataPath.PATTERN.matcher(path);
        List<String> fields = new ArrayList<String>();
        while (matcher.find()) {
            String var = path.substring(matcher.start(), matcher.end());
            fields.add(var);
        }
        Collections.sort(fields);
        return fields.toString();
    }

    private String name;
    private Frequency frequency;
    private String datePattern;
    private Set<String> feeds;

    public Set<String> getFeeds() {
        return feeds;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof FeedGroup)) {
            return false;
        }
        FeedGroup group = (FeedGroup) obj;
        return (this.name.equals(group.getName())
                && this.frequency.equals(group.frequency)
                && this.datePattern
                .equals(group.datePattern));

    }

    @Override
    public int hashCode() {
        return 127 * name.hashCode() + 31 * frequency.hashCode() + datePattern.hashCode();
    }

    public String getName() {
        return name;
    }

    public Frequency getFrequency() {
        return frequency;
    }

    public String getDatePattern() {
        return datePattern;
    }

    public boolean canContainFeed(org.apache.falcon.entity.v0.feed.Feed feed) {
        return this.frequency.equals(feed.getFrequency())
                && this.datePattern.equals(getDatePattern(FeedHelper.getLocation(feed, LocationType.DATA).getPath()));
    }
}
