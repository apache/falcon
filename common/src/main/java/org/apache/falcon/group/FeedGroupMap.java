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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.service.ConfigurationChangeListener;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Has 2 way mappings from feed to group and group to feed.
 */
public final class FeedGroupMap implements ConfigurationChangeListener {

    private static final FeedGroupMap INSTANCE = new FeedGroupMap();
    private Map<String, FeedGroup> groupsMapping = new ConcurrentHashMap<String, FeedGroup>();

    private FeedGroupMap() {
        // singleton
    }

    public static FeedGroupMap get() {
        return INSTANCE;
    }

    public Map<String, FeedGroup> getGroupsMapping() {
        return Collections.unmodifiableMap(groupsMapping);
    }

    @Override
    public void onAdd(Entity entity) throws FalconException {

        if (entity.getEntityType().equals(EntityType.FEED)) {
            Feed feed = (Feed) entity;
            if (feed.getGroups() == null || feed.getGroups().equals("")) {
                return;
            }
            Set<FeedGroup> groupSet = getGroups(feed);
            addGroups(feed.getName(), groupSet);
        }
    }

    @Override
    public void onRemove(Entity entity) throws FalconException {
        if (entity.getEntityType().equals(EntityType.FEED)) {
            Feed feed = (Feed) entity;
            if (StringUtils.isEmpty(feed.getGroups())) {
                return;
            }
            String[] groups = feed.getGroups().split(",");
            for (String group : groups) {
                groupsMapping.get(group).getFeeds().remove(entity.getName());
                if (groupsMapping.get(group).getFeeds().size() == 0) {
                    groupsMapping.remove(group);
                }
            }

        }
    }

    @Override
    public void onChange(Entity oldEntity, Entity newEntity)
        throws FalconException {

        onRemove(oldEntity);
        onAdd(newEntity);
    }

    @Override
    public void onReload(Entity entity) throws FalconException {
        onAdd(entity);
    }

    private void addGroups(String feed, Set<FeedGroup> groups) {
        for (FeedGroup group : groups) {
            if (groupsMapping.containsKey(group.getName())) {
                groupsMapping.get(group.getName()).getFeeds().add(feed);
            } else {
                group.getFeeds().add(feed);
                groupsMapping.put(group.getName(), group);
            }
        }
    }

    public Set<FeedGroup> getGroups(String groups, Frequency frequency, String path) {
        Set<FeedGroup> groupSet = new HashSet<FeedGroup>();
        String[] groupArray = groups.split(",");
        for (String group : groupArray) {
            groupSet.add(new FeedGroup(group, frequency, path));
        }
        return groupSet;
    }

    public Set<FeedGroup> getGroups(org.apache.falcon.entity.v0.feed.Feed feed) throws FalconException {
        return getGroups(feed.getGroups(), feed.getFrequency(),
                FeedHelper.createStorage(feed).getUriTemplate(LocationType.DATA));
    }
}
