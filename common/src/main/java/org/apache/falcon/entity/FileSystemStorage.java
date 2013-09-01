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

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;

import java.util.List;

/**
 * A file system implementation of a feed storage.
 */
public class FileSystemStorage implements Storage {

    private final String storageUrl;
    private final List<Location> locations;

    protected FileSystemStorage(List<Location> locations) {
        this("${nameNode}", locations);
    }

    protected FileSystemStorage(String storageUrl, List<Location> locations) {
        if (storageUrl == null || storageUrl.length() == 0) {
            throw new IllegalArgumentException("FileSystem URL cannot be null or empty");
        }

        if (locations == null || locations.size() == 0) {
            throw new IllegalArgumentException("FileSystem Locations cannot be null or empty");
        }

        this.storageUrl = storageUrl;
        this.locations = locations;
    }

    @Override
    public TYPE getType() {
        return TYPE.FILESYSTEM;
    }

    public String getStorageUrl() {
        return storageUrl;
    }

    public List<Location> getLocations() {
        return locations;
    }

    @Override
    public String getUriTemplate() {
        return getUriTemplate(LocationType.DATA);
    }

    @Override
    public String getUriTemplate(LocationType locationType) {
        Location locationForType = null;
        for (Location location : locations) {
            if (location.getType() == locationType) {
                locationForType = location;
                break;
            }
        }

        if (locationForType == null) {
            return "/tmp";
        }

        StringBuilder uriTemplate = new StringBuilder();
        uriTemplate.append(storageUrl);
        uriTemplate.append(locationForType.getPath());
        return uriTemplate.toString();
    }

    @Override
    public boolean exists() throws FalconException {
        // Directories on FS will be created if they don't exist.
        return true;
    }

    @Override
    public boolean isIdentical(Storage toCompareAgainst) throws FalconException {
        FileSystemStorage fsStorage = (FileSystemStorage) toCompareAgainst;
        final List<Location> fsStorageLocations = fsStorage.getLocations();

        return getLocations().size() == fsStorageLocations.size()
               && getLocation(getLocations(), LocationType.DATA).getPath().equals(
                   getLocation(fsStorageLocations, LocationType.DATA).getPath())
               && getLocation(getLocations(), LocationType.META).getPath().equals(
                   getLocation(fsStorageLocations, LocationType.META).getPath())
               && getLocation(getLocations(), LocationType.STATS).getPath().equals(
                   getLocation(fsStorageLocations, LocationType.STATS).getPath())
               && getLocation(getLocations(), LocationType.TMP).getPath().equals(
                   getLocation(fsStorageLocations, LocationType.TMP).getPath());
    }

    private static Location getLocation(List<Location> locations, LocationType type) {
        for (Location loc : locations) {
            if (loc.getType() == type) {
                return loc;
            }
        }

        Location loc = new Location();
        loc.setPath("/tmp");
        loc.setType(type);
        return loc;
    }
}
