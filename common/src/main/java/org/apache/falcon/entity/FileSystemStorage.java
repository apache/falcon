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
import org.apache.falcon.entity.common.FeedDataPath;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.security.CurrentUser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

/**
 * A file system implementation of a feed storage.
 */
public class FileSystemStorage implements Storage {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemStorage.class);

    public static final String FEED_PATH_SEP = "#";
    public static final String LOCATION_TYPE_SEP = "=";

    public static final String FILE_SYSTEM_URL = "${nameNode}";

    private final String storageUrl;
    private final List<Location> locations;

    protected FileSystemStorage(Feed feed) {
        this(FILE_SYSTEM_URL, feed.getLocations());
    }

    protected FileSystemStorage(String storageUrl, Locations locations) {
        this(storageUrl, locations.getLocations());
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

    /**
     * Create an instance from the URI Template that was generated using
     * the getUriTemplate() method.
     *
     * @param uriTemplate the uri template from org.apache.falcon.entity.FileSystemStorage#getUriTemplate
     * @throws URISyntaxException
     */
    protected FileSystemStorage(String uriTemplate) throws URISyntaxException {
        if (uriTemplate == null || uriTemplate.length() == 0) {
            throw new IllegalArgumentException("URI template cannot be null or empty");
        }

        String rawStorageUrl = null;
        List<Location> rawLocations = new ArrayList<Location>();
        String[] feedLocs = uriTemplate.split(FEED_PATH_SEP);
        for (String rawPath : feedLocs) {
            String[] typeAndPath = rawPath.split(LOCATION_TYPE_SEP);
            final String processed = typeAndPath[1].replaceAll(DOLLAR_EXPR_START_REGEX, DOLLAR_EXPR_START_NORMALIZED)
                                                   .replaceAll("}", EXPR_CLOSE_NORMALIZED);
            URI uri = new URI(processed);
            if (rawStorageUrl == null) {
                rawStorageUrl = uri.getScheme() + "://" + uri.getAuthority();
            }

            String path = uri.getPath();
            final String finalPath = path.replaceAll(DOLLAR_EXPR_START_NORMALIZED, DOLLAR_EXPR_START_REGEX)
                                         .replaceAll(EXPR_CLOSE_NORMALIZED, EXPR_CLOSE_REGEX);

            Location location = new Location();
            location.setPath(finalPath);
            location.setType(LocationType.valueOf(typeAndPath[0]));
            rawLocations.add(location);
        }

        this.storageUrl = rawStorageUrl;
        this.locations = rawLocations;
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
        String feedPathMask = getUriTemplate(LocationType.DATA);
        String metaPathMask = getUriTemplate(LocationType.META);
        String statsPathMask = getUriTemplate(LocationType.STATS);
        String tmpPathMask = getUriTemplate(LocationType.TMP);

        StringBuilder feedBasePaths = new StringBuilder();
        feedBasePaths.append(LocationType.DATA.name())
                     .append(LOCATION_TYPE_SEP)
                     .append(feedPathMask);

        if (metaPathMask != null) {
            feedBasePaths.append(FEED_PATH_SEP)
                         .append(LocationType.META.name())
                         .append(LOCATION_TYPE_SEP)
                         .append(metaPathMask);
        }

        if (statsPathMask != null) {
            feedBasePaths.append(FEED_PATH_SEP)
                         .append(LocationType.STATS.name())
                         .append(LOCATION_TYPE_SEP)
                         .append(statsPathMask);
        }

        if (tmpPathMask != null) {
            feedBasePaths.append(FEED_PATH_SEP)
                         .append(LocationType.TMP.name())
                         .append(LOCATION_TYPE_SEP)
                         .append(tmpPathMask);
        }

        return feedBasePaths.toString();
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

        // normalize the path so trailing and double '/' are removed
        Path locationPath = new Path(locationForType.getPath());
        locationPath = locationPath.makeQualified(getDefaultUri(), getWorkingDir());

        if (isRelativePath(locationPath)) {
            locationPath = new Path(storageUrl + locationPath);
        }

        return locationPath.toString();
    }

    private boolean isRelativePath(Path locationPath) {
        return locationPath.toUri().getAuthority() == null && isStorageUrlATemplate();
    }

    private boolean isStorageUrlATemplate() {
        return storageUrl.startsWith(FILE_SYSTEM_URL);
    }

    private URI getDefaultUri() {
        return new Path(isStorageUrlATemplate() ? "/" : storageUrl).toUri();
    }

    public Path getWorkingDir() {
        return new Path(CurrentUser.getSubject() == null ? "/" : "/user/" + CurrentUser.getUser());
    }

    @Override
    public boolean isIdentical(Storage toCompareAgainst) throws FalconException {
        if (!(toCompareAgainst instanceof FileSystemStorage)) {
            return false;
        }

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

    @Override
    public void validateACL(String owner, String group, String permissions) throws FalconException {
        try {
            FileSystem fileSystem = HadoopClientFactory.get().createProxiedFileSystem(getConf());
            for (Location location : getLocations()) {
                String pathString = getRelativePath(location);
                Path path = new Path(pathString);
                if (fileSystem.exists(path)) {
                    FileStatus fileStatus = fileSystem.getFileStatus(path);
                    if (!fileStatus.getOwner().equals(owner)) {
                        LOG.error("Feed ACL owner {} doesn't match the actual file owner {}",
                                owner, fileStatus.getOwner());
                        throw new FalconException("Feed ACL owner " + owner + " doesn't match the actual file owner "
                                + fileStatus.getOwner());
                    }
                }
            }
        } catch (IOException e) {
            LOG.error("Can't validate ACL on storage {}", getStorageUrl(), e);
            throw new RuntimeException("Can't validate storage ACL (URI " + getStorageUrl() + ")", e);
        }
    }

    private Configuration getConf() {
        Configuration conf = new Configuration();
        conf.set(HadoopClientFactory.FS_DEFAULT_NAME_KEY, storageUrl);
        return conf;
    }

    private String getRelativePath(Location location) {
        // if the path contains variables, locate on the "parent" path (just before first variable usage)
        Matcher matcher = FeedDataPath.PATTERN.matcher(location.getPath());
        boolean timedPath = matcher.find();
        if (timedPath) {
            return location.getPath().substring(0, matcher.start());
        }
        return location.getPath();
    }

    @Override
    public String toString() {
        return "FileSystemStorage{"
                + "storageUrl='" + storageUrl + '\''
                + ", locations=" + locations
                + '}';
    }
}
