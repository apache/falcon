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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.Pair;
import org.apache.falcon.entity.common.FeedDataPath;
import org.apache.falcon.entity.v0.AccessControlList;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.retention.EvictedInstanceSerDe;
import org.apache.falcon.retention.EvictionHelper;
import org.apache.falcon.security.CurrentUser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A file system implementation of a feed storage.
 */
public class FileSystemStorage extends Configured implements Storage {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemStorage.class);
    private final StringBuffer instancePaths = new StringBuffer();
    private final StringBuilder instanceDates = new StringBuilder();

    public static final String FEED_PATH_SEP = "#";
    public static final String LOCATION_TYPE_SEP = "=";

    public static final String FILE_SYSTEM_URL = "${nameNode}";

    private final String storageUrl;
    private final List<Location> locations;

    public FileSystemStorage(Feed feed) {
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
        return getUriTemplate(locationType, locations);
    }

    public String getUriTemplate(LocationType locationType, List<Location> locationList) {
        Location locationForType = null;
        for (Location location : locationList) {
            if (location.getType() == locationType) {
                locationForType = location;
                break;
            }
        }

        if (locationForType == null || StringUtils.isEmpty(locationForType.getPath())) {
            return null;
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
        return new Path(CurrentUser.isAuthenticated() ? "/user/" + CurrentUser.getUser() : "/");
    }

    @Override
    public boolean isIdentical(Storage toCompareAgainst) throws FalconException {
        if (!(toCompareAgainst instanceof FileSystemStorage)) {
            return false;
        }

        FileSystemStorage fsStorage = (FileSystemStorage) toCompareAgainst;
        final List<Location> fsStorageLocations = fsStorage.getLocations();

        return getLocations().size() == fsStorageLocations.size()
                && StringUtils.equals(getUriTemplate(LocationType.DATA, getLocations()),
                    getUriTemplate(LocationType.DATA, fsStorageLocations))
                && StringUtils.equals(getUriTemplate(LocationType.STATS, getLocations()),
                    getUriTemplate(LocationType.STATS, fsStorageLocations))
                && StringUtils.equals(getUriTemplate(LocationType.META, getLocations()),
                    getUriTemplate(LocationType.META, fsStorageLocations))
                && StringUtils.equals(getUriTemplate(LocationType.TMP, getLocations()),
                    getUriTemplate(LocationType.TMP, fsStorageLocations));
    }

    public static Location getLocation(List<Location> locations, LocationType type) {
        for (Location loc : locations) {
            if (loc.getType() == type) {
                return loc;
            }
        }

        return null;
    }

    @Override
    public void validateACL(AccessControlList acl) throws FalconException {
        try {
            for (Location location : getLocations()) {
                String pathString = getRelativePath(location);
                Path path = new Path(pathString);
                FileSystem fileSystem =
                    HadoopClientFactory.get().createProxiedFileSystem(path.toUri(), getConf());
                if (fileSystem.exists(path)) {
                    FileStatus fileStatus = fileSystem.getFileStatus(path);
                    Set<String> groups = CurrentUser.getGroupNames();

                    if (fileStatus.getOwner().equals(acl.getOwner())
                            || groups.contains(acl.getGroup())) {
                        return;
                    }

                    LOG.error("Permission denied: Either Feed ACL owner {} or group {} doesn't "
                                    + "match the actual file owner {} or group {} for file {}",
                            acl, acl.getGroup(), fileStatus.getOwner(), fileStatus.getGroup(), path);
                    throw new FalconException("Permission denied: Either Feed ACL owner "
                            + acl + " or group " + acl.getGroup() + " doesn't match the actual "
                            + "file owner " + fileStatus.getOwner() + " or group "
                            + fileStatus.getGroup() + "  for file " + path);
                }
            }
        } catch (IOException e) {
            LOG.error("Can't validate ACL on storage {}", getStorageUrl(), e);
            throw new RuntimeException("Can't validate storage ACL (URI " + getStorageUrl() + ")", e);
        }
    }

    @Override
    public StringBuilder evict(String retentionLimit, String timeZone, Path logFilePath) throws FalconException {
        TimeZone tz = TimeZone.getTimeZone(timeZone);
        try{
            for (Location location : getLocations()) {
                fileSystemEvictor(getUriTemplate(location.getType()), retentionLimit, tz, logFilePath);
            }
            EvictedInstanceSerDe.serializeEvictedInstancePaths(
                    HadoopClientFactory.get().createProxiedFileSystem(logFilePath.toUri(), getConf()),
                    logFilePath, instancePaths);
        }catch (IOException e){
            throw new FalconException("Couldn't evict feed from fileSystem", e);
        }catch (ELException e){
            throw new FalconException("Couldn't evict feed from fileSystem", e);
        }

        return instanceDates;
    }

    private void fileSystemEvictor(String feedPath, String retentionLimit, TimeZone timeZone,
                                   Path logFilePath) throws IOException, ELException, FalconException {
        Path normalizedPath = new Path(feedPath);
        FileSystem fs = HadoopClientFactory.get().createProxiedFileSystem(normalizedPath.toUri());
        feedPath = normalizedPath.toUri().getPath();
        LOG.info("Normalized path: {}", feedPath);

        Pair<Date, Date> range = EvictionHelper.getDateRange(retentionLimit);

        List<Path> toBeDeleted = discoverInstanceToDelete(feedPath, timeZone, range.first, fs);
        if (toBeDeleted.isEmpty()) {
            LOG.info("No instances to delete.");
            return;
        }

        DateFormat dateFormat = new SimpleDateFormat(FeedHelper.FORMAT);
        dateFormat.setTimeZone(timeZone);
        Path feedBasePath = fs.makeQualified(FeedHelper.getFeedBasePath(feedPath));
        for (Path path : toBeDeleted) {
            deleteInstance(fs, path, feedBasePath);
            Date date = FeedHelper.getDate(feedPath, new Path(path.toUri().getPath()), timeZone);
            instanceDates.append(dateFormat.format(date)).append(',');
            instancePaths.append(path).append(EvictedInstanceSerDe.INSTANCEPATH_SEPARATOR);
        }
    }

    private List<Path> discoverInstanceToDelete(String inPath, TimeZone timeZone, Date start, FileSystem fs)
        throws IOException {
        FileStatus[] files = findFilesForFeed(fs, inPath);
        if (files == null || files.length == 0) {
            return Collections.emptyList();
        }

        List<Path> toBeDeleted = new ArrayList<Path>();
        for (FileStatus file : files) {
            Date date = FeedHelper.getDate(inPath, new Path(file.getPath().toUri().getPath()), timeZone);
            LOG.debug("Considering {}", file.getPath().toUri().getPath());
            LOG.debug("Date: {}", date);
            if (date != null && !isDateInRange(date, start)) {
                toBeDeleted.add(file.getPath());
            }
        }
        return toBeDeleted;
    }

    private FileStatus[] findFilesForFeed(FileSystem fs, String feedBasePath) throws IOException {
        Matcher matcher = FeedDataPath.PATTERN.matcher(feedBasePath);
        while (matcher.find()) {
            String var = feedBasePath.substring(matcher.start(), matcher.end());
            feedBasePath = feedBasePath.replaceAll(Pattern.quote(var), "*");
            matcher = FeedDataPath.PATTERN.matcher(feedBasePath);
        }
        LOG.info("Searching for {}", feedBasePath);
        return fs.globStatus(new Path(feedBasePath));
    }

    private boolean isDateInRange(Date date, Date start) {
        //ignore end ( && date.compareTo(end) <= 0 )
        return date.compareTo(start) >= 0;
    }

    private void deleteInstance(FileSystem fs, Path path, Path feedBasePath) throws IOException {
        if (fs.delete(path, true)) {
            LOG.info("Deleted instance: {}", path);
        }else{
            throw new IOException("Unable to delete instance: " + path);
        }
        deleteParentIfEmpty(fs, path.getParent(), feedBasePath);
    }

    private void deleteParentIfEmpty(FileSystem fs, Path parent, Path feedBasePath) throws IOException {
        if (feedBasePath.equals(parent)) {
            LOG.info("Not deleting feed base path: {}", parent);
        } else {
            FileStatus[] files = fs.listStatus(parent);
            if (files != null && files.length == 0) {
                LOG.info("Parent path: {} is empty, deleting path", parent);
                if (fs.delete(parent, true)) {
                    LOG.info("Deleted empty dir: {}", parent);
                } else {
                    throw new IOException("Unable to delete parent path:" + parent);
                }
                deleteParentIfEmpty(fs, parent.getParent(), feedBasePath);
            }
        }
    }

    @Override
    @SuppressWarnings("MagicConstant")
    public List<FeedInstanceStatus> getListing(Feed feed, String clusterName, LocationType locationType,
                                               Date start, Date end) throws FalconException {

        Calendar calendar = Calendar.getInstance();
        List<Location> clusterSpecificLocation = FeedHelper.
                getLocations(FeedHelper.getCluster(feed, clusterName), feed);
        Location location = getLocation(clusterSpecificLocation, locationType);
        try {
            FileSystem fileSystem = HadoopClientFactory.get().createProxiedFileSystem(getConf());
            Cluster cluster = ClusterHelper.getCluster(clusterName);
            Properties baseProperties = FeedHelper.getClusterProperties(cluster);
            baseProperties.putAll(FeedHelper.getFeedProperties(feed));
            List<FeedInstanceStatus> instances = new ArrayList<FeedInstanceStatus>();
            Date feedStart = FeedHelper.getCluster(feed, clusterName).getValidity().getStart();
            TimeZone tz = feed.getTimezone();
            Date alignedStart = EntityUtil.getNextStartTime(feedStart, feed.getFrequency(), tz, start);

            String basePath = location.getPath();
            while (!end.before(alignedStart)) {
                Properties allProperties = ExpressionHelper.getTimeVariables(alignedStart, tz);
                allProperties.putAll(baseProperties);
                String feedInstancePath = ExpressionHelper.substitute(basePath, allProperties);
                FileStatus fileStatus = getFileStatus(fileSystem, new Path(feedInstancePath));
                FeedInstanceStatus instance = new FeedInstanceStatus(feedInstancePath);

                Date date = FeedHelper.getDate(basePath, new Path(feedInstancePath), tz);
                instance.setInstance(SchemaHelper.formatDateUTC(date));
                if (fileStatus != null) {
                    instance.setCreationTime(fileStatus.getModificationTime());
                    ContentSummary contentSummary = fileSystem.getContentSummary(fileStatus.getPath());
                    if (contentSummary != null) {
                        long size = contentSummary.getSpaceConsumed();
                        instance.setSize(size);
                        if (!StringUtils.isEmpty(feed.getAvailabilityFlag())) {
                            FileStatus doneFile = getFileStatus(fileSystem,
                                    new Path(fileStatus.getPath(), feed.getAvailabilityFlag()));
                            if (doneFile != null) {
                                instance.setStatus(FeedInstanceStatus.AvailabilityStatus.AVAILABLE);
                            } else {
                                instance.setStatus(FeedInstanceStatus.AvailabilityStatus.PARTIAL);
                            }
                        } else {
                            instance.setStatus(size > 0 ? FeedInstanceStatus.AvailabilityStatus.AVAILABLE
                                    : FeedInstanceStatus.AvailabilityStatus.EMPTY);
                        }
                    }
                }
                instances.add(instance);
                calendar.setTime(alignedStart);
                calendar.add(feed.getFrequency().getTimeUnit().getCalendarUnit(),
                        feed.getFrequency().getFrequencyAsInt());
                alignedStart = calendar.getTime();
            }
            return instances;
        } catch (IOException e) {
            LOG.error("Unable to retrieve listing for {}:{}", locationType, getStorageUrl(), e);
            throw new FalconException("Unable to retrieve listing for (URI " + getStorageUrl() + ")", e);
        }
    }

    public FileStatus getFileStatus(FileSystem fileSystem, Path feedInstancePath) {
        FileStatus fileStatus = null;
        try {
            fileStatus = fileSystem.getFileStatus(feedInstancePath);
        } catch (IOException ignore) {
            //ignore
        }
        return fileStatus;
    }

    public Configuration getConf() {
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
