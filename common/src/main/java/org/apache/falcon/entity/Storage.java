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
import org.apache.falcon.entity.v0.AccessControlList;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.Path;

import java.util.Date;
import java.util.List;

/**
 * A class to encapsulate the storage for a given feed which can either be
 * expressed as a path on the file system or a table in a catalog.
 */
public interface Storage extends Configurable {

    String DOLLAR_EXPR_START_REGEX = "\\$\\{";
    String QUESTION_EXPR_START_REGEX = "\\?\\{";
    String EXPR_CLOSE_REGEX = "\\}";

    /**
     * URI Friendly expression.
     */
    String DOLLAR_EXPR_START_NORMALIZED = "_D__START_";
    String EXPR_CLOSE_NORMALIZED = "_CLOSE_";

    /**
     * Enumeration for the various storage types.
     */
    enum TYPE {FILESYSTEM, TABLE}

    /**
     * Return the type of storage.
     *
     * @return storage type
     */
    TYPE getType();

    /**
     * Return the uri template.
     *
     * @return uri template
     */
    String getUriTemplate();

    /**
     * Return the uri template for a given location type.
     *
     * @param locationType type of location, applies only to filesystem type
     * @return uri template
     */
    String getUriTemplate(LocationType locationType);

    /**
     * Check for equality of this instance against the one in question.
     *
     * @param toCompareAgainst instance to compare
     * @return true if identical else false
     * @throws FalconException an exception
     */
    boolean isIdentical(Storage toCompareAgainst) throws FalconException;

    /**
     * Check the permission on the storage, regarding owner/group/permission coming from ACL.
     *
     * @param acl the ACL defined in the entity.
     * @throws FalconException if the permissions are not valid.
     */
    void validateACL(AccessControlList acl) throws FalconException;

    /**
     * Get Feed Listing for a feed between a date range.
     */
    List<FeedInstanceStatus> getListing(Feed feed, String cluster, LocationType locationType,
                                        Date start, Date end) throws FalconException;

    /**
     * Delete the instances of the feeds which are older than the retentionLimit specified.
     *
     * @param retentionLimit - retention limit of the feed e.g. hours(5).
     * @param timeZone - timeZone for the feed definition.
     * @param logFilePath - logFile to be used to record the deleted instances.
     * @return - StringBuffer containing comma separated list of dates for the deleted instances.
     * @throws FalconException
     */
    StringBuilder evict(String retentionLimit, String timeZone, Path logFilePath) throws FalconException;
}
