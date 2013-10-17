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
import org.apache.falcon.entity.v0.feed.LocationType;

/**
 * A class to encapsulate the storage for a given feed which can either be
 * expressed as a path on the file system or a table in a catalog.
 */
public interface Storage {

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
     * Check if the storage, filesystem location or catalog table exists.
     * Filesystem location always returns true.
     *
     * @return true if table exists else false
     * @throws FalconException an exception
     */
    boolean exists() throws FalconException;

    /**
     * Check for equality of this instance against the one in question.
     *
     * @param toCompareAgainst instance to compare
     * @return true if identical else false
     * @throws FalconException an exception
     */
    boolean isIdentical(Storage toCompareAgainst) throws FalconException;
}
