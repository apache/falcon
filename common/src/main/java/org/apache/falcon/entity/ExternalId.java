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
import org.apache.falcon.Tag;
import org.apache.falcon.entity.v0.SchemaHelper;

import java.util.Date;

/**
 * External id as represented by workflow engine.
 */
public class ExternalId {
    private static final String SEPARATOR = "/";
    private String id;

    public ExternalId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public ExternalId(String name, Tag tag, String elexpr) {
        if (StringUtils.isEmpty(name) || tag == null || StringUtils.isEmpty(elexpr)) {
            throw new IllegalArgumentException("Empty inputs!");
        }

        id = name + SEPARATOR + tag.name() + SEPARATOR + elexpr;
    }

    public ExternalId(String name, Tag tag, Date date) {
        this(name, tag, SchemaHelper.formatDateUTC(date));
    }

    public String getName() {
        String[] parts = id.split(SEPARATOR);
        return parts[0];
    }

    public Date getDate() throws FalconException {
        return EntityUtil.parseDateUTC(getDateAsString());
    }

    public String getDateAsString() {
        String[] parts = id.split(SEPARATOR);
        return parts[2];
    }

    public Tag getTag() {
        String[] parts = id.split(SEPARATOR);
        return Tag.valueOf(parts[1]);
    }

    public String getDFSname() {
        return id.replace(":", "-");
    }
}
