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

package org.apache.falcon.adfservice;

import org.apache.falcon.adfservice.util.FSUtils;
import org.apache.falcon.FalconException;

import java.net.URISyntaxException;

/**
 * Table Feed.
 */
public class TableFeed extends Feed {
    private static final String TABLE_FEED_TEMPLATE_FILE = "table-feed.xml";
    private static final String TABLE_PARTITION_SEPARATOR = "#";

    private String tableName;
    private String partitions;

    public TableFeed(final Builder builder) {
        this.feedName = builder.tableFeedName;
        this.clusterName = builder.feedClusterName;
        this.frequency = builder.feedFrequency;
        this.startTime = builder.feedStartTime;
        this.endTime = builder.feedEndTime;
        this.tableName = builder.feedTableName;
        this.aclOwner = builder.feedAclOwner;
        this.partitions = builder.feedPartitions;
    }

    private String getTable() {
        return tableName + TABLE_PARTITION_SEPARATOR + partitions;
    }

    @Override
    public String getEntityxml() throws FalconException {
        try {
            String template = FSUtils.readHDFSFile(ADFJob.TEMPLATE_PATH_PREFIX, TABLE_FEED_TEMPLATE_FILE);
            return template.replace("$feedName$", feedName)
                    .replace("$frequency$", frequency)
                    .replace("$startTime$", startTime)
                    .replace("$endTime$", endTime)
                    .replace("$cluster$", clusterName)
                    .replace("$table$", getTable())
                    .replace("$aclowner$", aclOwner);
        } catch (URISyntaxException e) {
            throw new FalconException("Error when generating entity xml for table feed", e);
        }
    }

    /**
     * Builder for table Feed.
     */
    public static class Builder {
        private String tableFeedName;
        private String feedClusterName;
        private String feedFrequency;
        private String feedStartTime;
        private String feedEndTime;
        private String feedTableName;
        private String feedAclOwner;
        private String feedPartitions;

        public TableFeed build() {
            return new TableFeed(this);
        }

        public Builder withFeedName(final String feedName) {
            this.tableFeedName = feedName;
            return this;
        }

        public Builder withClusterName(final String clusterName) {
            this.feedClusterName = clusterName;
            return this;
        }

        public Builder withFrequency(final String frequency) {
            this.feedFrequency = frequency;
            return this;
        }

        public Builder withStartTime(final String startTime) {
            this.feedStartTime = startTime;
            return this;
        }

        public Builder withEndTime(final String endTime) {
            this.feedEndTime = endTime;
            return this;
        }

        public Builder withTableName(final String tableName) {
            this.feedTableName = tableName;
            return this;
        }

        public Builder withAclOwner(final String aclOwner) {
            this.feedAclOwner = aclOwner;
            return this;
        }

        public Builder withPartitions(final String partitions) {
            this.feedPartitions = partitions;
            return this;
        }
    }

}
