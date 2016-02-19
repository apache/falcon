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
 * Class for data Feed.
 */
public class DataFeed extends Feed {
    private static final String FEED_TEMPLATE_FILE = "feed.xml";
    private String locationPath;

    public DataFeed(final Builder builder) {
        this.feedName = builder.name;
        this.clusterName = builder.feedClusterName;
        this.frequency = builder.feedFrequency;
        this.startTime = builder.feedStartTime;
        this.endTime = builder.feedEndTime;
        this.locationPath = builder.feedLocationPath;
        this.aclOwner = builder.feedAclOwner;
    }

    @Override
    public String getEntityxml() throws FalconException {
        try {
            String template = FSUtils.readHDFSFile(ADFJob.TEMPLATE_PATH_PREFIX, FEED_TEMPLATE_FILE);
            return template.replace("$feedName$", feedName)
                    .replace("$frequency$", frequency)
                    .replace("$startTime$", startTime)
                    .replace("$endTime$", endTime)
                    .replace("$cluster$", clusterName)
                    .replace("$location$", locationPath)
                    .replace("$aclowner$", aclOwner);
        } catch (URISyntaxException e) {
            throw new FalconException("Error when generating entity xml for table feed", e);
        }
    }

    /**
     * Builder for table Feed.
     */
    public static class Builder {
        private String name;
        private String feedClusterName;
        private String feedFrequency;
        private String feedStartTime;
        private String feedEndTime;
        private String feedLocationPath;
        private String feedAclOwner;

        public DataFeed build() {
            return new DataFeed(this);
        }

        public Builder withFeedName(final String feedName) {
            this.name = feedName;
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

        public Builder withLocationPath(final String locationPath) {
            this.feedLocationPath = locationPath;
            return this;
        }

        public Builder withAclOwner(final String aclOwner) {
            this.feedAclOwner = aclOwner;
            return this;
        }
    }
}
