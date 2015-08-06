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

/**
 * Feed Instance Status is used to provide feed instance listing and corresponding status.
 *
 * This is used for exchanging information for getListing api
 */
public class FeedInstanceStatus {

    private String instance;

    private final String uri;

    private long creationTime;

    private long size = -1;

    private String sizeH;

    private AvailabilityStatus status = AvailabilityStatus.MISSING;

    /**
     * Availability status of a feed instance.
     *
     * Missing if the feed partition is entirely missing,
     * Available if present and the availability flag is also present
     * Availability flag is configured in feed definition, but availability flag is missing in data path
     * Empty if the empty
     */
    public enum AvailabilityStatus {MISSING, AVAILABLE, PARTIAL, EMPTY}

    public FeedInstanceStatus(String uri) {
        this.uri = uri;
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public String getUri() {
        return uri;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public long getSize() {
        return size;
    }

    public String getSizeH(){
        return sizeH;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public void setSizeH(String sizeH) {
        this.sizeH = sizeH;
    }


    public AvailabilityStatus getStatus() {
        return status;
    }

    public void setStatus(AvailabilityStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "FeedInstanceStatus{"
                + "instance='" + instance + '\''
                + ", uri='" + uri + '\''
                + ", creationTime=" + creationTime
                + ", size=" + size
                + ", status='" + status + '\''
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FeedInstanceStatus that = (FeedInstanceStatus) o;

        if (creationTime != that.creationTime) {
            return false;
        }
        if (size != that.size) {
            return false;
        }
        if (!instance.equals(that.instance)) {
            return false;
        }
        if (status != that.status) {
            return false;
        }
        if (uri != null ? !uri.equals(that.uri) : that.uri != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = instance.hashCode();
        result = 31 * result + (uri != null ? uri.hashCode() : 0);
        result = 31 * result + (int) (creationTime ^ (creationTime >>> 32));
        result = 31 * result + (int) (size ^ (size >>> 32));
        result = 31 * result + (status != null ? status.hashCode() : 0);
        return result;
    }
}
