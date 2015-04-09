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

package org.apache.falcon.resource;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.entity.v0.feed.LocationType;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Entity list used for marshalling / unmarshalling with REST calls.
 */
@XmlRootElement(name = "feeds")
@XmlAccessorType(XmlAccessType.FIELD)
@edu.umd.cs.findbugs.annotations.SuppressWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class FeedLookupResult extends APIResult {

    @XmlElement(name = "feed")
    private FeedProperties[] elements;

    //For JAXB
    private FeedLookupResult() {
        super();
    }

    public FeedLookupResult(Status status, String message) {
        super(status, message);
    }

    public FeedProperties[] getElements() {
        return elements;
    }

    public void setElements(FeedProperties[] elements) {
        this.elements = elements;
    }


    @Override
    public Object[] getCollection() {
        return getElements();
    }

    @Override
    public void setCollection(Object[] items) {
        if (items == null) {
            setElements(new FeedProperties[0]);
        } else {
            FeedProperties[] newInstances = new FeedProperties[items.length];
            for (int index = 0; index < items.length; index++) {
                newInstances[index] = (FeedProperties)items[index];
            }
            setElements(newInstances);
        }
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        if (elements != null) {
            for (FeedProperties element : elements) {
                buffer.append(element.toString());
            }
        }
        return buffer.toString();
    }

    /**
     * A single instance in the result.
     */
    @XmlRootElement(name = "feed")
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class FeedProperties {
        @XmlElement
        private String feedName;

        @XmlElement
        private LocationType locationType;

        @XmlElement
        private String clusterName;

        public FeedProperties(String feedName, LocationType locationType, String clusterName){
            this.clusterName = clusterName;
            this.locationType = locationType;
            this.feedName = feedName;
        }

        //for JAXB
        private FeedProperties(){}

        public void setFeedName(String feedName) {
            this.feedName = feedName;
        }

        public void setLocationType(LocationType locationType) {
            this.locationType = locationType;
        }

        public void setClusterName(String clusterName) {
            this.clusterName = clusterName;
        }

        public String getFeedName() {
            return this.feedName;
        }

        public LocationType getLocationType() {
            return this.locationType;
        }

        public String getClusterName() {
            return this.clusterName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FeedProperties that = (FeedProperties) o;
            if (!StringUtils.equals(clusterName, that.clusterName)) {
                return false;
            }
            if (locationType != that.locationType) {
                return false;
            }
            if (!StringUtils.equals(feedName, that.feedName)) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = feedName.hashCode();
            result = 31 * result + (locationType != null ? locationType.hashCode() : 0);
            result = 31 * result + (clusterName != null ? clusterName.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return feedName + "  (CLUSTER:" + clusterName + ")  (LocationType:" + locationType.name() + ")";
        }

    }

}
