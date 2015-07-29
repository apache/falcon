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

package org.apache.falcon.regression.core.response.lineage;

import com.google.gson.annotations.SerializedName;

/** Class for representing an edge. */
public class Edge extends GraphEntity {

    /** Class for representing different labels of edge. */
    public static enum LabelType {
        @SerializedName("stored-in")STORED_IN,
        @SerializedName("runs-on")RUNS_ON,
        @SerializedName("input")INPUT,
        @SerializedName("output")OUTPUT,

        @SerializedName("instance-of")INSTANCE_ENTITY_EDGE,

        @SerializedName("collocated")CLUSTER_COLO,
        @SerializedName("owned-by")OWNED_BY,
        @SerializedName("grouped-as")GROUPS,

        @SerializedName("pipeline")PIPELINES,

        // replication labels
        @SerializedName("replicated-to")FEED_CLUSTER_REPLICATED_EDGE,

        // eviction labels
        @SerializedName("evicted-from")FEED_CLUSTER_EVICTED_EDGE,

        //custom labels for test tags
        @SerializedName("test")TEST,
        @SerializedName("testname")TESTNAME,
        @SerializedName("first")FIRST,
        @SerializedName("second")SECOND,
        @SerializedName("third")THIRD,
        @SerializedName("fourth")FOURTH,
        @SerializedName("fifth")FIFTH,
        @SerializedName("sixth")SIXTH,
        @SerializedName("seventh")SEVENTH,
        @SerializedName("eighth")EIGHTH,
        @SerializedName("ninth")NINTH,
        @SerializedName("tenth")TENTH,
        @SerializedName("value")VALUE,
    }
    @SerializedName("_id")
    private String id;

    @SerializedName("_outV")
    private int outV;

    @SerializedName("_inV")
    private int inV;

    @SerializedName("_label")
    private LabelType label;

    public String getId() {
        return id;
    }

    public int getOutV() {
        return outV;
    }

    public int getInV() {
        return inV;
    }

    public LabelType getLabel() {
        return label;
    }

    @Override
    public String toString() {
        return "Edge{"
                + "id='" + id + '\''
                + ", outV=" + outV
                + ", inV=" + inV
                + ", label=" + label
                + '}';
    }

}
