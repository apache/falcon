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
    public static enum LEBEL_TYPE {
        @SerializedName("stored-in")STORED_IN,
        @SerializedName("runs-on")RUNS_ON,
        @SerializedName("input")INPUT,
        @SerializedName("output")OUTPUT,

        @SerializedName("instance-of")INSTANCE_ENTITY_EDGE,

        @SerializedName("collocated")CLUSTER_COLO,
        @SerializedName("owned-by")OWNED_BY,
        @SerializedName("grouped-as")GROUPS,
        //custom labels for test tags
        @SerializedName("test")TEST,
        @SerializedName("testname")TESTNAME,
    }

    private String _id;
    private int _outV;
    private int _inV;
    private LEBEL_TYPE _label;

    public String get_id() {
        return _id;
    }

    public int get_outV() {
        return _outV;
    }

    public int get_inV() {
        return _inV;
    }

    public LEBEL_TYPE get_label() {
        return _label;
    }

    @Override
    public String toString() {
        return "Edge{"
                + "_id='" + _id + '\''
                + ", _outV=" + _outV
                + ", _inV=" + _inV
                + ", _label=" + _label
                + '}';
    }

}
