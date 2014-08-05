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

import java.util.ArrayList;
import java.util.List;

/** Class for Lineage API result having edges. */
public class EdgesResult extends GraphResult {
    private List<Edge> results;

    public List<Edge> getResults() {
        return results;
    }

    @Override
    public String toString() {
        return String.format("EdgesResult{totalSize=%d, results=%s}", totalSize, results);
    }

    public List<Edge> filterByType(Edge.LEBEL_TYPE edgeLabel) {
        return filterEdgesByType(results, edgeLabel);
    }

    public List<Edge> filterEdgesByType(List<Edge> edges, Edge.LEBEL_TYPE edgeLabel) {
        final List<Edge> result = new ArrayList<Edge>();
        for (Edge edge : edges) {
            if (edge.get_label() == edgeLabel) {
                result.add(edge);
            }
        }
        return result;
    }

}
