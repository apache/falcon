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

/** Class for Lineage API result having vertices. */
public class VerticesResult extends GraphResult {
    private List<Vertex> results;

    public List<Vertex> getResults() {
        return results;
    }

    @Override
    public String toString() {
        return String.format("VerticesResult{totalSize=%d, results=%s}", totalSize, results);
    }

    public List<Vertex> filterByType(Vertex.VERTEX_TYPE vertexType) {
        return filterVerticesByType(vertexType, results);
    }

    public List<Vertex> filterVerticesByType(Vertex.VERTEX_TYPE vertexType,
                                             List<Vertex> vertexList) {
        List<Vertex> result = new ArrayList<Vertex>();
        for (Vertex vertex : vertexList) {
            if (vertex.getType() == vertexType) {
                result.add(vertex);
            }
        }
        return result;
    }

    public List<Vertex> filterByName(String name) {
        return filterVerticesByName(name, results);
    }

    public List<Vertex> filterVerticesByName(String name, List<Vertex> vertexList) {
        List<Vertex> result = new ArrayList<Vertex>();
        for (Vertex vertex : vertexList) {
            if (vertex.getName().equals(name)) {
                result.add(vertex);
            }
        }
        return result;
    }

}
