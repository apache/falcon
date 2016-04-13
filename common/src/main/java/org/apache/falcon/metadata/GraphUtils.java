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

package org.apache.falcon.metadata;

import org.apache.commons.lang3.StringUtils;
import com.thinkaurelius.titan.core.BaseVertexQuery;
import com.thinkaurelius.titan.core.Order;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Query.Compare;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

/**
 * Utility class for graph operations.
 */
public final class GraphUtils {

    private static final Logger LOG = LoggerFactory.getLogger(GraphUtils.class);

    private GraphUtils() {
    }

    public static void dumpToLog(final Graph graph) {
        LOG.debug("Vertices of {}", graph);
        for (Vertex vertex : graph.getVertices()) {
            LOG.debug(vertexString(vertex));
        }

        LOG.debug("Edges of {}", graph);
        for (Edge edge : graph.getEdges()) {
            LOG.debug(edgeString(edge));
        }
    }

    public static void dump(final Graph graph) throws IOException {
        dump(graph, System.out);
    }

    public static void dump(final Graph graph, OutputStream outputStream) throws IOException {
        GraphSONWriter.outputGraph(graph, outputStream);
    }

    public static void dump(final Graph graph, String fileName) throws IOException {
        GraphSONWriter.outputGraph(graph, fileName);
    }

    public static String vertexString(final Vertex vertex) {
        StringBuilder properties = new StringBuilder();
        for (String propertyKey : vertex.getPropertyKeys()) {
            properties.append(propertyKey)
                    .append("=").append(vertex.getProperty(propertyKey))
                    .append(", ");
        }

        return "v[" + vertex.getId() + "], Properties[" + properties + "]";
    }

    public static String edgeString(final Edge edge) {
        return "e[" + edge.getLabel() + "], ["
                + edge.getVertex(Direction.OUT).getProperty("name")
                + " -> " + edge.getLabel() + " -> "
                + edge.getVertex(Direction.IN).getProperty("name")
                + "]";
    }

    public static Vertex findVertex(Graph graph, String name, RelationshipType relationshipType) {
        LOG.debug("Finding vertex for: name={}, type={}", name, relationshipType.getName());
        GraphQuery query = graph.query()
                .has(RelationshipProperty.NAME.getName(), name)
                .has(RelationshipProperty.TYPE.getName(), relationshipType.getName());
        Iterator<Vertex> results = query.vertices().iterator();
        return results.hasNext() ? results.next() : null;  // returning one since name is unique
    }

    public static BaseVertexQuery addRangeQuery(BaseVertexQuery query,
                                                RelationshipProperty property, String minValue, String maxValue) {
        if (StringUtils.isNotEmpty(minValue)) {
            query.has(property.getName(), Compare.GREATER_THAN_EQUAL, minValue);
        }
        if (StringUtils.isNotEmpty(maxValue)) {
            query.has(property.getName(), Compare.LESS_THAN_EQUAL, maxValue);
        }
        return query;
    }

    public static BaseVertexQuery addEqualityQuery(BaseVertexQuery query, RelationshipProperty property, String value) {
        if (StringUtils.isNotEmpty(value)) {
            query.has(property.getName(), value);
        }
        return query;
    }

    public static BaseVertexQuery addOrderLimitQuery(BaseVertexQuery query, String orderBy, int numResults) {
        if (StringUtils.isNotEmpty(orderBy)) {
            query.orderBy(orderBy, Order.DESC);
        }
        query.limit(numResults);
        return query;
    }
}
