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

package org.apache.falcon.regression.core.util;

import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.core.response.lineage.Edge;
import org.apache.falcon.regression.core.response.lineage.EdgesResult;
import org.apache.falcon.regression.core.response.lineage.GraphResult;
import org.apache.falcon.regression.core.response.lineage.NODE_TYPE;
import org.apache.falcon.regression.core.response.lineage.Vertex;
import org.apache.falcon.regression.core.response.lineage.VerticesResult;
import org.apache.log4j.Logger;
import org.testng.Assert;

/**
 * util methods for Graph Asserts.
 */
public final class GraphAssert {
    private GraphAssert() {
        throw new AssertionError("Instantiating utility class...");
    }
    private static final Logger LOGGER = Logger.getLogger(GraphAssert.class);

    /**
     * Check that the result has certain minimum number of vertices.
     * @param graphResult the result to be checked
     * @param minNumOfVertices required number of vertices
     */
    public static void checkVerticesPresence(final GraphResult graphResult,
                                             final int minNumOfVertices) {
        Assert.assertTrue(graphResult.getTotalSize() >= minNumOfVertices,
            "graphResult should have at least " + minNumOfVertices + " vertex");
    }

    /**
     * Check that the vertices in the result are sane.
     * @param verticesResult the result to be checked
     */
    public static void assertVertexSanity(final VerticesResult verticesResult) {
        Assert.assertEquals(verticesResult.getResults().size(), verticesResult.getTotalSize(),
            "Size of vertices don't match");
        for (Vertex vertex : verticesResult.getResults()) {
            Assert.assertNotNull(vertex.get_id(),
                "id of the vertex should be non-null: " + vertex);
            Assert.assertEquals(vertex.get_type(), NODE_TYPE.VERTEX,
                "_type of the vertex should be non-null: " + vertex);
            Assert.assertNotNull(vertex.getName(),
                "name of the vertex should be non-null: " + vertex);
            Assert.assertNotNull(vertex.getType(),
                "id of the vertex should be non-null: " + vertex);
            Assert.assertNotNull(vertex.getTimestamp(),
                "id of the vertex should be non-null: " + vertex);
        }
    }

    /**
     * Check that edges in the result are sane.
     * @param edgesResult result to be checked
     */
    public static void assertEdgeSanity(final EdgesResult edgesResult) {
        Assert.assertEquals(edgesResult.getResults().size(), edgesResult.getTotalSize(),
            "Size of edges don't match");
        for (Edge edge : edgesResult.getResults()) {
            assertEdgeSanity(edge);
        }
    }

    /**
     * Check that edge is sane.
     * @param edge edge to be checked
     */
    public static void assertEdgeSanity(Edge edge) {
        Assert.assertNotNull(edge.get_id(), "id of an edge can't be null: " + edge);
        Assert.assertEquals(edge.get_type(), NODE_TYPE.EDGE,
            "_type of an edge can't be null: " + edge);
        Assert.assertNotNull(edge.get_label(), "_label of an edge can't be null: " + edge);
        Assert.assertTrue(edge.get_inV() > 0, "_inV of an edge can't be null: " + edge);
        Assert.assertTrue(edge.get_outV() > 0, "_outV of an edge can't be null: " + edge);
    }

    /**
     * Check that user vertex is present.
     * @param verticesResult the result to be checked
     */
    public static void assertUserVertexPresence(final VerticesResult verticesResult) {
        checkVerticesPresence(verticesResult, 1);
        for(Vertex vertex : verticesResult.getResults()) {
            if (vertex.getType() == Vertex.VERTEX_TYPE.USER
                    && vertex.getName().equals(MerlinConstants.CURRENT_USER_NAME)) {
                return;
            }
        }
        Assert.fail(String.format("Vertex corresponding to user: %s is not present.",
            MerlinConstants.CURRENT_USER_NAME));
    }

    /**
     * Check that a vertex of a certain name is present.
     * @param verticesResult the result to be checked
     * @param name expected name
     */
    public static void assertVertexPresence(final VerticesResult verticesResult, final String name) {
        checkVerticesPresence(verticesResult, 1);
        for (Vertex vertex : verticesResult.getResults()) {
            if (vertex.getName().equals(name)) {
                return;
            }
        }
        Assert.fail(String.format("Vertex of name: %s is not present.", name));
    }

    /**
     * Check that the result has at least a certain number of vertices of a certain type.
     * @param verticesResult the result to be checked
     * @param vertexType vertex type
     * @param minOccurrence required number of vertices
     */
    public static void assertVerticesPresenceMinOccur(final VerticesResult verticesResult,
                                                      final Vertex.VERTEX_TYPE vertexType,
                                                      final int minOccurrence) {
        int occurrence = 0;
        for(Vertex vertex : verticesResult.getResults()) {
            if (vertex.getType() == vertexType) {
                LOGGER.info("Found vertex: " + vertex);
                occurrence++;
                if (occurrence >= minOccurrence) {
                    return;
                }
            }
        }
        Assert.fail(String.format("Expected at least %d vertices of type %s. But found only %d",
            minOccurrence, vertexType, occurrence));
    }

    /**
     * Check result to contain at least a certain number of edges of a certain type.
     * @param edgesResult result to be checked
     * @param edgeLabel edge label
     * @param minOccurrence required number of edges
     */
    public static void assertEdgePresenceMinOccur(final EdgesResult edgesResult,
                                                  final Edge.LEBEL_TYPE edgeLabel,
                                                  final int minOccurrence) {
        int occurrence = 0;
        for(Edge edge : edgesResult.getResults()) {
            if (edge.get_label() == edgeLabel) {
                LOGGER.info("Found edge: " + edge);
                occurrence++;
                if (occurrence >= minOccurrence) {
                    return;
                }
            }
        }
        Assert.fail(String.format("Expected at least %d vertices of type %s. But found only %d",
            minOccurrence, edgeLabel, occurrence));
    }
}
