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

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.falcon.security.CurrentUser;
import org.apache.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;

/**
 * Base class for Metadata relationship mapping helper.
 */
public abstract class RelationshipGraphBuilder {

    private static final Logger LOG = Logger.getLogger(RelationshipGraphBuilder.class);

    // vertex property keys
    public static final String NAME_PROPERTY_KEY = "name";
    public static final String TYPE_PROPERTY_KEY = "type";
    public static final String TIMESTAMP_PROPERTY_KEY = "timestamp";
    public static final String VERSION_PROPERTY_KEY = "version";

    // vertex types
    public static final String USER_TYPE = "user";
    public static final String COLO_TYPE = "data-center";
    public static final String TAGS_TYPE = "classification";
    public static final String GROUPS_TYPE = "group";

    // edge labels
    public static final String USER_LABEL = "owned-by";
    public static final String CLUSTER_COLO_LABEL = "collocated";
    public static final String GROUPS_LABEL = "grouped-as";

    // entity edge labels
    public static final String FEED_CLUSTER_EDGE_LABEL = "stored-in";
    public static final String PROCESS_CLUSTER_EDGE_LABEL = "runs-on";
    public static final String FEED_PROCESS_EDGE_LABEL = "input";
    public static final String PROCESS_FEED_EDGE_LABEL = "output";

    /**
     * A blueprints graph.
     */
    private final KeyIndexableGraph graph;

    /**
     * If enabled, preserves history of tags and groups for instances else will only
     * be available for entities.
     */
    private final boolean preserveHistory;

    protected RelationshipGraphBuilder(KeyIndexableGraph graph, boolean preserveHistory) {
        this.graph = graph;
        this.preserveHistory = preserveHistory;
    }

    protected KeyIndexableGraph getGraph() {
        return graph;
    }

    protected boolean isPreserveHistory() {
        return preserveHistory;
    }

    public Vertex addVertex(String name, String type) {
        Vertex vertex = findVertex(name, type);
        if (vertex != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Found an existing vertex for: name=" + name + ", type=" + type);
            }

            return vertex;
        }

        return createVertex(name, type);
    }

    protected Vertex addVertex(String name, String type, String timestamp) {
        Vertex vertex = findVertex(name, type);
        if (vertex != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Found an existing vertex for: name=" + name + ", type=" + type);
            }

            return vertex;
        }

        return createVertex(name, type, timestamp);
    }

    protected Vertex findVertex(String name, String type) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Finding vertex for: name=" + name + ", type=" + type);
        }

        GraphQuery query = graph.query()
                .has(NAME_PROPERTY_KEY, name)
                .has(TYPE_PROPERTY_KEY, type);
        Iterator<Vertex> results = query.vertices().iterator();
        return results.hasNext() ? results.next() : null;
    }

    protected Vertex createVertex(String name, String type) {
        return createVertex(name, type, getCurrentTimeStamp());
    }

    protected Vertex createVertex(String name, String type, String timestamp) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating a new vertex for: name=" + name + ", type=" + type);
        }

        Vertex vertex = graph.addVertex(null);
        vertex.setProperty(NAME_PROPERTY_KEY, name);
        vertex.setProperty(TYPE_PROPERTY_KEY, type);
        vertex.setProperty(TIMESTAMP_PROPERTY_KEY, timestamp);

        return vertex;
    }

    protected Edge addEdge(Vertex fromVertex, Vertex toVertex, String edgeLabel) {
        Edge edge = findEdge(fromVertex, edgeLabel);
        return edgeExists(edge, toVertex) ? edge : fromVertex.addEdge(edgeLabel, toVertex);
    }

    protected void removeEdge(Vertex fromVertex, Vertex toVertex, String edgeLabel) {
        Edge edge = findEdge(fromVertex, edgeLabel);
        if (edgeExists(edge, toVertex)) {
            getGraph().removeEdge(edge);
        }
    }

    protected void removeEdge(Vertex fromVertex, Object toVertexName, String edgeLabel) {
        Edge edge = findEdge(fromVertex, edgeLabel);
        if (edgeExists(edge, toVertexName)) {
            getGraph().removeEdge(edge);
        }
    }

    protected boolean edgeExists(Edge edge, Object toVertexName) {
        return edge != null && edge.getVertex(Direction.IN).getProperty(NAME_PROPERTY_KEY)
                .equals(toVertexName);
    }

    protected boolean edgeExists(Edge edge, Vertex toVertex) {
        return edge != null && edge.getVertex(Direction.IN).getProperty(NAME_PROPERTY_KEY)
                .equals(toVertex.getProperty(NAME_PROPERTY_KEY));
    }

    protected Edge findEdge(Vertex fromVertex, String edgeLabel) {
        Iterator<Edge> edges = fromVertex.getEdges(Direction.OUT, edgeLabel).iterator();
        return edges.hasNext() ? edges.next() : null;
    }

    protected void addUserRelation(Vertex fromVertex) {
        addUserRelation(fromVertex, USER_LABEL);
    }

    protected void addUserRelation(Vertex fromVertex, String edgeLabel) {
        Vertex relationToUserVertex = addVertex(CurrentUser.getUser(), USER_TYPE);
        addEdge(fromVertex, relationToUserVertex, edgeLabel);
    }

    protected void addDataClassification(String classification, Vertex entityVertex) {
        if (classification == null || classification.length() == 0) {
            return;
        }

        String[] tags = classification.split(",");
        for (String tag : tags) {
            int index = tag.indexOf("=");
            String tagKey = tag.substring(0, index);
            String tagValue = tag.substring(index + 1, tag.length());

            Vertex tagValueVertex = addVertex(tagValue, TAGS_TYPE);
            addEdge(entityVertex, tagValueVertex, tagKey);
        }
    }

    protected void addGroups(String groups, Vertex fromVertex) {
        if (groups == null || groups.length() == 0) {
            return;
        }

        String[] groupTags = groups.split(",");
        for (String groupTag : groupTags) {
            Vertex groupVertex = addVertex(groupTag, GROUPS_TYPE);
            addEdge(fromVertex, groupVertex, GROUPS_LABEL);
        }
    }

    protected void addProcessFeedEdge(Vertex processVertex, Vertex feedVertex, String edgeLabel) {
        if (edgeLabel.equals(FEED_PROCESS_EDGE_LABEL)) {
            addEdge(feedVertex, processVertex, edgeLabel);
        } else {
            addEdge(processVertex, feedVertex, edgeLabel);
        }
    }

    protected String getCurrentTimeStamp() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateFormat.format(new Date());
    }

    protected void addProperty(Vertex vertex, Map<String, String> lineageMetadata, String optionName) {
        String value = lineageMetadata.get(optionName);
        if (value == null || value.length() == 0) {
            return;
        }

        vertex.setProperty(optionName, value);
    }
}
