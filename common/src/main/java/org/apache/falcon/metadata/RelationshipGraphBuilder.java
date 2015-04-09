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
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.security.CurrentUser;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Iterator;

/**
 * Base class for Metadata relationship mapping helper.
 */
public abstract class RelationshipGraphBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(RelationshipGraphBuilder.class);

    /**
     * A blueprints graph.
     */
    private final Graph graph;

    /**
     * If enabled, preserves history of tags and groups for instances else will only
     * be available for entities.
     */
    private final boolean preserveHistory;

    protected RelationshipGraphBuilder(Graph graph, boolean preserveHistory) {
        this.graph = graph;
        this.preserveHistory = preserveHistory;
    }

    public Graph getGraph() {
        return graph;
    }

    protected boolean isPreserveHistory() {
        return preserveHistory;
    }

    public Vertex addVertex(String name, RelationshipType type) {
        Vertex vertex = findVertex(name, type);
        if (vertex != null) {
            LOG.debug("Found an existing vertex for: name={}, type={}", name, type);
            return vertex;
        }

        return createVertex(name, type);
    }

    protected Vertex addVertex(String name, RelationshipType type, long timestamp) {
        Vertex vertex = findVertex(name, type);
        if (vertex != null) {
            LOG.debug("Found an existing vertex for: name={}, type={}", name, type);
            return vertex;
        }

        return createVertex(name, type, timestamp);
    }

    protected Vertex findVertex(String name, RelationshipType type) {
        LOG.debug("Finding vertex for: name={}, type={}", name, type);

        GraphQuery query = graph.query()
                .has(RelationshipProperty.NAME.getName(), name)
                .has(RelationshipProperty.TYPE.getName(), type.getName());
        Iterator<Vertex> results = query.vertices().iterator();
        return results.hasNext() ? results.next() : null;  // returning one since name is unique
    }

    protected Vertex createVertex(String name, RelationshipType type) {
        return createVertex(name, type, System.currentTimeMillis());
    }

    protected Vertex createVertex(String name, RelationshipType type, long timestamp) {
        LOG.debug("Creating a new vertex for: name={}, type={}", name, type);

        Vertex vertex = graph.addVertex(null);
        vertex.setProperty(RelationshipProperty.NAME.getName(), name);
        vertex.setProperty(RelationshipProperty.TYPE.getName(), type.getName());
        vertex.setProperty(RelationshipProperty.TIMESTAMP.getName(), timestamp);

        return vertex;
    }

    protected Edge addEdge(Vertex fromVertex, Vertex toVertex, String edgeLabel) {
        return addEdge(fromVertex, toVertex, edgeLabel, null);
    }

    protected Edge addEdge(Vertex fromVertex, Vertex toVertex,
                           String edgeLabel, String timestamp) {
        Edge edge = findEdge(fromVertex, toVertex, edgeLabel);

        Edge edgeToVertex = edge != null ? edge : fromVertex.addEdge(edgeLabel, toVertex);
        if (timestamp != null) {
            edgeToVertex.setProperty(RelationshipProperty.TIMESTAMP.getName(), timestamp);
        }

        return edgeToVertex;
    }

    protected void removeEdge(Vertex fromVertex, Vertex toVertex, String edgeLabel) {
        Edge edge = findEdge(fromVertex, toVertex, edgeLabel);
        if (edge != null) {
            getGraph().removeEdge(edge);
        }
    }

    protected void removeEdge(Vertex fromVertex, Object toVertexName, String edgeLabel) {
        Edge edge = findEdge(fromVertex, toVertexName, edgeLabel);
        if (edge != null) {
            getGraph().removeEdge(edge);
        }
    }

    protected Edge findEdge(Vertex fromVertex, Vertex toVertex, String edgeLabel) {
        return findEdge(fromVertex, toVertex.getProperty(RelationshipProperty.NAME.getName()), edgeLabel);
    }

    protected Edge findEdge(Vertex fromVertex, Object toVertexName, String edgeLabel) {
        Edge edgeToFind = null;
        for (Edge edge : fromVertex.getEdges(Direction.OUT, edgeLabel)) {
            if (edge.getVertex(Direction.IN).getProperty(RelationshipProperty.NAME.getName()).equals(toVertexName)) {
                edgeToFind = edge;
                break;
            }
        }

        return edgeToFind;
    }

    protected void addUserRelation(Vertex fromVertex) {
        addUserRelation(fromVertex, RelationshipLabel.USER.getName());
    }

    protected void addUserRelation(Vertex fromVertex, String edgeLabel) {
        Vertex relationToUserVertex = addVertex(CurrentUser.getUser(), RelationshipType.USER);
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

            Vertex tagValueVertex = addVertex(tagValue, RelationshipType.TAGS);
            addEdge(entityVertex, tagValueVertex, tagKey);
        }
    }

    protected void addGroups(String groups, Vertex fromVertex) {
        addCSVTags(groups, fromVertex, RelationshipType.GROUPS, RelationshipLabel.GROUPS);
    }

    protected void addPipelines(String pipelines, Vertex fromVertex) {
        addCSVTags(pipelines, fromVertex, RelationshipType.PIPELINES, RelationshipLabel.PIPELINES);
    }

    protected void addProcessFeedEdge(Vertex processVertex, Vertex feedVertex,
                                      RelationshipLabel edgeLabel) {
        if (edgeLabel == RelationshipLabel.FEED_PROCESS_EDGE) {
            addEdge(feedVertex, processVertex, edgeLabel.getName());
        } else {
            addEdge(processVertex, feedVertex, edgeLabel.getName());
        }
    }

    protected String getCurrentTimeStamp() {
        return SchemaHelper.formatDateUTC(new Date());
    }

    /**
     * Adds comma separated values as tags.
     *
     * @param csvTags           comma separated values.
     * @param fromVertex        from vertex.
     * @param relationshipType  vertex type.
     * @param edgeLabel         edge label.
     */
    private void addCSVTags(String csvTags, Vertex fromVertex,
                            RelationshipType relationshipType, RelationshipLabel edgeLabel) {
        if (StringUtils.isEmpty(csvTags)) {
            return;
        }

        String[] tags = csvTags.split(",");
        for (String tag : tags) {
            Vertex vertex = addVertex(tag, relationshipType);
            addEdge(fromVertex, vertex, edgeLabel.getName());
        }
    }
}
