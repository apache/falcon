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

package org.apache.falcon.resource.metadata;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconWebException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.job.ReplicationJobCountersList;
import org.apache.falcon.metadata.RelationshipLabel;
import org.apache.falcon.metadata.RelationshipProperty;
import org.apache.falcon.metadata.RelationshipType;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Jersey Resource for metadata operations.
 */
@Path("metadata/discovery")
public class MetadataDiscoveryResource extends AbstractMetadataResource {

    /**
     * Get list of dimensions for the given dimension-type.
     * <p/>
     * GET http://host/metadata/discovery/dimension-type/list
     * @param clusterName <optional query param> Show dimensions related to this cluster.
     * @param dimensionType Valid dimension types are cluster_entity,feed_entity, process_entity, user, colo, tags,
     *                      groups, pipelines
     * @return List of dimensions that match requested type [and cluster].
     */
    @GET
    @Path("/{type}/list")
    @Produces({MediaType.APPLICATION_JSON})
    public Response listDimensionValues(@PathParam("type") String dimensionType,
                                        @QueryParam("cluster") final String clusterName) {
        RelationshipType relationshipType = validateAndParseDimensionType(dimensionType.toUpperCase());
        GraphQuery query = getGraph().query();
        JSONArray dimensionValues = new JSONArray();

        if (StringUtils.isEmpty(clusterName)) {
            query = query.has(RelationshipProperty.TYPE.getName(), relationshipType.getName());
            dimensionValues = getDimensionsFromVertices(dimensionValues,
                    query.vertices().iterator());
        } else {
            // Get clusterVertex, get adjacent vertices of clusterVertex that match dimension type.
            Vertex clusterVertex = getVertex(clusterName, RelationshipType.CLUSTER_ENTITY.getName());
            if (clusterVertex != null) {
                dimensionValues = getDimensionsFromClusterVertex(dimensionValues,
                        clusterVertex, relationshipType);
            } // else, no cluster found. Return empty results
        }

        try {
            JSONObject response = new JSONObject();
            response.put(RESULTS, dimensionValues);
            response.put(TOTAL_SIZE, dimensionValues.length());
            return Response.ok(response).build();
        } catch (JSONException e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Get list of dimensions for the replication metrics.
     * <p/>
     * GET http://host/metadata/discovery/replication-metrics/list
     * @param schedEntityType Type of the schedulable entity
     * @param schedEntityName Name of the schedulable entity.
     * @param numResults limit the number of metrics to return sorted in ascending order.
     * @return List of dimensions that match requested type [and cluster].
     */
    @GET
    @Path("/{name}/replication-metrics/list")
    @Produces({MediaType.APPLICATION_JSON})
    public Response listReplicationMetricsDimensionValues(@PathParam("name") final String schedEntityName,
                                                          @QueryParam("type") final String schedEntityType,
                                                          @QueryParam("numResults") Integer numResults) {
        JSONArray dimensionValues = new JSONArray();
        int resultsPerQuery = numResults == null ? 10 : numResults;
        if (StringUtils.isNotBlank(schedEntityName)) {
            try {
                EntityUtil.getEntity(schedEntityType, schedEntityName);
            } catch (Throwable e) {
                throw FalconWebException.newAPIException(e, Response.Status.BAD_REQUEST);
            }
            dimensionValues = getReplicationEntityDimensionValues(schedEntityName, resultsPerQuery);
        }

        try {
            JSONObject response = new JSONObject();
            response.put(RESULTS, dimensionValues);
            response.put(TOTAL_SIZE, dimensionValues.length());
            return Response.ok(response).build();
        } catch (JSONException e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }


    public JSONArray getReplicationEntityDimensionValues(final String schedEntityName,
                                            final int resultsPerQuery) {
        // Get schedule entity Vertex, get adjacent vertices of schedule entity Vertex that match dimension type.
        Vertex schedEntityVertex = getVertexByName(schedEntityName);
        if (schedEntityVertex == null) {
            return new JSONArray();
        }
        try {
            Iterator<Edge> inEdges = schedEntityVertex.query().direction(Direction.IN).edges().iterator();
            return getAdjacentVerticesForVertexMetrics(inEdges, Direction.OUT, resultsPerQuery);
        } catch (JSONException e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Get relations of a dimension identified by type and name.
     *
     * GET http://host/metadata/discovery/dimension-type/dimension-name/relations
     * @param dimensionName Name of the dimension.
     * @param dimensionType Valid dimension types are cluster_entity,feed_entity, process_entity, user, colo, tags,
     *                      groups, pipelines
     * @return Get all relations of a specific dimension.
     */
    @GET
    @Path("/{type}/{name}/relations")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getDimensionRelations(@PathParam("type") String dimensionType,
                                          @PathParam("name") String dimensionName) {
        RelationshipType relationshipType = validateAndParseDimensionType(dimensionType.toUpperCase());
        validateDimensionName(dimensionName);
        Vertex dimensionVertex = getVertex(dimensionName, relationshipType.getName());
        if (dimensionVertex == null) {
            return Response.ok(new JSONObject()).build();
        }

        JSONObject vertexProperties;
        try {
            vertexProperties = GraphSONUtility.jsonFromElement(
                    dimensionVertex, getVertexIndexedKeys(), GraphSONMode.NORMAL);
            // over-write the type - fix this kludge
            vertexProperties.put(RelationshipProperty.TYPE.getName(), relationshipType.toString());

            Iterator<Edge> inEdges = dimensionVertex.query().direction(Direction.IN).edges().iterator();
            vertexProperties.put("inVertices", getAdjacentVerticesJson(inEdges, Direction.OUT));

            Iterator<Edge> outEdges = dimensionVertex.query().direction(Direction.OUT).edges().iterator();
            vertexProperties.put("outVertices", getAdjacentVerticesJson(outEdges, Direction.IN));

        } catch (JSONException e) {
            throw FalconWebException.newAPIException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }

        return Response.ok(vertexProperties).build();
    }

    private JSONArray getAdjacentVerticesJson(Iterator<Edge> edges,
                                              Direction direction) throws JSONException {
        JSONArray adjVertices = new JSONArray();
        while (edges.hasNext()) {
            Edge edge = edges.next();
            Vertex vertex = edge.getVertex(direction);
            JSONObject vertexObject = new JSONObject();
            vertexObject.put(RelationshipProperty.NAME.getName(),
                    vertex.getProperty(RelationshipProperty.NAME.getName()));
            vertexObject.put(RelationshipProperty.TYPE.getName(),
                    getVertexRelationshipType(vertex));
            vertexObject.put("label", edge.getLabel());
            adjVertices.put(vertexObject);
        }

        return adjVertices;
    }

    private JSONArray getAdjacentVerticesForVertexMetrics(Iterator<Edge> edges, Direction direction,
                                                          int resultsPerQuery) throws JSONException {
        JSONArray adjVertices = new JSONArray();
        Iterator<Edge> sortedEdge = sortEdgesById(edges, direction);
        while (sortedEdge.hasNext() && resultsPerQuery!=0) {
            Edge edge = sortedEdge.next();
            Vertex vertex = edge.getVertex(direction);
            if (vertex.getProperty(ReplicationJobCountersList.BYTESCOPIED.getName()) != null) {
                JSONObject vertexObject = new JSONObject();
                vertexObject.put(RelationshipProperty.NAME.getName(),
                        vertex.getProperty(RelationshipProperty.NAME.getName()));
                vertexObject.put(ReplicationJobCountersList.TIMETAKEN.getName(),
                        vertex.getProperty(ReplicationJobCountersList.TIMETAKEN.getName()));
                vertexObject.put(ReplicationJobCountersList.BYTESCOPIED.getName(),
                        vertex.getProperty(ReplicationJobCountersList.BYTESCOPIED.getName()));
                vertexObject.put(ReplicationJobCountersList.COPY.getName(),
                        vertex.getProperty(ReplicationJobCountersList.COPY.getName()));
                adjVertices.put(vertexObject);

                resultsPerQuery--;
            }
        }

        return adjVertices;
    }

    Iterator<Edge> sortEdgesById(Iterator<Edge> edges, final Direction direction) {
        List<Edge> edgeList = new ArrayList<Edge>();

        while(edges.hasNext()) {
            Edge e = edges.next();
            edgeList.add(e);
        }

        Collections.sort(edgeList, new Comparator<Edge>() {
            @Override
            public int compare(Edge e1, Edge e2) {
                long l1 = (long)e1.getVertex(direction).getId();
                long l2 = (long)e2.getVertex(direction).getId();

                return l1 > l2 ? -1 : 1;
            }
        });

        return edgeList.iterator();
    }

    private String getVertexRelationshipType(Vertex vertex) {
        String type = vertex.getProperty(RelationshipProperty.TYPE.getName());
        return RelationshipType.fromString(type).toString();
    }

    private Vertex getVertexByName(String name) {
        Iterator<Vertex> vertexIterator = getGraph().query()
                .has(RelationshipProperty.NAME.getName(), name)
                .vertices().iterator();

        return vertexIterator.hasNext() ? vertexIterator.next() : null;
    }

    private Vertex getVertex(String name, String type) {
        Iterator<Vertex> vertexIterator = getGraph().query()
                .has(RelationshipProperty.TYPE.getName(), type)
                .has(RelationshipProperty.NAME.getName(), name)
                .vertices().iterator();

        return vertexIterator.hasNext() ? vertexIterator.next() :  null;
    }

    private JSONArray getDimensionsFromClusterVertex(JSONArray dimensionValues, Vertex clusterVertex,
                                                     RelationshipType relationshipType) {
        switch (relationshipType) {
        case FEED_ENTITY:
            return getDimensionsFromEdges(dimensionValues, Direction.OUT,
                    clusterVertex.getEdges(
                            Direction.IN, RelationshipLabel.FEED_CLUSTER_EDGE.getName()).iterator(),
                    relationshipType);

        case PROCESS_ENTITY:
            return getDimensionsFromEdges(dimensionValues, Direction.OUT,
                    clusterVertex.getEdges(
                            Direction.IN, RelationshipLabel.PROCESS_CLUSTER_EDGE.getName()).iterator(),
                    relationshipType);

        case COLO:
            return getDimensionsFromEdges(dimensionValues, Direction.IN,
                    clusterVertex.getEdges(
                            Direction.OUT, RelationshipLabel.CLUSTER_COLO.getName()).iterator(),
                    relationshipType);

        case CLUSTER_ENTITY:
            return getDimensionFromVertex(dimensionValues, clusterVertex);

        default:
            return dimensionValues;
        }
    }

    private JSONArray getDimensionsFromVertices(JSONArray dimensionValues,
                                                Iterator<Vertex> vertexIterator) {
        while (vertexIterator.hasNext()) {
            dimensionValues = getDimensionFromVertex(dimensionValues, vertexIterator.next());
        }

        return dimensionValues;
    }

    private JSONArray getDimensionsFromEdges(JSONArray dimensionValues, Direction direction,
                                             Iterator<Edge> edgeIterator,
                                             RelationshipType relationshipType) {
        while(edgeIterator.hasNext()) {
            Vertex vertex = edgeIterator.next().getVertex(direction);
            if (vertex.getProperty(RelationshipProperty.TYPE.getName())
                    .equals(relationshipType.getName())) {
                dimensionValues = getDimensionFromVertex(dimensionValues, vertex);
            }
        }

        return dimensionValues;
    }

    private JSONArray getDimensionFromVertex(JSONArray dimensionValues, Vertex vertex) {
        return dimensionValues.put(vertex.getProperty(RelationshipProperty.NAME.getName()));
    }

    private RelationshipType validateAndParseDimensionType(String type) {
        if (StringUtils.isEmpty(type) || type.contains("INSTANCE")) {
            throw FalconWebException.newMetadataResourceException(
                    "Invalid Dimension type : " + type, Response.Status.BAD_REQUEST);
        }

        try {
            return RelationshipType.valueOf(type);
        } catch (IllegalArgumentException iae) {
            throw FalconWebException.newMetadataResourceException(
                    "Invalid Dimension type : " + type, Response.Status.BAD_REQUEST);
        }
    }

    private void validateDimensionName(String name) {
        if (StringUtils.isEmpty(name)) {
            throw FalconWebException.newMetadataResourceException(
                    "Dimension name cannot be empty for Relations API", Response.Status.BAD_REQUEST);
        }
    }
}
