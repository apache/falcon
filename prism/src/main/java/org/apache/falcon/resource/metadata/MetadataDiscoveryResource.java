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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Iterator;

/**
 * Jersey Resource for metadata operations.
 */
@Path("metadata/discovery")
public class MetadataDiscoveryResource extends AbstractMetadataResource {

    /**
     * Get list of dimensions for the given dimension-type.
     * <p/>
     * GET http://host/metadata/discovery/dimension-type/list
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
            throw FalconWebException.newException(e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Get relations of a dimension identified by type and name.
     *
     * GET http://host/metadata/discovery/dimension-type/dimension-name/relations
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
            throw FalconWebException.newException(e, Response.Status.INTERNAL_SERVER_ERROR);
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

    private String getVertexRelationshipType(Vertex vertex) {
        String type = vertex.getProperty(RelationshipProperty.TYPE.getName());
        return RelationshipType.fromString(type).toString();
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
            throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST)
                    .entity("Invalid Dimension type : " +  type).type("text/plain").build());
        }

        try {
            return RelationshipType.valueOf(type);
        } catch (IllegalArgumentException iae) {
            throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST)
                    .entity("Invalid Dimension type : " + type).type("text/plain").build());
        }
    }

    private void validateDimensionName(String name) {
        if (StringUtils.isEmpty(name)) {
            throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST)
                    .entity("Dimension name cannot be empty for Relations API").type("text/plain")
                    .build());
        }
    }
}
