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
import org.apache.commons.lang.StringUtils;
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
@Path("metadata")
public class MetadataDiscoveryResource extends AbstractMetadataResource {

    /**
     * Get list of dimensions for the given dimension-type.
     * <p/>
     * GET http://host/metadata/dimension-type/list
     */
    @GET
    @Path("/{type}/list")
    @Produces({MediaType.APPLICATION_JSON})
    public Response listDimensionValues(@PathParam("type") String type,
                                        @QueryParam("cluster") final String clusterName) {
        RelationshipType relationshipType = validateAndParseDimensionType(type.toUpperCase());
        GraphQuery query = getGraph().query();
        JSONArray dimensionValues = new JSONArray();

        if (StringUtils.isEmpty(clusterName)) {
            query = query.has(RelationshipProperty.TYPE.getName(), relationshipType.getName());
            dimensionValues = getDimensionsFromVertices(dimensionValues,
                    query.vertices().iterator());
        } else {
            // Get clusterVertex, get adjacent vertices of clusterVertex that match dimension type.
            query = query
                    .has(RelationshipProperty.TYPE.getName(), RelationshipType.CLUSTER_ENTITY.getName())
                    .has(RelationshipProperty.NAME.getName(), clusterName);
            Iterator<Vertex> clusterIterator = query.vertices().iterator();
            if (clusterIterator.hasNext()) {
                dimensionValues = getDimensionsFromClusterVertex(
                        dimensionValues, clusterIterator.next(), relationshipType);
            } // else, no cluster found. Return empty results
        }

        try {
            JSONObject response = new JSONObject();
            response.put(RESULTS, dimensionValues);
            response.put(TOTAL_SIZE, dimensionValues.length());
            return Response.ok(response).build();
        } catch (JSONException e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(JSONObject.quote("An error occurred: " + e.getMessage())).build());
        }
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
                    .entity("Invalid Dimension type : " +  type).type("text/plain").build());
        }
    }
}
