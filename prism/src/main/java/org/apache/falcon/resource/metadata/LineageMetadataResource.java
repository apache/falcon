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
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility;
import org.apache.falcon.metadata.GraphUtils;
import org.apache.falcon.metadata.MetadataMappingService;
import org.apache.falcon.metadata.RelationshipLabel;
import org.apache.falcon.metadata.RelationshipProperty;
import org.apache.falcon.metadata.RelationshipType;
import org.apache.falcon.service.Services;
import org.apache.falcon.util.StartupProperties;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Jersey Resource for lineage metadata operations.
 * Implements most of the GET operations of Rexster API with out the indexes.
 * https://github.com/tinkerpop/rexster/wiki/Basic-REST-API
 */
@Path("graphs/lineage")
public class LineageMetadataResource {

    private static final Logger LOG = LoggerFactory.getLogger(LineageMetadataResource.class);

    public static final String RESULTS = "results";
    public static final String TOTAL_SIZE = "totalSize";

    private final MetadataMappingService service;

    public LineageMetadataResource() {
        if (Services.get().isRegistered(MetadataMappingService.SERVICE_NAME)) {
            service = Services.get().getService(MetadataMappingService.SERVICE_NAME);
        } else {
            service = null;
        }
    }

    private Graph getGraph() {
        return service.getGraph();
    }

    private Set<String> getVertexIndexedKeys() {
        return service.getVertexIndexedKeys();
    }

    private Set<String> getEdgeIndexedKeys() {
        return service.getEdgeIndexedKeys();
    }

    /**
     * Dump the graph.
     *
     * GET http://host/graphs/lineage/serialize
     * graph.getVertices();
     */
    @GET
    @Path("/serialize")
    @Produces({MediaType.APPLICATION_JSON})
    public Response serializeGraph() {
        checkIfMetadataMappingServiceIsEnabled();
        String file = StartupProperties.get().getProperty("falcon.graph.serialize.path")
                + "/lineage-graph-" + System.currentTimeMillis() + ".json";
        LOG.info("Serialize Graph to: {}", file);
        try {
            GraphUtils.dump(getGraph(), file);
            return Response.ok().build();
        } catch (Exception e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(JSONObject.quote("An error occurred: " + e.getMessage())).build());
        }
    }

    /**
     * Get all vertices.
     *
     * GET http://host/graphs/lineage/vertices/all
     * graph.getVertices();
     */
    @GET
    @Path("/vertices/all")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getVertices() {
        checkIfMetadataMappingServiceIsEnabled();
        LOG.info("Get All Vertices");
        try {
            JSONObject response = buildJSONResponse(getGraph().getVertices());
            return Response.ok(response).build();
        } catch (JSONException e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(JSONObject.quote("An error occurred: " + e.getMessage())).build());
        }
    }

    /**
     * Get a single vertex with a unique id.
     *
     * GET http://host/graphs/lineage/vertices/id
     * graph.getVertex(id);
     */
    @GET
    @Path("/vertices/{id}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getVertex(@PathParam("id") final String vertexId) {
        checkIfMetadataMappingServiceIsEnabled();
        LOG.info("Get vertex for vertexId= {}", vertexId);
        try {
            Vertex vertex = findVertex(vertexId);

            JSONObject response = new JSONObject();
            response.put(RESULTS, GraphSONUtility.jsonFromElement(
                    vertex, getVertexIndexedKeys(), GraphSONMode.NORMAL));
            return Response.ok(response).build();
        } catch (JSONException e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(JSONObject.quote("An error occurred: " + e.getMessage())).build());
        }
    }

    private Vertex findVertex(String vertexId) {
        Vertex vertex = getGraph().getVertex(vertexId);
        if (vertex == null) {
            String message = "Vertex with [" + vertexId + "] cannot be found.";
            LOG.info(message);
            throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND)
                    .entity(JSONObject.quote(message)).build());
        }

        return vertex;
    }

    /**
     * Get properties for a single vertex with a unique id.
     * This is NOT a rexster API.
     * <p/>
     * GET http://host/graphs/lineage/vertices/properties/id
     */
    @GET
    @Path("/vertices/properties/{id}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getVertexProperties(@PathParam("id") final String vertexId,
                                        @DefaultValue("false") @QueryParam("relationships")
                                        final String relationships) {
        checkIfMetadataMappingServiceIsEnabled();
        LOG.info("Get vertex for vertexId= {}", vertexId);
        try {
            Vertex vertex = findVertex(vertexId);

            Map<String, String> vertexProperties = getVertexProperties(vertex, Boolean.valueOf(relationships));

            JSONObject response = new JSONObject();
            response.put(RESULTS, new JSONObject(vertexProperties));
            response.put(TOTAL_SIZE, vertexProperties.size());
            return Response.ok(response).build();
        } catch (JSONException e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(JSONObject.quote("An error occurred: " + e.getMessage())).build());
        }
    }

    private Map<String, String> getVertexProperties(Vertex vertex, boolean captureRelationships) {
        Map<String, String> vertexProperties = new HashMap<String, String>();
        for (String key : vertex.getPropertyKeys()) {
            vertexProperties.put(key, vertex.<String>getProperty(key));
        }

        RelationshipType vertexType = RelationshipType.fromString(
                vertex.<String>getProperty(RelationshipProperty.TYPE.getName()));
        // get the properties from relationships
        if (captureRelationships && (vertexType == RelationshipType.FEED_INSTANCE
                || vertexType == RelationshipType.PROCESS_INSTANCE)) {
            for (Edge edge : vertex.getEdges(Direction.OUT)) {
                Vertex toVertex = edge.getVertex(Direction.IN);
                addRelationships(vertexType, toVertex, vertexProperties);
            }
        }

        return vertexProperties;
    }

    private void addRelationships(RelationshipType fromVertexType, Vertex toVertex,
                                  Map<String, String> vertexProperties) {
        String value = toVertex.getProperty(RelationshipProperty.NAME.getName());
        RelationshipType toVertexType = RelationshipType.fromString(
                toVertex.<String>getProperty(RelationshipProperty.TYPE.getName()));

        switch (toVertexType) {
        case CLUSTER_ENTITY:
            String key = fromVertexType == RelationshipType.FEED_INSTANCE
                    ? RelationshipLabel.FEED_CLUSTER_EDGE.getName()
                    : RelationshipLabel.PROCESS_CLUSTER_EDGE.getName();
            vertexProperties.put(key, value);
            break;

        case USER:
            vertexProperties.put(RelationshipLabel.USER.getName(), value);
            break;

        case FEED_ENTITY:
            addEntityRelationships(toVertex, vertexProperties);
            break;

        case PROCESS_ENTITY:
            addEntityRelationships(toVertex, vertexProperties);
            break;

        default:
        }
    }

    private void addEntityRelationships(Vertex vertex, Map<String, String> vertexProperties) {
        for (Edge edge : vertex.getEdges(Direction.OUT)) {
            Vertex toVertex = edge.getVertex(Direction.IN);
            String value = toVertex.getProperty(RelationshipProperty.NAME.getName());
            RelationshipType toVertexType = RelationshipType.fromString(
                    toVertex.<String>getProperty(RelationshipProperty.TYPE.getName()));

            switch (toVertexType) {
            case TAGS:
                vertexProperties.put(edge.getLabel(), value);
                break;

            case GROUPS:
                vertexProperties.put(RelationshipLabel.GROUPS.getName(), value);
                break;

            default:
            }
        }
    }

    /**
     * Get a list of vertices matching a property key and a value.
     * <p/>
     * GET http://host/graphs/lineage/vertices?key=<key>&value=<value>
     * graph.getVertices(key, value);
     */
    @GET
    @Path("/vertices")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getVertices(@QueryParam("key") final String key,
                                @QueryParam("value") final String value) {
        checkIfMetadataMappingServiceIsEnabled();
        LOG.info("Get vertices for property key= {}, value= {}", key, value);
        try {
            JSONObject response = buildJSONResponse(getGraph().getVertices(key, value));
            return Response.ok(response).build();

        } catch (JSONException e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(JSONObject.quote("An error occurred: " + e.getMessage())).build());
        }
    }

    /**
     * Get a list of adjacent edges with a direction.
     *
     * GET http://host/graphs/lineage/vertices/id/direction
     * graph.getVertex(id).get{Direction}Edges();
     * direction: {(?!outE)(?!bothE)(?!inE)(?!out)(?!both)(?!in)(?!query).+}
     */
    @GET
    @Path("vertices/{id}/{direction}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getVertexEdges(@PathParam("id") String vertexId,
                                   @PathParam("direction") String direction) {
        checkIfMetadataMappingServiceIsEnabled();
        LOG.info("Get vertex edges for vertexId= {}, direction= {}", vertexId, direction);
        try {
            Vertex vertex = findVertex(vertexId);

            return getVertexEdges(vertex, direction);

        } catch (JSONException e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(JSONObject.quote("An error occurred: " + e.getMessage())).build());
        }
    }

    private Response getVertexEdges(Vertex vertex, String direction) throws JSONException {
        // break out the segment into the return and the direction
        VertexQueryArguments queryArguments = new VertexQueryArguments(direction);
        // if this is a query and the _return is "count" then we don't bother to send back the result array
        boolean countOnly = queryArguments.isCountOnly();
        // what kind of data the calling client wants back (vertices, edges, count, vertex identifiers)
        ReturnType returnType = queryArguments.getReturnType();
        // the query direction (both, out, in)
        Direction queryDirection = queryArguments.getQueryDirection();

        VertexQuery query = vertex.query().direction(queryDirection);

        JSONArray elementArray = new JSONArray();
        long counter = 0;
        if (returnType == ReturnType.VERTICES || returnType == ReturnType.VERTEX_IDS) {
            Iterable<Vertex> vertexQueryResults = query.vertices();
            for (Vertex v : vertexQueryResults) {
                if (returnType.equals(ReturnType.VERTICES)) {
                    elementArray.put(GraphSONUtility.jsonFromElement(
                            v, getVertexIndexedKeys(), GraphSONMode.NORMAL));
                } else {
                    elementArray.put(v.getId());
                }
                counter++;
            }
        } else if (returnType == ReturnType.EDGES) {
            Iterable<Edge> edgeQueryResults = query.edges();
            for (Edge e : edgeQueryResults) {
                elementArray.put(GraphSONUtility.jsonFromElement(
                        e, getEdgeIndexedKeys(), GraphSONMode.NORMAL));
                counter++;
            }
        } else if (returnType == ReturnType.COUNT) {
            counter = query.count();
        }

        JSONObject response = new JSONObject();
        if (!countOnly) {
            response.put(RESULTS, elementArray);
        }
        response.put(TOTAL_SIZE, counter);
        return Response.ok(response).build();
    }

    /**
     * Get all edges.
     *
     * GET http://host/graphs/lineage/edges/all
     * graph.getEdges();
     */
    @GET
    @Path("/edges/all")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getEdges() {
        checkIfMetadataMappingServiceIsEnabled();
        LOG.info("Get All Edges.");
        try {
            JSONObject response = buildJSONResponse(getGraph().getEdges());
            return Response.ok(response).build();

        } catch (JSONException e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(JSONObject.quote("An error occurred: " + e.getMessage())).build());
        }
    }

    /**
     * Get a single edge with a unique id.
     *
     * GET http://host/graphs/lineage/edges/id
     * graph.getEdge(id);
     */
    @GET
    @Path("/edges/{id}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getEdge(@PathParam("id") final String edgeId) {
        checkIfMetadataMappingServiceIsEnabled();
        LOG.info("Get vertex for edgeId= {}", edgeId);
        try {
            Edge edge = getGraph().getEdge(edgeId);
            if (edge == null) {
                String message = "Edge with [" + edgeId + "] cannot be found.";
                LOG.info(message);
                throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND)
                        .entity(JSONObject.quote(message)).build());
            }

            JSONObject response = new JSONObject();
            response.put(RESULTS, GraphSONUtility.jsonFromElement(
                    edge, getEdgeIndexedKeys(), GraphSONMode.NORMAL));
            return Response.ok(response).build();
        } catch (JSONException e) {
            throw new WebApplicationException(Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(JSONObject.quote("An error occurred: " + e.getMessage())).build());
        }
    }

    private <T extends Element> JSONObject buildJSONResponse(Iterable<T> elements) throws JSONException {
        JSONArray vertexArray = new JSONArray();
        long counter = 0;
        for (Element element : elements) {
            counter++;
            vertexArray.put(GraphSONUtility.jsonFromElement(
                    element, getVertexIndexedKeys(), GraphSONMode.NORMAL));
        }

        JSONObject response = new JSONObject();
        response.put(RESULTS, vertexArray);
        response.put(TOTAL_SIZE, counter);

        return response;
    }

    private void checkIfMetadataMappingServiceIsEnabled() {
        if (service == null) {
            throw new WebApplicationException(
                    Response.status(Response.Status.NOT_FOUND)
                            .entity("Lineage Metadata Service is not enabled.")
                            .type("text/plain")
                            .build());
        }
    }

    private enum ReturnType {VERTICES, EDGES, COUNT, VERTEX_IDS}

    public static final String OUT_E = "outE";
    public static final String IN_E = "inE";
    public static final String BOTH_E = "bothE";
    public static final String OUT = "out";
    public static final String IN = "in";
    public static final String BOTH = "both";
    public static final String OUT_COUNT = "outCount";
    public static final String IN_COUNT = "inCount";
    public static final String BOTH_COUNT = "bothCount";
    public static final String OUT_IDS = "outIds";
    public static final String IN_IDS = "inIds";
    public static final String BOTH_IDS = "bothIds";

    /**
     * Helper class for query arguments.
     */
    public static final class VertexQueryArguments {

        private final Direction queryDirection;
        private final ReturnType returnType;
        private final boolean countOnly;

        public VertexQueryArguments(String directionSegment) {
            if (directionSegment.equals(OUT_E)) {
                returnType = ReturnType.EDGES;
                queryDirection = Direction.OUT;
                countOnly = false;
            } else if (directionSegment.equals(IN_E)) {
                returnType = ReturnType.EDGES;
                queryDirection = Direction.IN;
                countOnly = false;
            } else if (directionSegment.equals(BOTH_E)) {
                returnType = ReturnType.EDGES;
                queryDirection = Direction.BOTH;
                countOnly = false;
            } else if (directionSegment.equals(OUT)) {
                returnType = ReturnType.VERTICES;
                queryDirection = Direction.OUT;
                countOnly = false;
            } else if (directionSegment.equals(IN)) {
                returnType = ReturnType.VERTICES;
                queryDirection = Direction.IN;
                countOnly = false;
            } else if (directionSegment.equals(BOTH)) {
                returnType = ReturnType.VERTICES;
                queryDirection = Direction.BOTH;
                countOnly = false;
            } else if (directionSegment.equals(BOTH_COUNT)) {
                returnType = ReturnType.COUNT;
                queryDirection = Direction.BOTH;
                countOnly = true;
            } else if (directionSegment.equals(IN_COUNT)) {
                returnType = ReturnType.COUNT;
                queryDirection = Direction.IN;
                countOnly = true;
            } else if (directionSegment.equals(OUT_COUNT)) {
                returnType = ReturnType.COUNT;
                queryDirection = Direction.OUT;
                countOnly = true;
            } else if (directionSegment.equals(BOTH_IDS)) {
                returnType = ReturnType.VERTEX_IDS;
                queryDirection = Direction.BOTH;
                countOnly = false;
            } else if (directionSegment.equals(IN_IDS)) {
                returnType = ReturnType.VERTEX_IDS;
                queryDirection = Direction.IN;
                countOnly = false;
            } else if (directionSegment.equals(OUT_IDS)) {
                returnType = ReturnType.VERTEX_IDS;
                queryDirection = Direction.OUT;
                countOnly = false;
            } else {
                throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST)
                        .entity(JSONObject.quote(directionSegment + " segment was invalid."))
                        .build());
            }
        }

        public Direction getQueryDirection() {
            return queryDirection;
        }

        public ReturnType getReturnType() {
            return returnType;
        }

        public boolean isCountOnly() {
            return countOnly;
        }
    }
}
