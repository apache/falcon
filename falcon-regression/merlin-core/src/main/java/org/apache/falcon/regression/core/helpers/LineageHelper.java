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

package org.apache.falcon.regression.core.helpers;

import com.google.gson.GsonBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.falcon.regression.core.response.lineage.Direction;
import org.apache.falcon.regression.core.response.lineage.EdgeResult;
import org.apache.falcon.regression.core.response.lineage.EdgesResult;
import org.apache.falcon.regression.core.response.lineage.Vertex;
import org.apache.falcon.regression.core.response.lineage.VertexIdsResult;
import org.apache.falcon.regression.core.response.lineage.VertexResult;
import org.apache.falcon.regression.core.response.lineage.VerticesResult;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.GraphAssert;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.request.BaseRequest;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.http.HttpResponse;
import org.apache.log4j.Logger;
import org.testng.Assert;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.Map;
import java.util.TreeMap;

/**
 *  Class with helper functions to test lineage feature.
 */
public class LineageHelper {
    private static final Logger LOGGER = Logger.getLogger(LineageHelper.class);
    private final String hostname;

    /**
     * Lineage related REST endpoints.
     */
    public enum URL {
        SERIALIZE("/api/graphs/lineage/serialize"),
        VERTICES("/api/graphs/lineage/vertices"),
        VERTICES_ALL("/api/graphs/lineage/vertices/all"),
        VERTICES_PROPERTIES("/api/graphs/lineage/vertices/properties"),
        EDGES("/api/graphs/lineage/edges"),
        EDGES_ALL("/api/graphs/lineage/edges/all");

        private final String url;

        URL(String url) {
            this.url = url;
        }

        public String getValue() {
            return this.url;
        }
    }

    /**
     * Create a LineageHelper to use with a specified hostname.
     * @param hostname hostname
     */
    public LineageHelper(String hostname) {
        this.hostname = hostname.trim().replaceAll("/$", "");
    }

    /**
     * Create a LineageHelper to use with a specified prismHelper.
     * @param prismHelper prismHelper
     */
    public LineageHelper(ColoHelper prismHelper) {
        this(prismHelper.getClusterHelper().getHostname());
    }

    /**
     * Extract response string from the response object.
     * @param response the response object
     * @return the response string
     * @throws IOException
     */
    public String getResponseString(HttpResponse response) throws IOException {
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(response.getEntity().getContent(), "UTF-8"));
        StringBuilder sb = new StringBuilder();
        String line;
        while((line = reader.readLine()) != null){
            sb.append(line).append("\n");
        }
        return sb.toString();
    }

    /**
     * Run a get request on the specified url.
     * @param url url
     * @return response of the request
     * @throws URISyntaxException
     * @throws IOException
     * @throws AuthenticationException
     */
    public HttpResponse runGetRequest(String url)
        throws URISyntaxException, IOException, AuthenticationException {
        final BaseRequest request = new BaseRequest(url, "get", null);
        return request.run();
    }

    /**
     * Successfully run a get request on the specified url.
     * @param url url
     * @return string response of the request
     * @throws URISyntaxException
     * @throws IOException
     * @throws AuthenticationException
     */
    public String runGetRequestSuccessfully(String url)
        throws URISyntaxException, IOException, AuthenticationException {
        HttpResponse response = runGetRequest(url);
        String responseString = getResponseString(response);
        LOGGER.info(Util.prettyPrintXmlOrJson(responseString));
        Assert.assertEquals(response.getStatusLine().getStatusCode(), 200,
                "The get request  was expected to be successfully");
        return responseString;
    }

    /**
     * Create a full url for the given lineage endpoint, urlPath and parameter.
     * @param url        lineage endpoint
     * @param urlPath    url path to be added to lineage endpoint
     * @param paramPairs parameters to be passed
     * @return url string
     */
    public String getUrl(final URL url, final String urlPath, final Map<String,
            String> paramPairs) {
        Assert.assertNotNull(hostname, "Hostname can't be null.");
        String hostAndPath = hostname + url.getValue();
        if (urlPath != null) {
            hostAndPath += "/" + urlPath;
        }
        if (paramPairs != null && paramPairs.size() > 0) {
            String[] params = new String[paramPairs.size()];
            int i = 0;
            for (String key : paramPairs.keySet()) {
                params[i++] = key + '=' + paramPairs.get(key);
            }
            return hostAndPath + "/?" + StringUtils.join(params, "&");
        }
        return hostAndPath;
    }

    /**
     * Create a full url for the given lineage endpoint, urlPath and parameter.
     * @param url     lineage endpoint
     * @param urlPath url path to be added to lineage endpoint
     * @return url string
     */
    public String getUrl(final URL url, final String urlPath) {
        return getUrl(url, urlPath, null);
    }

    /**
     * Create a full url for the given lineage endpoint and parameter.
     * @param url        lineage endpoint
     * @param paramPairs parameters to be passed
     * @return url string
     */
    public String getUrl(final URL url, final Map<String, String> paramPairs) {
        return getUrl(url, null, paramPairs);
    }

    /**
     * Create a full url for the given lineage endpoint and parameter.
     * @param url lineage endpoint
     * @return url string
     */
    public String getUrl(final URL url) {
        return getUrl(url, null, null);
    }

    /**
     * Create url path from parts.
     * @param pathParts parts of the path
     * @return url path
     */
    public String getUrlPath(String... pathParts) {
        return StringUtils.join(pathParts, "/");
    }

    /**
     * Create url path from parts.
     * @param oneInt    part of the path
     * @param pathParts parts of the path
     * @return url path
     */
    public String getUrlPath(int oneInt, String... pathParts) {
        return oneInt + "/" + getUrlPath(pathParts);
    }

    /**
     * Get result of the supplied type for the given url.
     * @param url url
     * @return result of the REST request
     */
    public <T> T getResultOfType(String url, Class<T> clazz) {
        String responseString = null;
        try {
            responseString = runGetRequestSuccessfully(url);
        } catch (URISyntaxException e) {
            AssertUtil.fail(e);
        } catch (IOException e) {
            AssertUtil.fail(e);
        } catch (AuthenticationException e) {
            AssertUtil.fail(e);
        }
        return new GsonBuilder().create().fromJson(responseString, clazz);
    }

    /**
     * Get vertices result for the url.
     * @param url url
     * @return result of the REST request
     */
    public VerticesResult getVerticesResult(String url) {
        return getResultOfType(url, VerticesResult.class);
    }

    /**
     * Get vertex result for the url.
     * @param url url
     * @return result of the REST request
     */
    private VertexResult getVertexResult(String url) {
        return getResultOfType(url, VertexResult.class);
    }

    /**
     * Get vertex id result for the url.
     * @param url url
     * @return result of the REST request
     */
    private VertexIdsResult getVertexIdsResult(String url) {
        return getResultOfType(url, VertexIdsResult.class);
    }

    /**
     * Get all the vertices.
     * @return all the vertices
     */
    public VerticesResult getAllVertices() {
        return getVerticesResult(getUrl(URL.VERTICES_ALL));
    }

    public VerticesResult getVertices(Vertex.FilterKey key, String value) {
        Map<String, String> params = new TreeMap<String, String>();
        params.put("key", key.toString());
        params.put("value", value);
        return getVerticesResult(getUrl(URL.VERTICES, params));
    }

    public VertexResult getVertexById(int vertexId) {
        return getVertexResult(getUrl(URL.VERTICES, getUrlPath(vertexId)));
    }

    public VertexResult getVertexProperties(int vertexId) {
        return getVertexResult(getUrl(URL.VERTICES_PROPERTIES, getUrlPath(vertexId)));
    }

    public VerticesResult getVerticesByType(Vertex.VERTEX_TYPE vertexType) {
        return getVertices(Vertex.FilterKey.type, vertexType.getValue());
    }

    public VerticesResult getVerticesByName(String name) {
        return getVertices(Vertex.FilterKey.name, name);
    }

    public VerticesResult getVerticesByDirection(int vertexId, Direction direction) {
        Assert.assertTrue((EnumSet.of(Direction.bothCount, Direction.inCount, Direction.outCount,
                Direction.bothVertices, Direction.inComingVertices,
                Direction.outgoingVertices).contains(direction)),
                "Vertices requested.");
        return getVerticesResult(getUrl(URL.VERTICES, getUrlPath(vertexId, direction.getValue())));
    }

    public VertexIdsResult getVertexIdsByDirection(int vertexId, Direction direction) {
        Assert.assertTrue((EnumSet.of(Direction.bothVerticesIds, Direction.incomingVerticesIds,
                Direction.outgoingVerticesIds).contains(direction)),
                "Vertex Ids requested.");
        return getVertexIdsResult(getUrl(URL.VERTICES, getUrlPath(vertexId, direction.getValue())));
    }

    public Vertex getVertex(String vertexName) {
        final VerticesResult clusterResult = getVerticesByName(vertexName);
        GraphAssert.assertVertexSanity(clusterResult);
        Assert.assertEquals(clusterResult.getTotalSize(), 1,
                "Expected one node for vertex name:" + vertexName);
        return clusterResult.getResults().get(0);
    }

    /**
     * Get edges result for the url.
     * @param url url
     * @return result of the REST request
     */
    private EdgesResult getEdgesResult(String url) {
        return getResultOfType(url, EdgesResult.class);
    }

    private EdgeResult getEdgeResult(String url) {
        return getResultOfType(url, EdgeResult.class);
    }

    public EdgesResult getEdgesByDirection(int vertexId, Direction direction) {
        Assert.assertTrue((EnumSet.of(Direction.bothEdges, Direction.inComingEdges,
            Direction.outGoingEdges).contains(direction)), "Vertices requested.");
        return getEdgesResult(getUrl(URL.VERTICES, getUrlPath(vertexId, direction.getValue())));
    }

    public EdgesResult getAllEdges() {
        return getEdgesResult(getUrl(URL.EDGES_ALL));
    }

    public EdgeResult getEdgeById(String edgeId) {
        return getEdgeResult(getUrl(URL.EDGES, getUrlPath(edgeId)));
    }
}
