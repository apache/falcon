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

import org.json.simple.JSONValue;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for org.apache.falcon.resource.metadata.MetadataMappingResource.
 */
public class MetadataDiscoveryResourceTest {

    private MetadataTestContext testContext;

    @BeforeClass
    public void setUp() throws Exception {
        testContext = new MetadataTestContext();
        testContext.setUp();
    }

    @AfterClass
    public void tearDown() throws Exception {
        testContext.tearDown();
    }

    @Test
    public void testListDimensionsFeed() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        Response response = resource.listDimensionValues("feed_entity", MetadataTestContext.CLUSTER_ENTITY_NAME);
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(Integer.parseInt(results.get(MetadataDiscoveryResource.TOTAL_SIZE).toString()), 4);
        List dimensions = (List) results.get(MetadataDiscoveryResource.RESULTS);
        Assert.assertTrue(dimensions.contains("impression-feed"));

        response = resource.listDimensionValues("feed_entity", null);
        results = (Map) JSONValue.parse(response.getEntity().toString());
        dimensions = (List) results.get(MetadataDiscoveryResource.RESULTS);
        Assert.assertEquals(Integer.parseInt(results.get(MetadataDiscoveryResource.TOTAL_SIZE).toString()), 4);
        Assert.assertTrue(dimensions.contains("impression-feed"));
    }

    @Test
    public void testListDimensionsProcess() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        Response response = resource.listDimensionValues("process_entity",
                MetadataTestContext.CLUSTER_ENTITY_NAME);
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(Integer.parseInt(results.get(MetadataDiscoveryResource.TOTAL_SIZE).toString()), 1);
        List dimensions = (List) results.get(MetadataDiscoveryResource.RESULTS);
        Assert.assertEquals(dimensions.get(0), MetadataTestContext.PROCESS_ENTITY_NAME);

        response = resource.listDimensionValues("process_entity", null);
        results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(Integer.parseInt(results.get(MetadataDiscoveryResource.TOTAL_SIZE).toString()), 1);
    }

    @Test
    public void testListDimensionsCluster() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        Response response = resource.listDimensionValues("cluster_entity", MetadataTestContext.CLUSTER_ENTITY_NAME);
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(Integer.parseInt(results.get(MetadataDiscoveryResource.TOTAL_SIZE).toString()), 1);
        List dimensions = (List) results.get(MetadataDiscoveryResource.RESULTS);
        Assert.assertEquals(dimensions.get(0), MetadataTestContext.CLUSTER_ENTITY_NAME);

        response = resource.listDimensionValues("cluster_entity", null);
        results = (Map) JSONValue.parse(response.getEntity().toString());
        dimensions = (List) results.get(MetadataDiscoveryResource.RESULTS);
        Assert.assertEquals(Integer.parseInt(results.get(MetadataDiscoveryResource.TOTAL_SIZE).toString()), 1);
        Assert.assertEquals(dimensions.get(0), MetadataTestContext.CLUSTER_ENTITY_NAME);
    }

    @Test
    public void testListDimensionsColo() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        Response response = resource.listDimensionValues("colo", MetadataTestContext.CLUSTER_ENTITY_NAME);
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(Integer.parseInt(results.get(MetadataDiscoveryResource.TOTAL_SIZE).toString()), 1);
        List dimensions = (List) results.get(MetadataDiscoveryResource.RESULTS);
        Assert.assertEquals(dimensions.get(0), MetadataTestContext.COLO_NAME);

        response = resource.listDimensionValues("colo", null);
        results = (Map) JSONValue.parse(response.getEntity().toString());
        dimensions = (List) results.get(MetadataDiscoveryResource.RESULTS);
        Assert.assertEquals(Integer.parseInt(results.get(MetadataDiscoveryResource.TOTAL_SIZE).toString()), 1);
        Assert.assertEquals(dimensions.get(0), MetadataTestContext.COLO_NAME);

        try {
            resource.listDimensionValues("INVALID", null);
            Assert.assertTrue(false);
        } catch (Exception e) {
            Assert.assertTrue(true);
        }
    }

    @Test
    public void testListDimensionsPipelines() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        Response response = resource.listDimensionValues("pipelines", null);
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        List dimensions = (List) results.get(MetadataDiscoveryResource.RESULTS);
        Assert.assertEquals(Integer.parseInt(results.get(MetadataDiscoveryResource.TOTAL_SIZE).toString()), 1);
        Assert.assertEquals(dimensions.get(0), "testPipeline");

        response = resource.listDimensionValues("pipelines", MetadataTestContext.CLUSTER_ENTITY_NAME);
        results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(Integer.parseInt(results.get(MetadataDiscoveryResource.TOTAL_SIZE).toString()), 0);
    }

    @Test
    public void testListDimensionsTags() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        Response response = resource.listDimensionValues("tags", null);
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(Integer.parseInt(results.get(MetadataDiscoveryResource.TOTAL_SIZE).toString()), 4);
        List dimensions = (List) results.get(MetadataDiscoveryResource.RESULTS);
        Assert.assertTrue(dimensions.contains("production"));

        response = resource.listDimensionValues("tags", MetadataTestContext.CLUSTER_ENTITY_NAME);
        results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(Integer.parseInt(results.get(MetadataDiscoveryResource.TOTAL_SIZE).toString()), 0);
    }
}
