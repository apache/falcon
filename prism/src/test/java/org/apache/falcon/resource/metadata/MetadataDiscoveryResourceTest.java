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

import org.apache.falcon.FalconWebException;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.metadata.RelationshipType;
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
        Response response = resource.listDimensionValues(RelationshipType.FEED_ENTITY.toString(),
                MetadataTestContext.CLUSTER_ENTITY_NAME);
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(Integer.parseInt(results.get(MetadataDiscoveryResource.TOTAL_SIZE).toString()), 4);
        List dimensions = (List) results.get(MetadataDiscoveryResource.RESULTS);
        Assert.assertTrue(dimensions.contains("impression-feed"));

        response = resource.listDimensionValues(RelationshipType.FEED_ENTITY.toString(), null);
        results = (Map) JSONValue.parse(response.getEntity().toString());
        dimensions = (List) results.get(MetadataDiscoveryResource.RESULTS);
        Assert.assertEquals(Integer.parseInt(results.get(MetadataDiscoveryResource.TOTAL_SIZE).toString()), 4);
        Assert.assertTrue(dimensions.contains("impression-feed"));
    }

    @Test
    public void testListDimensionsProcess() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        Response response = resource.listDimensionValues(RelationshipType.PROCESS_ENTITY.toString(),
                MetadataTestContext.CLUSTER_ENTITY_NAME);
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(Integer.parseInt(results.get(MetadataDiscoveryResource.TOTAL_SIZE).toString()), 1);
        List dimensions = (List) results.get(MetadataDiscoveryResource.RESULTS);
        Assert.assertEquals(dimensions.get(0), MetadataTestContext.PROCESS_ENTITY_NAME);

        response = resource.listDimensionValues(RelationshipType.PROCESS_ENTITY.toString(), null);
        results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(Integer.parseInt(results.get(MetadataDiscoveryResource.TOTAL_SIZE).toString()), 1);
    }

    @Test
    public void testListDimensionsCluster() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        Response response = resource.listDimensionValues(RelationshipType.CLUSTER_ENTITY.toString(),
                MetadataTestContext.CLUSTER_ENTITY_NAME);
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(Integer.parseInt(results.get(MetadataDiscoveryResource.TOTAL_SIZE).toString()), 1);
        List dimensions = (List) results.get(MetadataDiscoveryResource.RESULTS);
        Assert.assertEquals(dimensions.get(0), MetadataTestContext.CLUSTER_ENTITY_NAME);

        response = resource.listDimensionValues(RelationshipType.CLUSTER_ENTITY.toString(), null);
        results = (Map) JSONValue.parse(response.getEntity().toString());
        dimensions = (List) results.get(MetadataDiscoveryResource.RESULTS);
        Assert.assertEquals(Integer.parseInt(results.get(MetadataDiscoveryResource.TOTAL_SIZE).toString()), 1);
        Assert.assertEquals(dimensions.get(0), MetadataTestContext.CLUSTER_ENTITY_NAME);
    }

    @Test
    public void testListDimensionsColo() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        Response response = resource.listDimensionValues(RelationshipType.COLO.toString(),
                MetadataTestContext.CLUSTER_ENTITY_NAME);
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(Integer.parseInt(results.get(MetadataDiscoveryResource.TOTAL_SIZE).toString()), 1);
        List dimensions = (List) results.get(MetadataDiscoveryResource.RESULTS);
        Assert.assertEquals(dimensions.get(0), MetadataTestContext.COLO_NAME);

        response = resource.listDimensionValues(RelationshipType.COLO.toString(), null);
        results = (Map) JSONValue.parse(response.getEntity().toString());
        dimensions = (List) results.get(MetadataDiscoveryResource.RESULTS);
        Assert.assertEquals(Integer.parseInt(results.get(MetadataDiscoveryResource.TOTAL_SIZE).toString()), 1);
        Assert.assertEquals(dimensions.get(0), MetadataTestContext.COLO_NAME);
    }

    @Test
    public void testListDimensionsPipelines() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        Response response = resource.listDimensionValues(RelationshipType.PIPELINES.toString(), null);
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        List dimensions = (List) results.get(MetadataDiscoveryResource.RESULTS);
        Assert.assertEquals(Integer.parseInt(results.get(MetadataDiscoveryResource.TOTAL_SIZE).toString()), 1);
        Assert.assertEquals(dimensions.get(0), "testPipeline");

        response = resource.listDimensionValues(RelationshipType.PIPELINES.toString(),
                MetadataTestContext.CLUSTER_ENTITY_NAME);
        results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(Integer.parseInt(results.get(MetadataDiscoveryResource.TOTAL_SIZE).toString()), 0);
    }

    @Test
    public void testListDimensionsTags() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        Response response = resource.listDimensionValues(RelationshipType.TAGS.toString(), null);
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(
                Integer.parseInt(results.get(MetadataDiscoveryResource.TOTAL_SIZE).toString()), 4);
        List dimensions = (List) results.get(MetadataDiscoveryResource.RESULTS);
        Assert.assertTrue(dimensions.contains("production"));
        response = resource.listDimensionValues(RelationshipType.TAGS.toString(),
                MetadataTestContext.CLUSTER_ENTITY_NAME);
        results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(Integer.parseInt(results.get(MetadataDiscoveryResource.TOTAL_SIZE).toString()), 0);
    }

    @Test
    public void testListDimensionsMetrics() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        Response response = resource.listReplicationMetricsDimensionValues("sample-process",
                EntityType.PROCESS.name(), 5);
        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        List metrics = (List) results.get(MetadataDiscoveryResource.RESULTS);
        Assert.assertEquals(metrics.size(), 1);
        Assert.assertTrue(metrics.get(0).toString().contains("BYTESCOPIED"));
        Assert.assertTrue(metrics.get(0).toString().contains("COPY"));
    }


    @Test(expectedExceptions = FalconWebException.class)
    public void testListInvalidDimensionType() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        resource.listDimensionValues("INVALID", null);
    }

    @Test(expectedExceptions = FalconWebException.class)
    public void testListFeedDimensionType() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        resource.listDimensionValues("Feed", null);
    }

    @Test(expectedExceptions = FalconWebException.class)
    public void testListInstanceDimensionType() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        resource.listDimensionValues("FEED_INSTANCE", null);
    }

    @Test
    public void testProcessGetDimensionRelations() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        Response response = resource.getDimensionRelations(RelationshipType.PROCESS_ENTITY.toString(),
                MetadataTestContext.PROCESS_ENTITY_NAME);
        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(results.get("type"), RelationshipType.PROCESS_ENTITY.toString());
        Assert.assertEquals(results.get("name"), MetadataTestContext.PROCESS_ENTITY_NAME);
        List inVertices = (List) results.get("inVertices");
        Assert.assertEquals(inVertices.size(), 3);
        Assert.assertNotNull(((Map) inVertices.get(0)).get("name"));
        List outVertices = (List) results.get("outVertices");
        Assert.assertEquals(outVertices.size(), 6);
        Assert.assertNotNull(((Map) outVertices.get(0)).get("name"));
    }

    @Test
    public void testFeedGetDimensionRelations() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        Response response = resource.getDimensionRelations(RelationshipType.FEED_ENTITY.toString(), "clicks-feed");
        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(results.get("type"), RelationshipType.FEED_ENTITY.toString());
        Assert.assertEquals(results.get("name"), "clicks-feed");
        List inVertices = (List) results.get("inVertices");
        Assert.assertEquals(inVertices.size(), 1);
        Assert.assertNotNull(((Map) inVertices.get(0)).get("name"));
        List outVertices = (List) results.get("outVertices");
        Assert.assertEquals(outVertices.size(), 3);
        Assert.assertNotNull(((Map) outVertices.get(0)).get("name"));
    }

    @Test
    public void testClusterGetDimensionRelations() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        Response response = resource.getDimensionRelations(RelationshipType.CLUSTER_ENTITY.toString(),
                "primary-cluster");
        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(results.get("type"), RelationshipType.CLUSTER_ENTITY.toString());
        Assert.assertEquals(results.get("name"), "primary-cluster");
        List inVertices = (List) results.get("inVertices");
        Assert.assertEquals(inVertices.size(), 10);
        Assert.assertNotNull(((Map) inVertices.get(0)).get("name"));
        List outVertices = (List) results.get("outVertices");
        Assert.assertEquals(outVertices.size(), 3);
        Assert.assertNotNull(((Map) outVertices.get(0)).get("name"));
    }

    @Test
    public void testTagsGetDimensionRelations() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        Response response = resource.getDimensionRelations(RelationshipType.TAGS.toString(), "Critical");
        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(results.get("type"), RelationshipType.TAGS.toString());
        Assert.assertEquals(results.get("name"), "Critical");
        List inVertices = (List) results.get("inVertices");
        Assert.assertEquals(inVertices.size(), 2);
        Assert.assertNotNull(((Map) inVertices.get(0)).get("name"));
        List outVertices = (List) results.get("outVertices");
        Assert.assertEquals(outVertices.size(), 0);
    }

    @Test
    public void testPipelinesGetDimensionRelations() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        Response response = resource.getDimensionRelations(RelationshipType.PIPELINES.toString(), "testPipeline");
        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(results.get("type"), RelationshipType.PIPELINES.toString());
        Assert.assertEquals(results.get("name"), "testPipeline");
        List inVertices = (List) results.get("inVertices");
        Assert.assertEquals(inVertices.size(), 2);
        Assert.assertNotNull(((Map) inVertices.get(0)).get("name"));
        List outVertices = (List) results.get("outVertices");
        Assert.assertEquals(outVertices.size(), 0);
    }

    @Test
    public void testUserGetDimensionRelations() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        Response response = resource.getDimensionRelations(RelationshipType.USER.toString(), "falcon-user");
        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(results.get("type"), RelationshipType.USER.toString());
        Assert.assertEquals(results.get("name"), "falcon-user");
        List inVertices = (List) results.get("inVertices");
        Assert.assertEquals(inVertices.size(), 11);
        Assert.assertNotNull(((Map) inVertices.get(0)).get("name"));
        List outVertices = (List) results.get("outVertices");
        Assert.assertEquals(outVertices.size(), 0);
    }

    @Test
    public void testColoGetDimensionRelations() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        Response response = resource.getDimensionRelations(RelationshipType.COLO.toString(), "west-coast");
        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(results.get("type"), RelationshipType.COLO.toString());
        Assert.assertEquals(results.get("name"), "west-coast");
        List inVertices = (List) results.get("inVertices");
        Assert.assertEquals(inVertices.size(), 1);
        Assert.assertNotNull(((Map) inVertices.get(0)).get("name"));
        List outVertices = (List) results.get("outVertices");
        Assert.assertEquals(outVertices.size(), 0);
    }

    @Test
    public void testGetNonExistingDimensionRelations() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        Response response = resource.getDimensionRelations(RelationshipType.GROUPS.toString(), "west-coast");
        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Assert.assertEquals(results.size(), 0);
    }


    @Test(expectedExceptions = FalconWebException.class)
    public void testEntityRelationsInvalidType() {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        resource.getDimensionRelations("INVALID", "name");
    }

    @Test(expectedExceptions = FalconWebException.class)
    public void testEntityRelationsFeedType() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        resource.getDimensionRelations("FEED", "name");
    }

    @Test(expectedExceptions = FalconWebException.class)
    public void testEntityRelationsInstanceType() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        resource.getDimensionRelations("FEED_INSTANCE", "name");
    }
    @Test(expectedExceptions = FalconWebException.class)
    public void testEntityRelationsNoName() throws Exception {
        MetadataDiscoveryResource resource = new MetadataDiscoveryResource();
        resource.getDimensionRelations(RelationshipType.TAGS.toString(), null);
    }
}
