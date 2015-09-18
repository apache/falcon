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

package org.apache.falcon.resource;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.resource.metadata.AbstractMetadataResource;
import org.apache.falcon.util.DeploymentUtil;
import org.json.simple.JSONValue;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

/**
 * Test class for Metadata REST APIs.
 *
 * Tests should be enabled only in local environments as they need running instance of the web server.
 */
public class MetadataResourceJerseyIT {

    private TestContext context;

    @BeforeClass
    public void prepare() throws Exception {
        context = newContext();
        ClientResponse response;
        Map<String, String> overlay = context.getUniqueOverlay();

        response = context.submitToFalcon(TestContext.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.FEED_TEMPLATE1, overlay, EntityType.FEED);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.FEED_TEMPLATE2, overlay, EntityType.FEED);
        context.assertSuccessful(response);

        response = context.submitToFalcon(TestContext.PROCESS_TEMPLATE, overlay, EntityType.PROCESS);
        context.assertSuccessful(response);
    }

    @AfterClass
    public void tearDown() throws Exception {
        TestContext.deleteEntitiesFromStore();
    }

    @Test
    public void testMetadataDiscoveryResourceList() throws Exception {

        ClientResponse response = context.service
                .path("api/metadata/discovery/cluster_entity/list")
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.APPLICATION_JSON)
                .get(ClientResponse.class);
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        Map results = (Map) JSONValue.parse(response.getEntity(String.class));
        List dimensions = (List) results.get(AbstractMetadataResource.RESULTS);
        Assert.assertTrue(dimensions.contains(context.clusterName));

        response = context.service
                .path("api/metadata/discovery/process_entity/list")
                .queryParam("cluster", context.clusterName)
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.APPLICATION_JSON)
                .get(ClientResponse.class);
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        results = (Map) JSONValue.parse(response.getEntity(String.class));
        dimensions = (List) results.get(AbstractMetadataResource.RESULTS);
        Assert.assertTrue(dimensions.contains(context.processName));

        response = context.service
                .path("api/metadata/discovery/process_entity/list")
                .queryParam("cluster", "random")
                .queryParam("doAs", "testUser")
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.APPLICATION_JSON)
                .get(ClientResponse.class);
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        results = (Map) JSONValue.parse(response.getEntity(String.class));
        Assert.assertEquals(Integer.parseInt(results.get(AbstractMetadataResource.TOTAL_SIZE).toString()), 0);
    }

    @Test
    public void testMetadataDiscoveryResourceRelations() throws Exception {
        ClientResponse response = context.service
                .path("api/metadata/discovery/process_entity/" + context.processName + "/relations")
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.APPLICATION_JSON)
                .get(ClientResponse.class);
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        Map results = (Map) JSONValue.parse(response.getEntity(String.class));
        Assert.assertEquals(results.get("name"), context.processName);

        response = context.service
                .path("api/metadata/discovery/colo/" + DeploymentUtil.getCurrentColo() + "/relations")
                .header("Cookie", context.getAuthenticationToken())
                .accept(MediaType.APPLICATION_JSON)
                .get(ClientResponse.class);
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        results = (Map) JSONValue.parse(response.getEntity(String.class));
        Assert.assertEquals(results.get("name"), DeploymentUtil.getCurrentColo());
        List inVertices = (List) results.get("inVertices");
        Assert.assertTrue(inVertices.size() >= 1);
    }

    private ThreadLocal<TestContext> contexts = new ThreadLocal<TestContext>();

    private TestContext newContext() throws Exception {
        TestContext.prepare(TestContext.CLUSTER_TEMPLATE, false);
        contexts.set(new TestContext());
        return contexts.get();
    }
}
