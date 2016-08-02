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
import org.apache.falcon.util.OozieTestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;
import java.util.Map;

/**
 * Test class for Entity LIST pagination REST APIs.
 */
public class EntityManagerPaginationJerseyIT {

    private TestContext context;

    @BeforeClass
    public void prepare() throws Exception {
        TestContext.prepare();
        context = new TestContext();
        scheduleMultipleProcess(5);
    }

    @AfterClass
    public void tearDown() throws Exception {
        TestContext.deleteEntitiesFromStore();
    }

    @AfterMethod
    public void cleanup() throws Exception {
        OozieTestUtils.killOozieJobs(context);
    }

    @Test
    public void testGetEntityList() throws Exception {
        ClientResponse response = context.service
                .path("api/entities/list/process/")
                .header("Cookie", context.getAuthenticationToken())
                .type(MediaType.TEXT_XML)
                .accept(MediaType.TEXT_XML)
                .get(ClientResponse.class);
        Assert.assertEquals(response.getStatus(), 200);
        EntityList result = response.getEntity(EntityList.class);
        Assert.assertNotNull(result);
        for (EntityList.EntityElement entityElement : result.getElements()) {
            Assert.assertNull(entityElement.status); // status is null
            Assert.assertNotNull(entityElement.name);
        }
    }

    @Test
    public void testPagination() {
        ClientResponse response = context.service
                .path("api/entities/list/process/")
                .queryParam("filterBy", "TYPE:PROCESS")
                .queryParam("tags", "owner=producer@xyz.com, department=forecasting")
                .queryParam("orderBy", "name").queryParam("sortOrder", "desc").queryParam("offset", "2")
                .queryParam("numResults", "2").queryParam("fields", "status,tags")
                .header("Cookie", context.getAuthenticationToken())
                .type(MediaType.TEXT_XML)
                .accept(MediaType.TEXT_XML)
                .get(ClientResponse.class);
        Assert.assertEquals(response.getStatus(), 200);
        EntityList result = response.getEntity(EntityList.class);
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getElements().length, 2);
    }

    @Test
    public void testFilterTags() {
        ClientResponse response = context.service
                .path("api/entities/list/process/")
                .queryParam("tags", "owner=producer@xyz.com, department=forecasting")
                .queryParam("orderBy", "name").queryParam("sortOrder", "desc").queryParam("offset", "2")
                .queryParam("numResults", "2").queryParam("fields", "status,tags")
                .header("Cookie", context.getAuthenticationToken())
                .type(MediaType.TEXT_XML)
                .accept(MediaType.TEXT_XML)
                .get(ClientResponse.class);
        Assert.assertEquals(response.getStatus(), 200);
        EntityList result = response.getEntity(EntityList.class);
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getElements().length, 2);

        response = context.service
                .path("api/entities/list/feed/")
                .queryParam("tags", "owner=producer@xyz.com, department=forecasting")
                .queryParam("fields", "status,tags")
                .header("Cookie", context.getAuthenticationToken())
                .type(MediaType.TEXT_XML)
                .accept(MediaType.TEXT_XML)
                .get(ClientResponse.class);
        Assert.assertEquals(response.getStatus(), 200);
        result = response.getEntity(EntityList.class);
        Assert.assertNotNull(result);
        Assert.assertNull(result.getElements());

    }

    @Test
    public void testPaginationHugeOffset() throws Exception {
        ClientResponse response = context.service
                .path("api/entities/list/process/")
                .queryParam("orderBy", "name").queryParam("sortOrder", "asc")
                .queryParam("offset", "50").queryParam("numResults", "2")
                .header("Cookie", context.getAuthenticationToken())
                .type(MediaType.TEXT_XML)
                .accept(MediaType.TEXT_XML)
                .get(ClientResponse.class);
        Assert.assertEquals(response.getStatus(), 200);
        EntityList result = response.getEntity(EntityList.class);
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getElements(), null);
    }

    @Test
    public void testPaginationEntitySummary() throws Exception {
        Map<String, String> overlay = context.getUniqueOverlay();
        context.scheduleProcess(TestContext.PROCESS_TEMPLATE, overlay);
        OozieTestUtils.waitForProcessWFtoStart(context);

        ClientResponse response = context.service
                .path("api/entities/summary/process")
                .queryParam("cluster", overlay.get("cluster"))
                .queryParam("fields", "status,pipelines")
                .queryParam("numInstances", "1")
                .queryParam("orderBy", "name")
                .header("Cookie", context.getAuthenticationToken())
                .type(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .get(ClientResponse.class);
        Assert.assertEquals(response.getStatus(), 200);
        EntitySummaryResult summaryResult = response.getEntity(EntitySummaryResult.class);
        Assert.assertNotNull(summaryResult);
    }

    private void scheduleMultipleProcess(int count) throws Exception {
        for (int i=0; i<count; i++) {
            context.scheduleProcess();
            Thread.sleep(100);
        }
        OozieTestUtils.waitForProcessWFtoStart(context);
    }
}
