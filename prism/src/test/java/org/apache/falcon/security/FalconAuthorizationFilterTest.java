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

package org.apache.falcon.security;

import org.apache.falcon.cluster.util.EntityBuilderTestUtil;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.security.UserGroupInformation;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Test for FalconAuthorizationFilter using mock objects.
 */
public class FalconAuthorizationFilterTest {

    public static final String CLUSTER_ENTITY_NAME = "primary-cluster";
    public static final String PROCESS_ENTITY_NAME = "sample-process";

    @Mock
    private HttpServletRequest mockRequest;

    @Mock
    private HttpServletResponse mockResponse;

    @Mock
    private FilterChain mockChain;

    @Mock
    private FilterConfig mockConfig;

    @Mock
    private UserGroupInformation mockUgi;

    private ConfigurationStore configStore;
    private Cluster clusterEntity;
    private org.apache.falcon.entity.v0.process.Process processEntity;

    @BeforeClass
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        CurrentUser.authenticate(EntityBuilderTestUtil.USER);
        Assert.assertEquals(CurrentUser.getUser(), EntityBuilderTestUtil.USER);

        configStore = ConfigurationStore.get();

        addClusterEntity();
        addProcessEntity();
        Assert.assertNotNull(processEntity);
    }

    @DataProvider(name = "resourceWithNoEntity")
    private Object[][] createOptions() {
        return new Object[][] {
            {"/admin/version"},
            {"/entities/list/feed"},
            {"/entities/list/process"},
            {"/entities/list/cluster"},
            {"/metadata/lineage/vertices/all"},
            {"/metadata/lineage/vertices/_1"},
            {"/metadata/lineage/vertices/properties/_1"},
            {"/metadata/discovery/process_entity/sample-process/relations"},
            {"/metadata/discovery/process_entity/list?cluster=primary-cluster"},
        };
    }

    @Test (dataProvider = "resourceWithNoEntity")
    public void testDoFilter(String resource) throws Exception {
        boolean[] enabledFlags = {false, true};
        for (boolean enabled : enabledFlags) {
            StartupProperties.get().setProperty(
                "falcon.security.authorization.enabled", String.valueOf(enabled));

            Filter filter = new FalconAuthorizationFilter();
            synchronized (StartupProperties.get()) {
                filter.init(mockConfig);
            }

            StringBuffer requestUrl = new StringBuffer("http://localhost" + resource);
            Mockito.when(mockRequest.getRequestURL()).thenReturn(requestUrl);
            Mockito.when(mockRequest.getRequestURI()).thenReturn("/api" + resource);
            Mockito.when(mockRequest.getPathInfo()).thenReturn(resource);

            try {
                filter.doFilter(mockRequest, mockResponse, mockChain);
            } finally {
                filter.destroy();
            }
        }
    }

    @DataProvider(name = "invalidResource")
    private Object[][] createBadOptions() {
        return new Object[][] {
            {"/admin1/version"},
            {"/entities1/list/feed"},
            {"/metadata1/lineage/vertices/all"},
            {"/foo/bar"},
        };
    }

    @Test (dataProvider = "invalidResource")
    public void testDoFilterBadResource(String resource) throws Exception {
        boolean[] enabledFlags = {false, true};
        for (boolean enabled : enabledFlags) {
            StartupProperties.get().setProperty(
                "falcon.security.authorization.enabled", String.valueOf(enabled));

            Filter filter = new FalconAuthorizationFilter();
            synchronized (StartupProperties.get()) {
                filter.init(mockConfig);
            }

            StringBuffer requestUrl = new StringBuffer("http://localhost" + resource);
            Mockito.when(mockRequest.getRequestURL()).thenReturn(requestUrl);
            Mockito.when(mockRequest.getRequestURI()).thenReturn("/api" + resource);
            Mockito.when(mockRequest.getPathInfo()).thenReturn(resource);

            Mockito.when(mockResponse.getOutputStream()).thenReturn(
                new ServletOutputStream() {
                    @Override
                    public void write(int b) throws IOException {
                        System.out.print(b);
                    }
                });

            try {
                filter.doFilter(mockRequest, mockResponse, mockChain);

                // todo: verify the response error code to 400
            } finally {
                filter.destroy();
            }
        }
    }

    @DataProvider(name = "resourceWithEntity")
    private Object[][] createOptionsForResourceWithEntity() {
        return new Object[][] {
            {"/entities/status/process/"},
            {"/entities/suspend/process/"},
            {"/instance/running/process/"},
        };
    }

    @Test (dataProvider = "resourceWithEntity")
    public void testDoFilterForEntity(String resource) throws Exception {
        boolean[] enabledFlags = {false, true};
        for (boolean enabled : enabledFlags) {
            StartupProperties.get().setProperty(
                "falcon.security.authorization.enabled", String.valueOf(enabled));

            Filter filter = new FalconAuthorizationFilter();
            synchronized (StartupProperties.get()) {
                filter.init(mockConfig);
            }

            String uri = resource + processEntity.getName();
            StringBuffer requestUrl = new StringBuffer("http://localhost" + uri);
            Mockito.when(mockRequest.getRequestURL()).thenReturn(requestUrl);
            Mockito.when(mockRequest.getRequestURI()).thenReturn("/api" + uri);
            Mockito.when(mockRequest.getPathInfo()).thenReturn(uri);

            try {
                filter.doFilter(mockRequest, mockResponse, mockChain);
            } finally {
                filter.destroy();
            }
        }
    }

    @Test
    public void testDoFilterForEntityWithInvalidEntity() throws Exception {
        CurrentUser.authenticate("falcon");

        StartupProperties.get().setProperty("falcon.security.authorization.enabled", "true");

        Filter filter = new FalconAuthorizationFilter();
        synchronized (StartupProperties.get()) {
            filter.init(mockConfig);
        }

        String uri = "/entities/suspend/process/bad-entity";
        StringBuffer requestUrl = new StringBuffer("http://localhost" + uri);
        Mockito.when(mockRequest.getRequestURL()).thenReturn(requestUrl);
        Mockito.when(mockRequest.getRequestURI()).thenReturn("/api" + uri);
        Mockito.when(mockRequest.getPathInfo()).thenReturn(uri);

        try {
            filter.doFilter(mockRequest, mockResponse, mockChain);

            // todo: verify the response error code to 403
        } finally {
            filter.destroy();
        }
    }

    public void addClusterEntity() throws Exception {
        clusterEntity = EntityBuilderTestUtil.buildCluster(CLUSTER_ENTITY_NAME);
        configStore.publish(EntityType.CLUSTER, clusterEntity);
    }

    public void addProcessEntity() throws Exception {
        processEntity = EntityBuilderTestUtil.buildProcess(PROCESS_ENTITY_NAME,
                clusterEntity, "classified-as=Critical");
        EntityBuilderTestUtil.addProcessWorkflow(processEntity);
        EntityBuilderTestUtil.addProcessACL(processEntity);

        configStore.publish(EntityType.PROCESS, processEntity);
    }
}
