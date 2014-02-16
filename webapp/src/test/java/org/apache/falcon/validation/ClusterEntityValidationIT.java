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

package org.apache.falcon.validation;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;

import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.resource.TestContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.sun.jersey.api.client.ClientResponse;

/**
 * Tests cluster entity validation to verify if each of the specified
 * interface endpoints are valid.
 */
public class ClusterEntityValidationIT {
    private final TestContext context = new TestContext();
    private Map<String, String> overlay;


    @BeforeClass
    public void setup() throws Exception {
        TestContext.prepare();
    }

    /**
     * Positive test.
     *
     * @throws Exception
     */
    @Test
    public void testClusterEntityWithValidInterfaces() throws Exception {
        overlay = context.getUniqueOverlay();
        overlay.put("colo", "default");
        ClientResponse response = context.submitToFalcon(TestContext.CLUSTER_TEMPLATE, overlay, EntityType.CLUSTER);
        context.assertSuccessful(response);
    }


    @DataProvider(name = "interfaceToInvalidURLs")
    public Object[][] createInterfaceToInvalidURLData() {
        return new Object[][] {
            // TODO FileSystem validates invalid hftp url, does NOT fail
            // {Interfacetype.READONLY, "hftp://localhost:41119"},
            {Interfacetype.READONLY, ""},
            {Interfacetype.READONLY, "localhost:41119"},
            {Interfacetype.WRITE, "write-interface:9999"},
            {Interfacetype.WRITE, "hdfs://write-interface:9999"},
            {Interfacetype.EXECUTE, "execute-interface:9999"},
            {Interfacetype.WORKFLOW, "workflow-interface:9999/oozie/"},
            {Interfacetype.WORKFLOW, "http://workflow-interface:9999/oozie/"},
            {Interfacetype.MESSAGING, "messaging-interface:9999"},
            {Interfacetype.MESSAGING, "tcp://messaging-interface:9999"},
            {Interfacetype.REGISTRY, "catalog-interface:9999"},
            {Interfacetype.REGISTRY, "http://catalog-interface:9999"},
        };
    }

    @Test (dataProvider = "interfaceToInvalidURLs")
    public void testClusterEntityWithInvalidInterfaces(Interfacetype interfacetype, String endpoint)
        throws Exception {
        overlay = context.getUniqueOverlay();
        String filePath = TestContext.overlayParametersOverTemplate(TestContext.CLUSTER_TEMPLATE, overlay);
        InputStream stream = new FileInputStream(filePath);
        Cluster cluster = (Cluster) EntityType.CLUSTER.getUnmarshaller().unmarshal(stream);
        Assert.assertNotNull(cluster);
        cluster.setColo("default");  // validations will be ignored if not default & tests fail

        Interface anInterface = ClusterHelper.getInterface(cluster, interfacetype);
        anInterface.setEndpoint(endpoint);

        File tmpFile = TestContext.getTempFile();
        EntityType.CLUSTER.getMarshaller().marshal(cluster, tmpFile);
        ClientResponse response = context.submitFileToFalcon(EntityType.CLUSTER, tmpFile.getAbsolutePath());
        context.assertFailure(response);
    }
}
