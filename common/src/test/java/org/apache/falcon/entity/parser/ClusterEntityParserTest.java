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

package org.apache.falcon.entity.parser;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.falcon.FalconException;
import org.apache.falcon.catalog.CatalogServiceFactory;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.AbstractTestBase;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.util.StartupProperties;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Test for validating cluster entity parsing.
 */
public class ClusterEntityParserTest extends AbstractTestBase {

    private final ClusterEntityParser parser = (ClusterEntityParser) EntityParserFactory.getParser(EntityType.CLUSTER);

    @Test
    public void testParse() throws IOException, FalconException, JAXBException {

        InputStream stream = this.getClass().getResourceAsStream(CLUSTER_XML);

        Cluster cluster = parser.parse(stream);
        ClusterHelper.getInterface(cluster, Interfacetype.WRITE).setEndpoint(conf.get("fs.default.name"));

        Assert.assertNotNull(cluster);
        Assert.assertEquals(cluster.getName(), "testCluster");

        Interface execute = ClusterHelper.getInterface(cluster, Interfacetype.EXECUTE);

        Assert.assertEquals(execute.getEndpoint(), "localhost:8021");
        Assert.assertEquals(execute.getVersion(), "0.20.2");

        Interface readonly = ClusterHelper.getInterface(cluster, Interfacetype.READONLY);
        Assert.assertEquals(readonly.getEndpoint(), "hftp://localhost:50010");
        Assert.assertEquals(readonly.getVersion(), "0.20.2");

        Interface write = ClusterHelper.getInterface(cluster, Interfacetype.WRITE);
        //assertEquals(write.getEndpoint(), conf.get("fs.default.name"));
        Assert.assertEquals(write.getVersion(), "0.20.2");

        Interface workflow = ClusterHelper.getInterface(cluster, Interfacetype.WORKFLOW);
        Assert.assertEquals(workflow.getEndpoint(), "http://localhost:11000/oozie/");
        Assert.assertEquals(workflow.getVersion(), "3.1");

        Assert.assertEquals(ClusterHelper.getLocation(cluster, "staging"), "/projects/falcon/staging");

        StringWriter stringWriter = new StringWriter();
        Marshaller marshaller = EntityType.CLUSTER.getMarshaller();
        marshaller.marshal(cluster, stringWriter);
        System.out.println(stringWriter.toString());

        Interface catalog = ClusterHelper.getInterface(cluster, Interfacetype.REGISTRY);
        Assert.assertEquals(catalog.getEndpoint(), "http://localhost:48080/templeton/v1");
        Assert.assertEquals(catalog.getVersion(), "0.11.0");

        Assert.assertEquals(ClusterHelper.getLocation(cluster, "staging"), "/projects/falcon/staging");
    }

    @Test
    public void testParseClusterWithoutRegistry() throws IOException, FalconException, JAXBException {

        StartupProperties.get().setProperty(CatalogServiceFactory.CATALOG_SERVICE, "thrift://localhost:9083");
        Assert.assertTrue(CatalogServiceFactory.isEnabled());

        InputStream stream = this.getClass().getResourceAsStream("/config/cluster/cluster-no-registry.xml");
        Cluster cluster = parser.parse(stream);

        Interface catalog = ClusterHelper.getInterface(cluster, Interfacetype.REGISTRY);
        Assert.assertNull(catalog);

        StartupProperties.get().remove(CatalogServiceFactory.CATALOG_SERVICE);
        Assert.assertFalse(CatalogServiceFactory.isEnabled());

        catalog = ClusterHelper.getInterface(cluster, Interfacetype.REGISTRY);
        Assert.assertNull(catalog);
    }

    @Test
    public void testParseClusterWithBadRegistry() throws Exception {
        // disable catalog service
        StartupProperties.get().remove(CatalogServiceFactory.CATALOG_SERVICE);
        Assert.assertFalse(CatalogServiceFactory.isEnabled());

        InputStream stream = this.getClass().getResourceAsStream("/config/cluster/cluster-bad-registry.xml");
        Cluster cluster = parser.parse(stream);

        Interface catalog = ClusterHelper.getInterface(cluster, Interfacetype.REGISTRY);
        Assert.assertEquals(catalog.getEndpoint(), "Hcat");
        Assert.assertEquals(catalog.getVersion(), "0.1");
    }

    /**
     * A positive test for validating tags key value pair regex: key=value, key=value.
     * @throws FalconException
     */
    @Test
    public void testClusterTags() throws FalconException {
        InputStream stream = this.getClass().getResourceAsStream(CLUSTER_XML);
        Cluster cluster = parser.parse(stream);

        final String tags = cluster.getTags();
        Assert.assertEquals("consumer=consumer@xyz.com, owner=producer@xyz.com, department=forecasting", tags);

        final String[] keys = {"consumer", "owner", "department", };
        final String[] values = {"consumer@xyz.com", "producer@xyz.com", "forecasting", };

        final String[] pairs = tags.split(",");
        Assert.assertEquals(3, pairs.length);
        for (int i = 0; i < pairs.length; i++) {
            String pair = pairs[i].trim();
            String[] parts = pair.split("=");
            Assert.assertEquals(keys[i], parts[0]);
            Assert.assertEquals(values[i], parts[1]);
        }
    }

    @BeforeClass
    public void init() throws Exception {
        this.dfsCluster = EmbeddedCluster.newCluster("testCluster");
        this.conf = dfsCluster.getConf();
    }

    @AfterClass
    public void tearDown() {
        this.dfsCluster.shutdown();
    }
}
