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

import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.AbstractTestBase;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.datasource.Datasource;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.InputStream;

/**
 * Test class for Datasource Entity.
 */
public class DatasourceEntityParserTest extends AbstractTestBase {

    private EmbeddedCluster cluster;
    private String hdfsUrl;

    private final DatasourceEntityParser datasourceEntityParser =
            (DatasourceEntityParser) EntityParserFactory.getParser(EntityType.DATASOURCE);
    private final FeedEntityParser feedEntityParser =
            (FeedEntityParser) EntityParserFactory.getParser(EntityType.FEED);

    @BeforeClass
    public void start() throws Exception {
        cluster = EmbeddedCluster.newCluster("test");
        hdfsUrl = cluster.getConf().get(HadoopClientFactory.FS_DEFAULT_NAME_KEY);
    }

    @AfterClass
    public void close() throws Exception {
        cluster.shutdown();
    }

    @BeforeMethod
    public void setup() throws Exception {
        cleanupStore();
    }

    @Test
    public void testDatasourceEntity() throws Exception {

        InputStream stream = this.getClass().getResourceAsStream("/config/datasource/datasource-0.1.xml");
        Datasource datasource = datasourceEntityParser.parse(stream);

        ConfigurationStore store = ConfigurationStore.get();
        store.publish(EntityType.DATASOURCE, datasource);

        Datasource databaseEntity = EntityUtil.getEntity(EntityType.DATASOURCE, datasource.getName());
        Assert.assertEquals("test-hsql-db", datasource.getName());
        Assert.assertEquals("test-hsql-db", databaseEntity.getName());
        Assert.assertEquals("hsql", databaseEntity.getType().value());
        Assert.assertEquals("org.hsqldb.jdbcDriver", databaseEntity.getDriver().getClazz());
        Assert.assertEquals(datasource.getVersion(), 0);
    }

    @Test
    public void testDatasourcePasswordFileEntity() throws Exception {

        InputStream stream = this.getClass().getResourceAsStream("/config/datasource/datasource-file-0.1.xml");
        Datasource datasource = datasourceEntityParser.parse(stream);
        ConfigurationStore store = ConfigurationStore.get();
        store.publish(EntityType.DATASOURCE, datasource);

        Datasource databaseEntity = EntityUtil.getEntity(EntityType.DATASOURCE, datasource.getName());
        Assert.assertEquals("test-hsql-db", datasource.getName());
        Assert.assertEquals("test-hsql-db", databaseEntity.getName());
        Assert.assertEquals("hsql", databaseEntity.getType().value());
        Assert.assertEquals("org.hsqldb.jdbcDriver", databaseEntity.getDriver().getClazz());
    }
}
