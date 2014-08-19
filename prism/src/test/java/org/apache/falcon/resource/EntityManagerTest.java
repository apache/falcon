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

import org.apache.falcon.FalconWebException;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.ACL;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.util.StartupProperties;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.InputStream;

import static org.mockito.Mockito.when;

/**
 * Unit testing class for AbstractEntityManager class for testing APIs/methods in it.
 */
public class EntityManagerTest extends AbstractEntityManager {

    @Mock
    private HttpServletRequest mockHttpServletRequest;
    private static final String SAMPLE_PROCESS_XML = "/process-version-0.xml";

    private static final String SAMPLE_INVALID_PROCESS_XML = "/process-invalid.xml";

    @BeforeClass
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    @SuppressWarnings("unused")
    @DataProvider(name = "validXMLServletStreamProvider")
    private Object[][] servletStreamProvider() {
        ServletInputStream validProcessXML = getServletInputStream(SAMPLE_PROCESS_XML);
        return new Object[][]{{EntityType.PROCESS, validProcessXML},
        };

    }

    /**
     * Run this testcase for different types of VALID entity xmls like process, feed, dataEndPoint.
     *
     * @param stream entity stream
     * @throws IOException
     */
    @Test(dataProvider = "validXMLServletStreamProvider")
    public void testValidateForValidEntityXML(ServletInputStream stream) throws IOException {

        when(mockHttpServletRequest.getInputStream()).thenReturn(stream);
    }

    @Test
    public void testValidateForInvalidEntityXML() throws IOException {
        ServletInputStream invalidProcessXML = getServletInputStream(SAMPLE_INVALID_PROCESS_XML);
        when(mockHttpServletRequest.getInputStream()).thenReturn(
                invalidProcessXML);

        try {
            validate(mockHttpServletRequest,
                    EntityType.PROCESS.name());
            Assert.fail("Invalid entity type was accepted by the system");
        } catch (FalconWebException ignore) {
            // ignore
        }
    }

    @Test
    public void testValidateForInvalidEntityType() throws IOException {
        ServletInputStream invalidProcessXML = getServletInputStream(SAMPLE_PROCESS_XML);
        when(mockHttpServletRequest.getInputStream()).thenReturn(
                invalidProcessXML);

        try {
            validate(mockHttpServletRequest,
                    "InvalidEntityType");
            Assert.fail("Invalid entity type was accepted by the system");
        } catch (FalconWebException ignore) {
            // ignore
        }
    }

    @Test
    public void testGetEntityList() throws Exception {

        Entity process1 = buildProcess("processFakeUser", "fakeUser", "", "");
        configStore.publish(EntityType.PROCESS, process1);

        Entity process2 = buildProcess("processAuthUser", System.getProperty("user.name"), "", "");
        configStore.publish(EntityType.PROCESS, process2);

        EntityList entityList = this.getEntityList("process", "", "", "", "", 0, -1);
        Assert.assertNotNull(entityList.getElements());
        Assert.assertEquals(entityList.getElements().length, 2);

        /*
         * Both entities should be returned when the user is SuperUser.
         */
        StartupProperties.get().setProperty("falcon.security.authorization.enabled", "true");
        CurrentUser.authenticate(System.getProperty("user.name"));
        entityList = this.getEntityList("process", "", "", "", "", 0, -1);
        Assert.assertNotNull(entityList.getElements());
        Assert.assertEquals(entityList.getElements().length, 2);

        /*
         * Only one entity should be returned when the auth is enabled.
         */
        CurrentUser.authenticate("fakeUser");
        entityList = this.getEntityList("process", "", "", "", "", 0, -1);
        Assert.assertNotNull(entityList.getElements());
        Assert.assertEquals(entityList.getElements().length, 1);

        // reset values
        StartupProperties.get().setProperty("falcon.security.authorization.enabled", "false");
        CurrentUser.authenticate(System.getProperty("user.name"));
    }

    @Test
    public void testGetEntityListPagination() throws Exception {
        String user = System.getProperty("user.name");

        Entity process1 = buildProcess("process1", user,
                "consumer=consumer@xyz.com, owner=producer@xyz.com",
                "testPipeline,dataReplicationPipeline");
        configStore.publish(EntityType.PROCESS, process1);

        Entity process2 = buildProcess("process2", user,
                "consumer=consumer@xyz.com, owner=producer@xyz.com",
                "testPipeline,dataReplicationPipeline");
        configStore.publish(EntityType.PROCESS, process2);

        Entity process3 = buildProcess("process3", user, "consumer=consumer@xyz.com", "testPipeline");
        configStore.publish(EntityType.PROCESS, process3);

        Entity process4 = buildProcess("process4", user, "owner=producer@xyz.com", "dataReplicationPipeline");
        configStore.publish(EntityType.PROCESS, process4);

        EntityList entityList = this.getEntityList("process", "tags", "PIPELINES:dataReplicationPipeline",
                "", "name", 1, 2);
        Assert.assertNotNull(entityList.getElements());
        Assert.assertEquals(entityList.getElements().length, 2);
        Assert.assertEquals(entityList.getElements()[1].name, "process4");
        Assert.assertEquals(entityList.getElements()[1].tag.size(), 1);
        Assert.assertEquals(entityList.getElements()[1].tag.get(0), "owner=producer@xyz.com");
        Assert.assertEquals(entityList.getElements()[0].status, null);


        entityList = this.getEntityList("process", "pipelines", "",
                "consumer=consumer@xyz.com, owner=producer@xyz.com", "name", 0, 2);
        Assert.assertNotNull(entityList.getElements());
        Assert.assertEquals(entityList.getElements().length, 2);
        Assert.assertEquals(entityList.getElements()[1].name, "process2");
        Assert.assertEquals(entityList.getElements()[1].pipelines.size(), 2);
        Assert.assertEquals(entityList.getElements()[1].pipelines.get(0), "testPipeline");
        Assert.assertEquals(entityList.getElements()[0].tag, null);

        entityList = this.getEntityList("process", "pipelines", "",
                "consumer=consumer@xyz.com, owner=producer@xyz.com", "name", 10, 2);
        Assert.assertEquals(entityList.getElements().length, 0);
    }

    private Entity buildProcess(String name, String username, String tags, String pipelines) {
        ACL acl = new ACL();
        acl.setOwner(username);
        acl.setGroup("hdfs");
        acl.setPermission("*");

        Process p = new Process();
        p.setName(name);
        p.setACL(acl);
        p.setPipelines(pipelines);
        p.setTags(tags);
        return p;
    }



    /**
     * Converts a InputStream into ServletInputStream.
     *
     * @param resourceName resource name
     * @return ServletInputStream
     */
    private ServletInputStream getServletInputStream(String resourceName) {
        final InputStream stream = this.getClass().getResourceAsStream(resourceName);
        return new ServletInputStream() {

            @Override
            public int read() throws IOException {
                return stream.read();
            }
        };
    }
}
