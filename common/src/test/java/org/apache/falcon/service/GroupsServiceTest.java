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

package org.apache.falcon.service;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.Assert;

import java.util.List;

/**
 * Unit tests for GroupsService.
 */
public class GroupsServiceTest {

    private GroupsService service;

    @BeforeClass
    public void setUp() throws Exception {
        service = new GroupsService();
        service.init();
    }

    @AfterClass
    public void tearDown() throws Exception {
        service.destroy();
    }

    @Test
    public void testGetName() throws Exception {
        Assert.assertEquals(service.getName(), GroupsService.SERVICE_NAME);
    }

    @Test
    public void testGroupsService() throws Exception {
        List<String> g = service.getGroups(System.getProperty("user.name"));
        Assert.assertNotSame(g.size(), 0);
    }
}
