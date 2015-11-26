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
package org.apache.falcon.state.service;

import org.apache.falcon.FalconException;
import org.apache.falcon.state.AbstractSchedulerTestBase;
import org.apache.falcon.state.store.service.FalconJPAService;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.persistence.EntityManager;
import java.io.IOException;
import java.util.Map;

/**
 * Test cases for FalconJPAService.
 */
public class TestFalconJPAService extends AbstractSchedulerTestBase {

    private static FalconJPAService falconJPAService = FalconJPAService.get();

    @BeforeClass
    public void setUp() throws Exception {
        super.setup();
        createDB(DB_SQL_FILE);
    }

    @Test
    public void testService() throws FalconException {
        // initialize it
        falconJPAService.init();
        EntityManager entityManager = falconJPAService.getEntityManager();
        Map<String, Object> props = entityManager.getProperties();
        Assert.assertNotNull(props);
        entityManager.close();
    }

    @AfterClass
    public void tearDown() throws FalconException, IOException {
        falconJPAService.destroy();
        super.cleanup();
    }




}
