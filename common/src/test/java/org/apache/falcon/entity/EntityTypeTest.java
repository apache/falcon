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

package org.apache.falcon.entity;

import org.apache.falcon.entity.v0.EntityType;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test for validating entity types.
 */
public class EntityTypeTest {

    @Test
    public void testGetEntityClass() {
        Assert.assertEquals(EntityType.PROCESS.getEntityClass().getName(),
                "org.apache.falcon.entity.v0.process.Process");
    }

    @Test
    public void testIsSchedulable() {
        Assert.assertTrue(EntityType.PROCESS.isSchedulable());
        Assert.assertTrue(EntityType.FEED.isSchedulable());
        Assert.assertFalse(EntityType.CLUSTER.isSchedulable());
    }

    @Test
    public void testValidEntityTypes() {
        Assert.assertEquals(EntityType.FEED, EntityType.getEnum("feed"));
        Assert.assertEquals(EntityType.FEED, EntityType.getEnum("FeEd"));
        Assert.assertEquals(EntityType.CLUSTER, EntityType.getEnum("cluster"));
        Assert.assertEquals(EntityType.CLUSTER, EntityType.getEnum("cluSTER"));
        Assert.assertEquals(EntityType.PROCESS, EntityType.getEnum("process"));
        Assert.assertEquals(EntityType.PROCESS, EntityType.getEnum("pRocess"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testInvalidEntityTypes() throws Exception {
        EntityType.getEnum("invalid");
    }
}
