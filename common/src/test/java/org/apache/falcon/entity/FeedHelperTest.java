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

import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Properties;
import org.apache.falcon.entity.v0.cluster.Property;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test for feed helper methods.
 */
public class FeedHelperTest {
    @Test
    public void testPartitionExpression() {
        Assert.assertEquals(FeedHelper.normalizePartitionExpression(" /a// ", "  /b// "), "a/b");
        Assert.assertEquals(FeedHelper.normalizePartitionExpression(null, "  /b// "), "b");
        Assert.assertEquals(FeedHelper.normalizePartitionExpression(null, null), "");
    }

    @Test
    public void testEvaluateExpression() throws Exception {
        Cluster cluster = new Cluster();
        cluster.setName("name");
        cluster.setColo("colo");
        cluster.setProperties(new Properties());
        Property prop = new Property();
        prop.setName("pname");
        prop.setValue("pvalue");
        cluster.getProperties().getProperties().add(prop);

        Assert.assertEquals(FeedHelper.evaluateClusterExp(cluster, "${cluster.colo}/*/US"), "colo/*/US");
        Assert.assertEquals(FeedHelper.evaluateClusterExp(cluster, "${cluster.name}/*/${cluster.pname}"),
                "name/*/pvalue");
        Assert.assertEquals(FeedHelper.evaluateClusterExp(cluster, "IN"), "IN");
    }
}
