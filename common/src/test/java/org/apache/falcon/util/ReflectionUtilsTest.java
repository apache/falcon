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

package org.apache.falcon.util;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.parser.ClusterEntityParser;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests ReflectionUtils.
 */
@Test
public class ReflectionUtilsTest {
    public void testGetInstance() throws FalconException {
        //with 1 arg constructor, arg null
        Object e = ReflectionUtils.getInstanceByClassName("org.apache.falcon.FalconException", Throwable.class, null);
        Assert.assertTrue(e instanceof  FalconException);

        //with 1 arg constructor, arg not null
        e = ReflectionUtils.getInstanceByClassName("org.apache.falcon.FalconException", Throwable.class,
            new Throwable());
        Assert.assertTrue(e instanceof  FalconException);

        //no constructor, using get() method
        e = ReflectionUtils.getInstanceByClassName("org.apache.falcon.util.StartupProperties");
        Assert.assertTrue(e instanceof  StartupProperties);

        //with empty constructor
        e = ReflectionUtils.getInstanceByClassName("org.apache.falcon.entity.parser.ClusterEntityParser");
        Assert.assertTrue(e instanceof ClusterEntityParser);
    }
}
