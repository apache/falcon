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

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Test frequency.
 */
@Test
public class LineageGraphResultTest {

    @Test
    public void testEquals() {
        Set<LineageGraphResult.Edge> set1 = new HashSet<>();
        Set<LineageGraphResult.Edge> set2 = new HashSet<>();

        List<String> from =  Arrays.asList(new String[]{"from1", "from2", "from3"});
        List<String> to =  Arrays.asList(new String[]{"to1", "to2", "to3"});
        List<String> label =  Arrays.asList(new String[]{"label1", "label2", "label3"});

        for (int i = 0; i < 3; i++) {
            set1.add(new LineageGraphResult.Edge(from.get(i), to.get(i), label.get(i)));
            set2.add(new LineageGraphResult.Edge(from.get(i), to.get(i), label.get(i)));
        }
        Assert.assertEquals(set1, set2);
    }
}
