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

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

/**
 * Utilities for falcon unit tests.
 */
public final class HadoopQueueUtilTest {

    @Test
    public void testGetHadoopClusterQueueNamesHelper1() throws Exception {
        final InputStream inputStream = this.getClass().getResourceAsStream("/config/feed/feed-schedulerinfo-1.json");
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        String jsonResult = "";
        String line;
        while((line = br.readLine()) != null) {
            jsonResult += line;
        }
        Set<String> qNames = new HashSet<>();
        HadoopQueueUtil.getHadoopClusterQueueNamesHelper(jsonResult, qNames);
        Assert.assertEquals(qNames.size(), 9);
    }

    @Test
    public void testGetHadoopClusterQueueNamesHelper2() throws Exception {
        final InputStream inputStream = this.getClass().getResourceAsStream("/config/feed/feed-schedulerinfo-2.json");
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        String jsonResult = "";
        String line;
        while((line = br.readLine()) != null) {
            jsonResult += line;
        }
        Set<String> qNames = new HashSet<>();
        HadoopQueueUtil.getHadoopClusterQueueNamesHelper(jsonResult, qNames);
        Assert.assertTrue(qNames.contains("default"));
    }

}
