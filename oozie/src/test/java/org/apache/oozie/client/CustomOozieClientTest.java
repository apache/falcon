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

package org.apache.oozie.client;

import org.testng.annotations.Test;

import java.util.Properties;

public class CustomOozieClientTest {

    @Test (enabled = false)
    public void testGetConfiguration() throws Exception {
        CustomOozieClient client = new CustomOozieClient("http://localhost:11000/oozie");
        Properties props = client.getConfiguration();
        System.out.println(props);
        props = client.getProperties();
        System.out.println(props);
    }
}
