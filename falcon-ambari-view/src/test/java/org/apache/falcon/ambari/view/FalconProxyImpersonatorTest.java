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
package org.apache.falcon.ambari.view;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import org.apache.ambari.view.ViewContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * This is a class used to test FalconProxyImpersonator to bridge the communication between the falcon-ui
 * and the falcon API executing inside ambari.
 */
public class FalconProxyImpersonatorTest {

    private ViewContext viewContext;
    private HttpHeaders headers;
    private UriInfo ui;

    private FalconProxyImpersonator impersonator;

    @Before
    public void setUp() throws Exception {
        viewContext = mock(ViewContext.class);
        headers = mock(HttpHeaders.class);
        ui = mock(UriInfo.class);
        impersonator = new FalconProxyImpersonator(viewContext);
    }

    @Test
    public void testSetUser(){
        Mockito.when(viewContext.getUsername()).thenReturn("ambari-qa");
        Response response = impersonator.setUser(headers, ui);
        assertEquals(200, response.getStatus());
    }

}
