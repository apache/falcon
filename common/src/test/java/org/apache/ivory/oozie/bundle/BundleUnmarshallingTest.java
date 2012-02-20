/*
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

package org.apache.ivory.oozie.bundle;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.testng.Assert;
import org.testng.annotations.Test;

public class BundleUnmarshallingTest {
    @Test
    public void testValidBundleUnamrashalling() throws Exception {
        Unmarshaller unmarshaller = JAXBContext.newInstance(org.apache.ivory.oozie.bundle.BUNDLEAPP.class).createUnmarshaller();
        SchemaFactory schemaFactory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
        Schema schema = schemaFactory.newSchema(this.getClass().getResource("/bundle.xsd"));
        unmarshaller.setSchema(schema);
        Object bundle = unmarshaller.unmarshal(new StreamSource(BundleUnmarshallingTest.class.getResourceAsStream("/bundle.xml")),
                BUNDLEAPP.class);
        BUNDLEAPP bundleApp = ((JAXBElement<BUNDLEAPP>) bundle).getValue();

        Assert.assertEquals(bundleApp.getName(), "bundle-app");
        Assert.assertEquals(bundleApp.getCoordinator().get(0).getName(), "coord-1");

    }
}
