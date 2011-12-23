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

package org.apache.ivory;

import java.io.InputStream;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.xml.sax.SAXException;

public class UtilTest {

	@Test
	public void getSchema() throws SAXException {
		Schema schema = Util.getSchema(UtilTest.class
				.getResource("/coordinator.xsd"));
		Assert.assertNotNull(schema);
	}

	@Test
	public void getStreamFromString() {
		InputStream stream = Util.getStreamFromString("<test>hi</test>");
		Assert.assertNotNull(stream);
	}

	@Test
	public void getUnmarshaller() throws JAXBException {
		Unmarshaller unmarshaller = Util.getUnmarshaller(UtilTest.class);
		Assert.assertNotNull(unmarshaller);
	}

	@Test
	public void getMarshaller() throws JAXBException {
		Marshaller marshaller = Util.getMarshaller(UtilTest.class);
		Assert.assertNotNull(marshaller);
	}
}
