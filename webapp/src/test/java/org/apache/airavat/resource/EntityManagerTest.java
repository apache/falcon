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
package org.apache.airavat.resource;

import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;

import org.apache.airavat.entity.v0.EntityType;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit testing class for EntityManager class for testing APIs/methods in it.
 */
public class EntityManagerTest {

	@Mock
	private HttpServletRequest mockHttpServletRequest;

	private static final String SAMPLE_PROCESS_XML = "/process-version-0.xml";

	private static final String SAMPLE_INVALID_PROCESS_XML = "/process-invalid.xml";

	private final EntityManager entityManager = new EntityManager();

	@BeforeClass
	public void init() {
		MockitoAnnotations.initMocks(this);
	}

	@SuppressWarnings("unused")
	@DataProvider(name = "validXMLServletStreamProvider")
	private Object[][] servletStreamProvider() {
		ServletInputStream validProcessXML = getServletInputStream(SAMPLE_PROCESS_XML);

		// TODO change the xml for Feed and DataEndPoint
		return new Object[][] { { EntityType.PROCESS, validProcessXML },
		// { EntityType.FEED, validProcessXML },
		// { EntityType.DATAENDPOINT, validProcessXML }
		};

	}

	/**
	 * Run this testcase for different types of VALID entity xmls like process,
	 * feed, dataEndPoint
	 * 
	 * @param testType
	 * @param stream
	 * @throws IOException
	 */
	@Test(dataProvider = "validXMLServletStreamProvider")
	public void testValidateForValidEntityXML(EntityType entityType,
			ServletInputStream stream) throws IOException {

		when(mockHttpServletRequest.getInputStream()).thenReturn(stream);

		APIResult apiResult = entityManager.validate(mockHttpServletRequest,
				entityType.name());

		Assert.assertNotNull(apiResult);
		Assert.assertEquals(APIResult.Status.SUCCEEDED, apiResult.getStatus());

		// verify(mockHttpServletRequest, times(1)).getInputStream();
	}

	@Test
	public void testValidateForInvalidEntityXML() throws IOException {
		ServletInputStream invalidProcessXML = getServletInputStream(SAMPLE_INVALID_PROCESS_XML);
		when(mockHttpServletRequest.getInputStream()).thenReturn(
				invalidProcessXML);

		APIResult apiResult = entityManager.validate(mockHttpServletRequest,
				EntityType.PROCESS.name());

		Assert.assertNotNull(apiResult);

		Assert.assertEquals(APIResult.Status.FAILED, apiResult.getStatus());

		// Assert.assertEquals("[org.xml.sax.SAXParseException: cvc-complex-type.2.4.a: Invalid content was found starting with element 'somenode'. One of '{input}' is expected.]",apiResult.getMessage());

	}

	@Test
	public void testValidateForInvalidEntityType() throws IOException {
		ServletInputStream invalidProcessXML = getServletInputStream(SAMPLE_PROCESS_XML);
		when(mockHttpServletRequest.getInputStream()).thenReturn(
				invalidProcessXML);

		APIResult apiResult = entityManager.validate(mockHttpServletRequest,
				"InvalidEntityType");

		Assert.assertNotNull(apiResult);

		Assert.assertEquals(APIResult.Status.FAILED, apiResult.getStatus());

		Assert.assertEquals(
				"No enum const class org.apache.airavat.entity.v0.EntityType.INVALIDENTITYTYPE",
				apiResult.getMessage());

	}

	/**
	 * Converts a InputStream into ServletInputStream
	 * 
	 * @param resourceName
	 * @return ServletInputStream
	 */
	private ServletInputStream getServletInputStream(String resourceName) {
		final InputStream stream = this.getClass().getResourceAsStream(
				resourceName);

		return new ServletInputStream() {

			@Override
			public int read() throws IOException {
				return stream.read();
			}
		};
	}

}
