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

package org.apache.ivory.entity.parser;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;

import org.apache.ivory.IvoryException;
import org.apache.ivory.Util;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.cluster.Interface;
import org.apache.ivory.entity.v0.cluster.Interfacetype;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClusterEntityParserTest {

	private final ClusterEntityParser parser = (ClusterEntityParser) EntityParserFactory
			.getParser(EntityType.CLUSTER);

	private static final String SAMPLE_DATASET_XML = "/config/cluster/cluster-0.1.xml";

	@Test
	public void testParse() throws IOException, IvoryException, JAXBException {

		Cluster cluster = null;

		InputStream stream = this.getClass().getResourceAsStream(
				SAMPLE_DATASET_XML);

		cluster = (Cluster) parser.parse(stream);

		Assert.assertNotNull(cluster);

		Assert.assertEquals(cluster.getName(), "corp");

		Interface _interface = cluster.getInterfaces().get(
				Interfacetype.EXECUTE);
		Assert.assertEquals(_interface.getEndpoint(), "localhost:8021");
		Assert.assertEquals(_interface.getVersion(), "0.20.2");

		Assert.assertEquals(cluster.getLocations().getLocation().get(0)
				.getName(), "stagingdir");
		Assert.assertEquals(cluster.getLocations().getLocation().get(0)
				.getPath(), "/projects/ivory/staging");

		Assert.assertEquals(cluster.getProperties().getProperty().get(0)
				.getName(), "field1");
		Assert.assertEquals(cluster.getProperties().getProperty().get(0)
				.getValue(), "value1");

		StringWriter stringWriter = new StringWriter();
		Marshaller marshaller = Util.getMarshaller(Cluster.class);
		marshaller.marshal(cluster, stringWriter);
		System.out.println(stringWriter.toString());

	}

	// TODO
	@Test
	public void applyValidations() {
		// throw new RuntimeException("Test not implemented");
	}
}
