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

import java.io.InputStream;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;

import org.apache.ivory.Util;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.store.StoreAccessException;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.dataset.Dataset;
import org.apache.ivory.entity.v0.process.Cluster;
import org.apache.ivory.entity.v0.process.Input;
import org.apache.ivory.entity.v0.process.Output;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

/**
 * Concrete Parser which has XML parsing and validation logic for Process XML.
 * 
 */
public class ProcessEntityParser extends EntityParser<Process> {

	private static final Logger LOG = Logger
			.getLogger(ProcessEntityParser.class);

	private static final String SCHEMA_FILE_NAME = "/schema/process/process-0.1.xsd";

	protected ProcessEntityParser(EntityType entityType,
			Class<Process> clazz) {
		super(entityType, clazz);
	}

	/**
	 * Applying Schema Validation during Unmarshalling Instead of using
	 * Validator class JAXB 2.0 supports this out-of-the-box
	 * 
	 * @throws JAXBException
	 * @throws SAXException
	 */
	@Override
	protected Process doParse(InputStream xmlStream) throws JAXBException,
			SAXException {
		Process processDefinitionElement = null;
		Unmarshaller unmarshaller;
		unmarshaller = EntityUnmarshaller.getInstance(this.getEntityType(),
				this.getClazz());
		// Validate against schema
		synchronized (this) {
			Schema schema = Util.getSchema(ProcessEntityParser.class
					.getResource(SCHEMA_FILE_NAME));
			unmarshaller.setSchema(schema);
			processDefinitionElement = (Process) unmarshaller
					.unmarshal(xmlStream);
		}

		return processDefinitionElement;
	}

	@Override
	protected void applyValidations(Process process) throws StoreAccessException,
			ValidationException {
		// check if dependent entities exists
		for (Cluster cluster : process.getClusters().getCluster()) {
			org.apache.ivory.entity.v0.cluster.Cluster clusterEntity = ConfigurationStore
					.get().get(EntityType.CLUSTER, cluster.getName());
			if (clusterEntity == null) {
				LOG.error("Dependent cluster "
						+ cluster.getName() + " not found for process "
						+ process.getName());
				throw new ValidationException("Dependent cluster "
						+ cluster.getName() + " not found for process "
						+ process.getName());
			}
		}
		for (Input input : process.getInputs().getInput()) {
			Dataset dataset = ConfigurationStore.get().get(EntityType.DATASET,
					input.getFeed());
			if (dataset == null) {
				LOG.error("Dependent dataset "
						+ input.getFeed() + " not found for process "
						+ process.getName());
				throw new ValidationException("Dependent dataset "
						+ input.getFeed() + " not found for process "
						+ process.getName());
			}
		}
		for (Output output : process.getOutputs().getOutput()) {
			Dataset dataset = ConfigurationStore.get().get(EntityType.DATASET,
					output.getFeed());
			if (dataset == null) {
				LOG.error("Dependent dataset "
						+ output.getFeed() + " not found for process "
						+ process.getName());
				throw new ValidationException("Dependent dataset "
						+ output.getFeed() + " not found for process "
						+ process.getName());
			}
		}

		fieldValidations(process);
	}

	private void fieldValidations(Process entity)
			throws ValidationException {
		// TODO

	}
}
