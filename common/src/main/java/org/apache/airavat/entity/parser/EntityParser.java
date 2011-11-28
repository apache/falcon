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

package org.apache.airavat.entity.parser;

import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.airavat.entity.v0.Entity;
import org.apache.airavat.entity.v0.EntityType;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

/**
 * 
 * Generic Abstract Entity Parser, the concrete FEED, PROCESS and DATAENDPOINT
 * Should extend this parser to implement specific parsing.
 * 
 * @param <T>
 */
public abstract class EntityParser<T extends Entity> {

	private static Logger LOG = Logger.getLogger(EntityParser.class);

	private EntityType entityType;

	private Class<? extends Entity> clazz;

	/**
	 * Constructor
	 * 
	 * @param entityType
	 *            - can be FEED or PROCESS
	 * @param clazz
	 *            - Class to be used for Unmarshaling.
	 */
	protected EntityParser(EntityType entityType, Class<? extends Entity> clazz) {
		this.entityType = entityType;
		this.clazz = clazz;
	}

	public Class<? extends Entity> getClazz() {
		return clazz;
	}

	public EntityType getEntityType() {
		return entityType;
	}

	/**
	 * Parses a sent XML and validates it using JAXB.
	 * 
	 * @param xml
	 * @return Entity
	 */
	public Entity parse(String xml) {
		if (validateSchema(xml)) {
			T entity = null;
			try {
				entity = doParse(xml);
			} catch (SAXException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JAXBException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			applyValidations(entity);
		}
		return null;
	}

	private boolean validateSchema(String xml) {
		// TODO use getEntityType to fetch xsd for validation
		return true;
	}

	protected abstract T doParse(String xml) throws SAXException, JAXBException;

	protected abstract void applyValidations(T entity);

	/**
	 * Static Inner class that will be used by the concrete Entity to get
	 * Unmarshallers
	 * 
	 */
	public static class EntityUnmarshaller {

		private static final Map<EntityType, Unmarshaller> unmarshallers = new HashMap<EntityType, Unmarshaller>();

		private EntityUnmarshaller() {
		}

		synchronized public static Unmarshaller getInstance(
				EntityType entityType, Class<? extends Entity> clazz)
				throws JAXBException {
			if (unmarshallers.get(entityType) == null) {
				try {
					JAXBContext jaxbContext = JAXBContext.newInstance(clazz);
					Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
					unmarshallers.put(entityType, unmarshaller);
				} catch (JAXBException e) {
					LOG.fatal("Unable to get JAXBContext", e);
					throw new JAXBException(e);
				}
			}
			return unmarshallers.get(entityType);
		}
	}

}
