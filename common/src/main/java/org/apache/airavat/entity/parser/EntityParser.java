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

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.airavat.Util;
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
	 * @param xmlString
	 *            - Entity XML
	 * @return Entity - JAVA Object
	 */
	public Entity parse(String xmlString) {

		InputStream inputStream = Util.getStreamFromString(xmlString);
		Entity entity = parse(inputStream);
		return entity;

	}

	public Entity parse(InputStream xmlStream) {
		T entity = null;
		if (validateSchema(xmlStream)) {
			try {
				entity = doParse(xmlStream);
			} catch (SAXException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JAXBException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			applyValidations(entity);
		}
		return entity;
	}

	public boolean validateSchema(String xmlString) {
		InputStream xmlStream = Util.getStreamFromString(xmlString);
		return validateSchema(xmlStream);
	}

	public abstract boolean validateSchema(InputStream xmlStream);

	protected abstract T doParse(InputStream xml) throws SAXException,
			JAXBException;

	protected abstract void applyValidations(T entity);

	/**
	 * Static Inner class that will be used by the concrete Entity to get
	 * Unmarshallers based on the Entity Type
	 * 
	 */
	public static class EntityUnmarshaller {

		/**
		 * Map which holds Unmarshaller as value for each entity type key.
		 */
		private static final Map<EntityType, Unmarshaller> unmarshallers = new HashMap<EntityType, Unmarshaller>();

		private EntityUnmarshaller() {
		}

		public static Unmarshaller getInstance(EntityType entityType,
				Class<? extends Entity> clazz) throws JAXBException {
			if (unmarshallers.get(entityType) == null) {
				synchronized (Unmarshaller.class) {
					if (unmarshallers.get(entityType) == null)
						try {
							JAXBContext jaxbContext = JAXBContext
									.newInstance(clazz);
							Unmarshaller unmarshaller = jaxbContext
									.createUnmarshaller();
							unmarshallers.put(entityType, unmarshaller);
						} catch (JAXBException e) {
							LOG.fatal("Unable to get JAXBContext", e);
							throw new JAXBException(e);
						}
				}
			}
			return unmarshallers.get(entityType);
		}
	}

}
