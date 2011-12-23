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

import org.apache.ivory.IvoryException;
import org.apache.ivory.Util;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 * Generic Abstract Entity Parser, the concrete FEED, PROCESS and DATAENDPOINT
 * Should extend this parser to implement specific parsing.
 * 
 * @param <T>
 */
public abstract class EntityParser<T extends Entity> {

	private static final Logger LOG = Logger.getLogger(EntityParser.class);

	private final EntityType entityType;

	private final Class<? extends Entity> clazz;

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
		return this.clazz;
	}

	public EntityType getEntityType() {
		return this.entityType;
	}

	/**
	 * Parses a sent XML and validates it using JAXB.
	 * 
	 * @param xmlString
	 *            - Entity XML
	 * @return Entity - JAVA Object
	 * @throws IvoryException
	 */
	public Entity parse(String xmlString) throws IvoryException {

		InputStream inputStream = Util.getStreamFromString(xmlString);
		Entity entity = parse(inputStream);
		return entity;

	}

	/**
	 * Parses xml stream
	 * 
	 * @param xmlStream
	 * @return entity
	 * @throws IvoryException
	 */
	public Entity parse(InputStream xmlStream) throws IvoryException {
		T entity = null;

		try {
			entity = doParse(xmlStream);
		} catch (JAXBException e) {
			throw new IvoryException(e);
		} catch (SAXException e) {
			throw new IvoryException(e);
		}

		applyValidations(entity);

		return entity;
	}

	/**
	 * Validates a entity xmlString
	 * 
	 * @param xmlString
	 * @return
	 * @throws IvoryException
	 */
	public boolean validateSchema(String xmlString) throws IvoryException {
		InputStream xmlStream = Util.getStreamFromString(xmlString);
		return validateSchema(xmlStream);
	}

	/**
	 * Validate also uses JAXB 2.0 unmarshalling If No JAXB error than validate
	 * success.
	 * 
	 * @throws IvoryException
	 */
	public boolean validateSchema(InputStream xmlStream)
			throws IvoryException {
		try {
			doParse(xmlStream);
		} catch (JAXBException e) {
			throw new IvoryException(e);
		} catch (SAXException e) {
			throw new IvoryException(e);
		}
		return true;
	}

	protected abstract T doParse(InputStream xml) throws JAXBException,
			SAXException;

	protected abstract void applyValidations(T entity);

	/**
	 * Static Inner class that will be used by the concrete Entity to get
	 * Unmarshallers based on the Entity Type
	 * 
	 */
	public static final class EntityUnmarshaller {

		/**
		 * Map which holds Unmarshaller as value for each entity type key.
		 */
		private static final Map<EntityType, Unmarshaller> UNMARSHALLER = new HashMap<EntityType, Unmarshaller>();

		private EntityUnmarshaller() {
		}

		public static Unmarshaller getInstance(EntityType entityType,
				Class<? extends Entity> clazz) throws JAXBException {
			if (UNMARSHALLER.get(entityType) == null) {
				try {
					Unmarshaller unmarshaller = Util.getUnmarshaller(clazz);
					UNMARSHALLER.put(entityType, unmarshaller);
				} catch (JAXBException e) {
					LOG.fatal("Unable to get JAXBContext", e);
					throw new JAXBException(e);
				}
			}
			return UNMARSHALLER.get(entityType);
		}
	}

}
