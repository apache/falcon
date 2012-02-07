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
import org.apache.ivory.entity.store.StoreAccessException;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

public class FeedEntityParser extends EntityParser<Feed> {

	private static final Logger LOG = Logger
			.getLogger(ProcessEntityParser.class);

	private static final String SCHEMA_FILE = "/schema/feed/feed-0.1.xsd";

	protected FeedEntityParser(EntityType entityType, Class<Feed> clazz) {
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
	public Feed doParse(InputStream xmlStream) throws JAXBException,
			SAXException {

		Feed feed = null;
		Unmarshaller unmarshaller = EntityUnmarshaller.getInstance(
				this.getEntityType(), this.getClazz());
		// Validate against schema
		synchronized (this) {
			Schema schema = Util.getSchema(FeedEntityParser.class
					.getResource(SCHEMA_FILE));
			unmarshaller.setSchema(schema);
			feed = (Feed) unmarshaller.unmarshal(xmlStream);
		}
		return feed;
	}

	@Override
	public void applyValidations(Feed entity) throws StoreAccessException,
			ValidationException {
		// ConfigurationStore store = ConfigurationStore.get();
		// Dataset existingEntity = store.get(EntityType.DATASET,
		// entity.getName());
		// if (existingEntity != null) {
		// throw new ValidationException("Entity: " + entity.getName()
		// + " already submitted");
		// }
		// TODO check if dependent Feed and Datastore exists
		fieldValidations(entity);
	}

	private void fieldValidations(Feed entity) throws ValidationException {
		// TODO

	}

}
