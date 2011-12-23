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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URL;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.ivory.entity.parser.EntityParser;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

/**
 * 
 * Util classes containing helper methods required by other classes.
 * 
 */
public final class Util {

	private static final Logger LOG = Logger.getLogger(EntityParser.class);

	private static final SchemaFactory schemaFactory = SchemaFactory
			.newInstance("http://www.w3.org/2001/XMLSchema");

	private Util() {

	}

	/**
	 * Returns inputstream from a given text
	 * 
	 * @param text
	 * @return
	 */
	public static InputStream getStreamFromString(String text) {
		InputStream inputStream = null;
		inputStream = new ByteArrayInputStream(text.getBytes());
		return inputStream;
	}

	/**
	 * Retruns JAXB unmarshaller for a given class type
	 * 
	 * @param clazz
	 * @return
	 * @throws JAXBException
	 */
	public static Unmarshaller getUnmarshaller(Class<?> clazz)
			throws JAXBException {
		JAXBContext jaxbContext = JAXBContext.newInstance(clazz);
		Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
		return unmarshaller;
	}

	/**
	 * Retruns JAXB Marshaller for a given class type
	 * 
	 * @param clazz
	 * @return
	 * @throws JAXBException
	 */
	public static Marshaller getMarshaller(Class<?> clazz) throws JAXBException {
		JAXBContext jaxbContext = JAXBContext.newInstance(clazz);
		Marshaller marshaller = jaxbContext.createMarshaller();
		return marshaller;
	}

	/**
	 * Returns Schema for a given Schema URL
	 * 
	 * @param xmlSchemaURL
	 * @return
	 * @throws SAXException
	 */
	public static Schema getSchema(URL xmlSchemaURL) throws SAXException {
		Schema schema = null;
		schema = schemaFactory.newSchema(xmlSchemaURL);
		return schema;
	}

}
