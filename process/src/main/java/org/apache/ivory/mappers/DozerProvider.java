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
package org.apache.ivory.mappers;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.dozer.DozerBeanMapper;

/**
 * Dozer Bean Mapping class, which creates a singleton mapper, which should be
 * used across the application
 */
public final class DozerProvider {

	private static final Logger LOG = Logger.getLogger(DozerProvider.class);
	private DozerProvider() {
		// let clients not create this.		
	}

	public static void map(String[] filenames, Object source, Object destination) {
		DozerBeanMapper mapper = new DozerBeanMapper();
		List<String> mapperFiles = Arrays.asList(filenames);
		mapper.setMappingFiles(mapperFiles);
		mapper.map(source, destination);
	}

}
