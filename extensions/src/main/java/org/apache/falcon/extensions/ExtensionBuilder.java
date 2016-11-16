/**
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

package org.apache.falcon.extensions;

import org.apache.falcon.FalconException;
import org.apache.falcon.Pair;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.feed.Schema;

import java.io.InputStream;
import java.util.List;

/**
 * Extension interface to be implemented by all trusted and custom extensions.
 */
public interface ExtensionBuilder {

    /**
     * @param extensionName extension name.
     * @param extensionConfigStream stream comprising of the extension properties.
     * @return List of the entities that are involved in the extension.
     * @throws FalconException
     */
    List<Entity> getEntities(final String extensionName, final InputStream extensionConfigStream)
        throws FalconException;

    /**
     * @param extensionName extension name.
     * @param extensionConfigStream Properties supplied will be validated.
     * @throws FalconException
     */
    void validateExtensionConfig(final String extensionName, final InputStream extensionConfigStream)
        throws FalconException;

    /**
     * @param extensionName extension name.
     * @return List of the feed names along with the schema that the extension has generated if any.
     * @throws FalconException
     */
    List<Pair<String, Schema>> getOutputSchemas(final String extensionName) throws FalconException;
}
