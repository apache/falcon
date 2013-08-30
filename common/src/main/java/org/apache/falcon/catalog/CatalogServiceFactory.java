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

package org.apache.falcon.catalog;

import org.apache.falcon.FalconException;
import org.apache.falcon.util.ReflectionUtils;
import org.apache.falcon.util.StartupProperties;

/**
 * Factory for providing appropriate catalog service
 * implementation to the falcon service.
 */
@SuppressWarnings("unchecked")
public final class CatalogServiceFactory {

    public static final String CATALOG_SERVICE = "catalog.service.impl";

    private CatalogServiceFactory() {
    }

    public static boolean isEnabled() {
        return StartupProperties.get().containsKey(CATALOG_SERVICE);
    }

    public static AbstractCatalogService getCatalogService() throws FalconException {
        if (!isEnabled()) {
            throw new FalconException(
                "Catalog integration is not enabled in falcon. Implementation is missing: " + CATALOG_SERVICE);
        }

        return ReflectionUtils.getInstance(CATALOG_SERVICE);
    }
}
