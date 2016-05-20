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
import org.apache.falcon.extensions.store.ExtensionStore;
import org.apache.falcon.service.FalconService;

/**
 * Extension service.
 */
public class ExtensionService implements FalconService {
    public static final String SERVICE_NAME = ExtensionService.class.getSimpleName();

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public void init() throws FalconException {
        // Validate if extension store is setup
        getExtensionStore();
    }

    @Override
    public void destroy() throws FalconException {
    }

    public static ExtensionStore getExtensionStore() {
        return ExtensionStore.get();
    }
}
