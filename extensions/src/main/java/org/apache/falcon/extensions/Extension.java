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

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.extensions.store.ExtensionStore;
import org.apache.falcon.extensions.util.ExtensionProcessBuilderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Extension class to construct well formed entities from the templates for trusted extensions.
 */
public class Extension implements ExtensionBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(Extension.class);
    private static final String EXTENSION_WF_SUFFIX = "-workflow.xml";
    private static final String EXTENSION_TEMPLATE_SUFFIX = "-template.xml";

    private static void validateProperties(final Properties extensionProperties) throws FalconException {
        for (ExtensionProperties option : ExtensionProperties.values()) {
            if (extensionProperties.getProperty(option.getName()) == null && option.isRequired()) {
                throw new FalconException("Missing extension property: " + option.getName());
            }
        }
    }

    private static String getExtensionTemplate(final ExtensionStore store,
                                            final Map<String, String> extensionResources,
                                            final String extensionName) throws FalconException {
        return store.getExtensionResource(extensionResources.get(extensionName.toLowerCase()
                + EXTENSION_TEMPLATE_SUFFIX));
    }

    private static String getWFPath(final Map<String, String> extensionResources,
                                    final String extensionName) {
        return extensionResources.get(extensionName.toLowerCase() + EXTENSION_WF_SUFFIX);
    }

    @Override
    public List<Entity> getEntities(final String extensionName, final Properties extensionProperties)
        throws FalconException {
        if (StringUtils.isBlank(extensionName)) {
            throw new FalconException("Extension name cannot be null or empty");
        }
        validateProperties(extensionProperties);

        String name = extensionName.toLowerCase();
        AbstractExtension extension = ExtensionFactory.getExtensionType(name);
        if (extension != null) {
            extension.validate(extensionProperties);
            Properties props = extension.getAdditionalProperties(extensionProperties);
            if (props != null && !props.isEmpty()) {
                extensionProperties.putAll(props);
            }
        }

        ExtensionStore store = ExtensionService.getExtensionStore();

        String resourceName = extensionProperties.getProperty(ExtensionProperties.RESOURCE_NAME.getName());
        if (StringUtils.isBlank(resourceName)) {
            resourceName = name;
        }

        Map<String, String> extensionResources = store.getExtensionResources(name);
        /* Get the resources */
        String extensionTemplate = getExtensionTemplate(store, extensionResources, resourceName);
        String wfPath = getWFPath(extensionResources, resourceName);

        /* Get Lib path */
        String wfLibPath = store.getExtensionLibPath(name);
        Entity entity = ExtensionProcessBuilderUtils.createProcessFromTemplate(extensionTemplate,
                name, extensionProperties, wfPath, wfLibPath);
        if (entity == null) {
            throw new FalconException("Entity created from the extension template cannot be null");
        }
        LOG.info("Extension processing complete");
        return Arrays.asList(entity);
    }
}
