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

package org.apache.falcon.service;

import org.apache.falcon.FalconException;
import org.apache.falcon.util.ReflectionUtils;
import org.apache.falcon.util.StartupProperties;
import org.apache.log4j.Logger;

public class ServiceInitializer {

    private static Logger LOG = Logger.getLogger(ServiceInitializer.class);
    private final Services services = Services.get();

    public void initialize() throws FalconException {
        String serviceClassNames = StartupProperties.get().
                getProperty("application.services", "org.apache.falcon.entity.store.ConfigurationStore");
        for (String serviceClassName : serviceClassNames.split(",")) {
            serviceClassName = serviceClassName.trim();
            if (serviceClassName.isEmpty()) continue;
            FalconService service = ReflectionUtils.getInstanceByClassName(serviceClassName);
            services.register(service);
            LOG.info("Initializing service : " + serviceClassName);
            try {
                service.init();
            } catch(Throwable t) {
                LOG.fatal("Failed to initialize service " + serviceClassName, t);
                throw new FalconException(t);
            }
            LOG.info("Service initialized : " + serviceClassName);
        }
    }

    public void destroy() throws FalconException {
        for (FalconService service : services) {
            LOG.info("Destroying service : " + service.getClass().getName());
            try {
                service.destroy();
            } catch(Throwable t) {
                LOG.fatal("Failed to destroy service " + service.getClass().getName(), t);
                throw new FalconException(t);
            }
            LOG.info("Service destroyed : " + service.getClass().getName());
        }
    }
}
