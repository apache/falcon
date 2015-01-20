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

package org.apache.falcon.util;

import org.apache.falcon.FalconException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Dynamic properties that may be modified while the server is running.
 */
public final class RuntimeProperties extends ApplicationProperties {

    private static final Logger LOG = LoggerFactory.getLogger(RuntimeProperties.class);

    private static final String PROPERTY_FILE = "runtime.properties";

    private static final AtomicReference<RuntimeProperties> INSTANCE =
            new AtomicReference<RuntimeProperties>();

    private RuntimeProperties() throws FalconException {
        super();
    }

    @Override
    protected String getPropertyFile() {
        return PROPERTY_FILE;
    }

    public static Properties get() {
        try {
            if (INSTANCE.get() == null) {
                RuntimeProperties properties = new RuntimeProperties();
                properties.loadProperties();
                properties.validateProperties();
                INSTANCE.compareAndSet(null, properties);
                if (INSTANCE.get() == properties) {
                    Thread refreshThread = new Thread(new DynamicLoader());
                    refreshThread.start();
                }
            }
            return INSTANCE.get();
        } catch (FalconException e) {
            throw new RuntimeException("Unable to read application " + "runtime properties", e);
        }
    }

    protected void validateProperties() throws FalconException {
        String colosProp = getProperty("all.colos");
        if (colosProp == null || colosProp.isEmpty()) {
            return;
        }
        String[] colos = colosProp.split(",");
        for (int i = 0; i < colos.length; i++) {
            colos[i] = colos[i].trim();
            String falconEndpoint = getProperty("falcon." + colos[i] + ".endpoint");
            if (falconEndpoint == null || falconEndpoint.isEmpty()) {
                throw new FalconException("No falcon server endpoint mentioned in Prism runtime for colo, "
                        + colos[i] + ".");
            }
        }
    }

    /**
     * Thread for loading properties periodically.
     */
    private static  final class DynamicLoader implements Runnable {

        private static final long REFRESH_DELAY = 300000L;
        private static final int MAX_ITER = 20;  //1hr

        @Override
        public void run() {
            long backOffDelay = REFRESH_DELAY;
            while (true) {
                try {
                    try {
                        RuntimeProperties newProperties = new RuntimeProperties();
                        newProperties.loadProperties();
                        newProperties.validateProperties();
                        INSTANCE.set(newProperties);
                        backOffDelay = REFRESH_DELAY;
                    } catch (FalconException e) {
                        LOG.warn("Error refreshing runtime properties", e);
                        backOffDelay += REFRESH_DELAY;
                    }
                    Thread.sleep(Math.min(MAX_ITER * REFRESH_DELAY, backOffDelay));
                } catch (InterruptedException e) {
                    LOG.error("Application is stopping. Aborting...");
                    break;
                }
            }
        }
    }
}
