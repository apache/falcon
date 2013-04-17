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
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Dynamic properties that may be modified while the server is running.
 */
public final class RuntimeProperties extends ApplicationProperties {

    private static final Logger LOG = Logger.getLogger(RuntimeProperties.class);

    private static final String PROPERTY_FILE = "runtime.properties";

    private static final AtomicReference<RuntimeProperties> INSTANCE =
            new AtomicReference<RuntimeProperties>();

    private RuntimeProperties() throws FalconException {
        super();
        Thread refreshThread = new Thread(new DynamicLoader(this));
        refreshThread.start();
    }

    @Override
    protected String getPropertyFile() {
        return PROPERTY_FILE;
    }

    public static Properties get() {
        try {
            if (INSTANCE.get() == null) {
                INSTANCE.compareAndSet(null, new RuntimeProperties());
            }
            return INSTANCE.get();
        } catch (FalconException e) {
            throw new RuntimeException("Unable to read application " + "runtime properties", e);
        }
    }

    /**
     * Thread for loading properties periodically.
     */
    private final class DynamicLoader implements Runnable {

        private static final long REFRESH_DELAY = 300000L;
        private static final int MAX_ITER = 20;  //1hr
        private final ApplicationProperties applicationProperties;

        private DynamicLoader(ApplicationProperties applicationProperties) {
            this.applicationProperties = applicationProperties;
        }

        @Override
        public void run() {
            long backOffDelay = REFRESH_DELAY;
            while (true) {
                try {
                    try {
                        applicationProperties.loadProperties();
                        backOffDelay = REFRESH_DELAY;
                    } catch (FalconException e) {
                        LOG.warn("Error refreshing runtime properties", e);
                        backOffDelay += REFRESH_DELAY;
                    }
                    Thread.sleep(Math.min(MAX_ITER * REFRESH_DELAY, backOffDelay));
                } catch (InterruptedException e) {
                    LOG.info("Application is stopping. Aborting...");
                    break;
                }
            }
        }
    }
}
