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

package org.apache.falcon.listener;

import org.apache.activemq.broker.BrokerService;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * Listener for bootstrapping embedded hadoop cluster for integration tests.
 */
public class HadoopStartupListener implements ServletContextListener {
    private static final Logger LOG = LoggerFactory.getLogger(HadoopStartupListener.class);
    private BrokerService broker;

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        try {
            startBroker();
            startHiveMetaStore();

        } catch (Exception e) {
            LOG.error("Unable to start daemons", e);
            throw new RuntimeException("Unable to start daemons", e);
        }
    }

    private void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setDataDirectory("target/data");
        broker.addConnector("vm://localhost");
        broker.addConnector("tcp://0.0.0.0:61616");
        broker.start();
    }

    public static final String META_STORE_PORT = "49083";
    private void startHiveMetaStore() {
        try {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        String[] args = new String[]{
                            "-v",
                            "-p", META_STORE_PORT,
                        };

                        HiveMetaStore.main(args);
                    } catch (Throwable t) {
                        throw new RuntimeException(t);
                    }
                }
            }).start();
        } catch (Exception e) {
            throw new RuntimeException("Unable to start hive metastore server.", e);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        try {
            if (broker != null) {
                broker.stop();
            }
        } catch(Exception e) {
            LOG.warn("Failed to stop activemq", e);
        }
    }
}
