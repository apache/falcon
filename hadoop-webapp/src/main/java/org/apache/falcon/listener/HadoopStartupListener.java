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
import org.apache.falcon.JobTrackerService;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.log4j.Logger;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * Listener for bootstrapping embedded hadoop cluster for integration tests.
 */
public class HadoopStartupListener implements ServletContextListener {
    private static final Logger LOG = Logger.getLogger(HadoopStartupListener.class);
    private BrokerService broker;

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        try {
            startLocalJobRunner();

            startBroker();
            startHiveMetaStore();

        } catch (Exception e) {
            LOG.error("Unable to start daemons", e);
            throw new RuntimeException("Unable to start daemons", e);
        }
    }

    @SuppressWarnings("unchecked")
    private void startLocalJobRunner() throws Exception {
        String className = "org.apache.hadoop.mapred.LocalRunnerV1";
        try {
            Class<? extends JobTrackerService>  runner = (Class<? extends JobTrackerService>) Class.forName(className);
            JobTrackerService service = runner.newInstance();
            service.start();
        } catch (ClassNotFoundException e) {
            LOG.warn("v1 Hadoop components not found. Assuming v2", e);
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

    private Object instance(String clsName) throws Exception {
        return Class.forName(clsName).newInstance();
    }

    @SuppressWarnings("rawtypes")
    private void invoke(Object service, String methodName, Class argCls, Object arg) throws Exception {
        if (argCls == null) {
            service.getClass().getMethod(methodName).invoke(service);
        } else {
            service.getClass().getMethod(methodName, argCls).invoke(service, arg);
        }
    }

    private void startService(final Object service, final String method) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    LOG.info("Starting service " + service.getClass().getName());
                    invoke(service, method, null, null);
                } catch(Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }).start();
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
