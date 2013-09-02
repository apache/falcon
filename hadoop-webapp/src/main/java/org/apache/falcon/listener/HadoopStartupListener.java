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

import java.io.File;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.activemq.broker.BrokerService;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

/**
 * Listener for bootstrapping embedded hadoop cluster for integration tests.
 */
public class HadoopStartupListener implements ServletContextListener {
    private static final Logger LOG = Logger.getLogger(HadoopStartupListener.class);
    private BrokerService broker;

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        try {
            FileUtils.deleteDirectory(new File(System.getProperty("hadoop.tmp.dir")));
            final Configuration conf = new Configuration();

            NameNode.format(conf);
            final String[] emptyArgs = {};
            String hadoopProfle = System.getProperty("hadoop.profile", "1");
            if (hadoopProfle.equals("1")) {
                NameNode.createNameNode(emptyArgs, conf);
                DataNode.createDataNode(emptyArgs, conf);
                JobConf jobConf = new JobConf(conf);
                /**
                 * Reflection code:
                 * JobTracker jt = JobTracker.startTracker(jobConf);
                 * jt.offerService();
                 * TaskTracker tt = new TaskTracker(jobConf);
                 * tt.run();
                 */
                Object jt = Class.forName("org.apache.hadoop.mapred.JobTracker")
                                .getMethod("startTracker", JobConf.class).invoke(null, jobConf);
                startService(jt, "offerService");
                Object tt = Class.forName("org.apache.hadoop.mapred.TaskTracker")
                                .getConstructor(JobConf.class).newInstance(jobConf);
                startService(tt, "run");
            } else if (hadoopProfle.equals("2")) {
                /**
                 * Reflection code:
                 * DefaultMetricsSystem.setMiniClusterMode(true);
                 * ResourceManager resourceManager = new ResourceManager(new MemStore());
                 * YarnConfiguration yarnConf = new YarnConfiguration(conf);
                 * resourceManager.init(yarnConf);
                 * resourceManager.start();
                 * NodeManager nodeManager = new NodeManager();
                 * nodeManager.init(yarnConf);
                 * nodeManager.start();
                 */
                Class.forName("org.apache.hadoop.metrics2.lib.DefaultMetricsSystem")
                                .getMethod("setMiniClusterMode", boolean.class).invoke(null, true);
                NameNode.createNameNode(emptyArgs, conf);
                DataNode.createDataNode(emptyArgs, conf);

                Object memStore = instance("org.apache.hadoop.yarn.server.resourcemanager.recovery.MemStore");
                Object resourceManager = Class.forName("org.apache.hadoop.yarn.server.resourcemanager.ResourceManager")
                        .getConstructor(Class.forName("org.apache.hadoop.yarn.server.resourcemanager.recovery.Store"))
                        .newInstance(memStore);
                Object yarnConf = Class.forName("org.apache.hadoop.yarn.conf.YarnConfiguration")
                        .getConstructor(Configuration.class).newInstance(conf);
                invoke(resourceManager, "init", Configuration.class, yarnConf);
                startService(resourceManager, "start");
                Object nodeManager = instance("org.apache.hadoop.yarn.server.nodemanager.NodeManager");
                invoke(nodeManager, "init", Configuration.class, yarnConf);
                startService(nodeManager, "start");
            } else {
                throw new RuntimeException("Unhandled hadoop profile " + hadoopProfle);
            }
            startBroker();
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Unable to start hadoop cluster", e);
            throw new RuntimeException("Unable to start hadoop cluster", e);
        }
    }

    private void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setUseJmx(false);
        broker.setDataDirectory("target/data");
        broker.addConnector("vm://localhost");
        broker.addConnector("tcp://localhost:61616");
        broker.start();
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
