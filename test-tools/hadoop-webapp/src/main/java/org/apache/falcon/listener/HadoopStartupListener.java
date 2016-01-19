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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Listener for bootstrapping embedded hadoop cluster for integration tests.
 */
public class HadoopStartupListener implements ServletContextListener {
    private static final Logger LOG = LoggerFactory.getLogger(HadoopStartupListener.class);
    private BrokerService broker;
    private final String shareLibPath = "target/share/lib";
    private static final String SHARE_LIB_PREFIX = "lib_";
    private static final String USER = System.getProperty("user.name");

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        try {
            copyShareLib();
            startBroker();
            startHiveMetaStore();
        } catch (Exception e) {
            LOG.error("Unable to start daemons", e);
            throw new RuntimeException("Unable to start daemons", e);
        }
    }

    private void copyShareLib() throws Exception {
        Path shareLibFSPath = new Path(getShareLibPath() +  File.separator + SHARE_LIB_PREFIX
                + getTimestampDirectory());
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(shareLibFSPath.toUri(), conf);
        if (!fs.exists(shareLibFSPath)) {
            fs.mkdirs(shareLibFSPath);
        }

        String[] actionDirectories = getLibActionDirectories();
        for(String actionDirectory : actionDirectories) {
            LOG.info("Copying Action Directory: {}", actionDirectory);
            fs.copyFromLocalFile(new Path(shareLibPath, actionDirectory), shareLibFSPath);
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

    private String getShareLibPath() {
        return File.separator + "user" + File.separator + USER + File.separator + "share/lib";
    }

    private String getTimestampDirectory() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        Date date = new Date();
        return dateFormat.format(date).toString();
    }

    private String[] getLibActionDirectories() {
        StringBuilder libActionDirectories = new StringBuilder();
        File f = new File(shareLibPath);

        if (f != null) {
            File[] files = f.listFiles();
            if (files != null) {
                for (File libDir : files) {
                    if (libDir != null && libDir.isDirectory()) {
                        libActionDirectories.append(libDir.getName()).append("\t");
                    }
                }
            }
        }
        String actionDirectories = libActionDirectories.toString();
        return (actionDirectories).substring(0, actionDirectories.lastIndexOf('\t'))
                .split("\t");
    }
}
