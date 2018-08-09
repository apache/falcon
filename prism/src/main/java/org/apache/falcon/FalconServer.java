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

package org.apache.falcon;

import org.apache.activemq.broker.BrokerService;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.util.BuildProperties;
import org.apache.falcon.util.EmbeddedServer;
import org.apache.falcon.util.StartupProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Driver for running Falcon as a standalone server with embedded jetty server.
 */
public final class FalconServer {
    private static final Logger LOG = LoggerFactory.getLogger(FalconServer.class);
    private static final String APP_PATH = "app";
    private static final String APP_PORT = "port";
    private static final String SAFE_MODE = "setsafemode";

    private static EmbeddedServer server;
    private static BrokerService broker;

    /**
     * Prevent users from constructing this.
     */
    private FalconServer() {
    }

    private static CommandLine parseArgs(String[] args) throws ParseException {
        Options options = new Options();
        Option opt;

        opt = new Option(APP_PATH, true, "Application Path");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(APP_PORT, true, "Application Port");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(SAFE_MODE, true, "Application mode, start safemode if true");
        opt.setRequired(false);
        options.addOption(opt);

        return new GnuParser().parse(options, args);
    }

    static class ShutDown extends Thread {
        public void run() {
            try {
                LOG.info("calling shutdown hook");
                if (server != null) {
                    server.stop();
                }
                if (broker != null) {
                    broker.stop();
                }
                LOG.info("Shutdown Complete.");
            } catch (Exception e) {
                LOG.error("Server shutdown failed with " , e);
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Runtime.getRuntime().addShutdownHook(new ShutDown());
        CommandLine cmd = parseArgs(args);
        String projectVersion = BuildProperties.get().getProperty("project.version");
        String appPath = "webapp/target/falcon-webapp-" + projectVersion;

        if (cmd.hasOption(APP_PATH)) {
            appPath = cmd.getOptionValue(APP_PATH);
        }

        if (cmd.hasOption(SAFE_MODE)) {
            validateSafemode(cmd.getOptionValue(SAFE_MODE));
            boolean isSafeMode = Boolean.parseBoolean(cmd.getOptionValue(SAFE_MODE));
            if (isSafeMode) {
                StartupProperties.createSafemodeFile();
                LOG.info("Falcon is starting in safemode.");
            } else {
                StartupProperties.deleteSafemodeFile();
            }
            StartupProperties.get().setProperty(StartupProperties.SAFEMODE_PROPERTY, Boolean.toString(isSafeMode));
        }

        final String enableTLSFlag = StartupProperties.get().getProperty("falcon.enableTLS");
        final int appPort = getApplicationPort(cmd, enableTLSFlag);
        final boolean enableTLS = isTLSEnabled(enableTLSFlag, appPort);
        StartupProperties.get().setProperty("falcon.enableTLS", String.valueOf(enableTLS));

        startEmbeddedMQIfEnabled();

        LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        LOG.info("Server starting with TLS ? {} on port {}", enableTLS, appPort);
        LOG.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        server = EmbeddedServer.newServer(appPort, appPath, enableTLS);
        server.start();
    }

    private static void validateSafemode(String isSafeMode) throws Exception {
        if (!("true".equals(isSafeMode) || "false".equals(isSafeMode))) {
            throw new Exception("Invalid value for argument safemode. Allowed values are \"true\" or \"false\"");
        }
    }

    private static int getApplicationPort(CommandLine cmd, String enableTLSFlag) {
        final int appPort;
        if (cmd.hasOption(APP_PORT)) {
            appPort = Integer.parseInt(cmd.getOptionValue(APP_PORT));
        } else {
            // default : falcon.enableTLS is true
            appPort = StringUtils.isEmpty(enableTLSFlag)
                    || enableTLSFlag.equals("true") ? 15443 : 15000;
        }

        return appPort;
    }

    private static boolean isTLSEnabled(String enableTLSFlag, int appPort) {
        return Boolean.valueOf(StringUtils.isEmpty(enableTLSFlag)
                ? System.getProperty("falcon.enableTLS", (appPort % 1000) == 443 ? "true" : "false")
                : enableTLSFlag);
    }

    private static void startEmbeddedMQIfEnabled() throws Exception {
        boolean startActiveMq = Boolean.valueOf(System.getProperty("falcon.embeddedmq", "true"));
        if (startActiveMq) {
            String dataDir = System.getProperty("falcon.embeddedmq.data", "target/");
            int mqport = Integer.parseInt(System.getProperty("falcon.embeddedmq.port", "61616"));
            LOG.info("Starting ActiveMQ at port {} with data dir {}", mqport, dataDir);

            broker = new BrokerService();
            broker.setUseJmx(false);
            broker.setDataDirectory(dataDir);
            broker.addConnector("vm://localhost");
            broker.addConnector("tcp://0.0.0.0:" + mqport);
            broker.setSchedulerSupport(true);
            broker.setUseShutdownHook(false);
            broker.start();
        }
    }
}
