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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.falcon.util.StartupProperties;

/**
 * Driver for running Falcon as a standalone server with embedded jetty server.
 */
public final class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static final String APP_PATH = "app";
    private static final String APP_PORT = "port";

    /**
     * Prevent users from constructing this.
     */
    private Main() {
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

        return new GnuParser().parse(options, args);
    }

    public static void main(String[] args) throws Exception {
        CommandLine cmd = parseArgs(args);
        String projectVersion = BuildProperties.get().getProperty("project.version");
        String appPath = "webapp/target/falcon-webapp-" + projectVersion;

        if (cmd.hasOption(APP_PATH)) {
            appPath = cmd.getOptionValue(APP_PATH);
        }

        final String enableTLSFlag = StartupProperties.get().getProperty("falcon.enableTLS");
        final int appPort = getApplicationPort(cmd, enableTLSFlag);
        final boolean enableTLS = isTLSEnabled(enableTLSFlag, appPort);
        StartupProperties.get().setProperty("falcon.enableTLS", String.valueOf(enableTLS));

        startEmbeddedMQIfEnabled();

        LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        LOG.info("Server starting with TLS ? {} on port {}", enableTLS, appPort);
        LOG.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        EmbeddedServer server = EmbeddedServer.newServer(appPort, appPath, enableTLS);
        server.start();
    }

    private static int getApplicationPort(CommandLine cmd, String enableTLSFlag) {
        final int appPort;
        if (cmd.hasOption(APP_PORT)) {
            appPort = Integer.valueOf(cmd.getOptionValue(APP_PORT));
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
            int mqport = Integer.valueOf(System.getProperty("falcon.embeddedmq.port", "61616"));
            LOG.info("Starting ActiveMQ at port {} with data dir {}", mqport, dataDir);

            BrokerService broker = new BrokerService();
            broker.setUseJmx(false);
            broker.setDataDirectory(dataDir);
            broker.addConnector("vm://localhost");
            broker.addConnector("tcp://0.0.0.0:" + mqport);
            broker.setSchedulerSupport(true);
            broker.start();
        }
    }
}
