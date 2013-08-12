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
import org.apache.falcon.util.BuildProperties;
import org.apache.falcon.util.EmbeddedServer;

/**
 * Driver for running Falcon as a standalone server with embedded jetty server.
 */
public final class Main {

    private static final String APP_PATH = "app";
    private static final String EMBEDDED_ACTIVEMQ = "embeddedmq";
    private static final String ACTIVEMQ_BASE = "mqbase";

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

        opt = new Option(EMBEDDED_ACTIVEMQ, true, "Should start embedded activemq?");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option(ACTIVEMQ_BASE, true, "Activemq data directory");
        opt.setRequired(false);
        options.addOption(opt);

        return new GnuParser().parse(options, args);
    }

    public static void main(String[] args) throws Exception {
        CommandLine cmd = parseArgs(args);
        String projectVersion = BuildProperties.get().getProperty("project.version");
        String appPath = "webapp/target/falcon-webapp-" + projectVersion;
        String dataDir = "target/";
        boolean startActiveMq = true;

        if (cmd.hasOption(APP_PATH)) {
            appPath = cmd.getOptionValue(APP_PATH);
        }

        if (cmd.hasOption(EMBEDDED_ACTIVEMQ)) {
            startActiveMq = Boolean.valueOf(cmd.getOptionValue(EMBEDDED_ACTIVEMQ));
        }

        if (cmd.hasOption(ACTIVEMQ_BASE)) {
            dataDir = cmd.getOptionValue(ACTIVEMQ_BASE);
        }

        if (startActiveMq) {
            BrokerService broker = new BrokerService();
            broker.setUseJmx(false);
            broker.setDataDirectory(dataDir);
            broker.addConnector("vm://localhost");
            broker.addConnector("tcp://localhost:61616");
            broker.start();
        }

        EmbeddedServer server = new EmbeddedServer(15000, appPath);
        server.start();
    }
}
