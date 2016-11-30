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
package org.apache.falcon.unit;

import org.apache.commons.io.FileUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.hadoop.JailedFileSystem;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.service.FalconJPAService;
import org.apache.falcon.service.ServiceInitializer;
import org.apache.falcon.tools.FalconStateStoreDBCLI;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.util.StateStoreProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.action.hadoop.LauncherMapper;
import org.apache.oozie.local.LocalOozie;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * FalconUnit runs jobs in an Local Mode and Cluster mode . <p/> Falon Unit is meant for development/debugging purposes
 * only.
 */
public final class FalconUnit {

    private static final Logger LOG = LoggerFactory.getLogger(FalconUnit.class);
    private static final String OOZIE_SITE_XML = "oozie-site.xml";
    private static final String OOZIE_DEFAULT_XML = "oozie-default.xml";
    private static final String STORAGE_URL = "jail://global:00";
    private static final String OOZIE_HOME_DIR = "/tmp/oozie-" + System.getProperty("user.name");

    private static JailedFileSystem jailedFileSystem = new JailedFileSystem();
    private static final ServiceInitializer STARTUP_SERVICES = new ServiceInitializer();
    private static Map<String, String> sysProps;
    private static FalconUnitClient falconUnitClient;
    private static boolean isLocalMode;
    private static boolean isFalconUnitActive = false;

    private static final String DB_BASE_DIR = "target/test-data/persistenceDB";
    protected static final String DB_LOCATION = DB_BASE_DIR + File.separator + "data.db";
    protected static final String URL = "jdbc:derby:"+ DB_LOCATION +";create=true";
    protected static final String DB_SQL_FILE = DB_BASE_DIR + File.separator + "out.sql";
    protected static final  LocalFileSystem LOCAL_FS = new LocalFileSystem();

    private FalconUnit() {
    }


    public static synchronized void start(boolean isLocal) throws Exception {
        if (isFalconUnitActive) {
            throw new IllegalStateException("Falcon Unit is already initialized");
        }
        isLocalMode = isLocal;
        //Initialize Startup and runtime properties
        LOG.info("Initializing startup properties ...");
        StartupProperties.get();

        LOG.info("Initializing runtime properties ...");
        RuntimeProperties.get();

        setupExtensionConfigs();
        //Initializing Services
        STARTUP_SERVICES.initialize();
        ConfigurationStore.get();

        if (isLocalMode) {
            setupOozieConfigs();
            initFileSystem();
        }
        isFalconUnitActive = true;
    }

    public static void setupExtensionConfigs() throws Exception {
        String configPath = new URI(StartupProperties.get().getProperty("config.store.uri")).getPath();
        String location = configPath + "-extensionSore";
        StartupProperties.get().setProperty("config.store.uri", location);
        FileUtils.deleteDirectory(new File(location));
        StateStoreProperties.get().setProperty(FalconJPAService.URL, URL);
        Configuration localConf = new Configuration();
        LOCAL_FS.initialize(LocalFileSystem.getDefaultUri(localConf), localConf);
        LOCAL_FS.mkdirs(new Path(DB_BASE_DIR));
        createDB(DB_SQL_FILE);
    }

    public static void createDB(String file) {
        File sqlFile = new File(file);
        String[] argsCreate = {"create", "-sqlfile", sqlFile.getAbsolutePath(), "-run"};
        new FalconStateStoreDBCLI().run(argsCreate);
    }

    private static void initFileSystem() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", STORAGE_URL);
        jailedFileSystem.initialize(LocalFileSystem.getDefaultUri(conf), conf);
    }

    private static void setupOozieConfigs() throws IOException {
        sysProps = new HashMap<>();
        String oozieHomeDir = OOZIE_HOME_DIR;
        String oozieConfDir = oozieHomeDir + "/conf";
        String oozieHadoopConfDir = oozieConfDir + "/hadoop-conf";
        String oozieActionConfDir = oozieConfDir + "/action-conf";
        String oozieLogsDir = oozieHomeDir + "/logs";
        String oozieDataDir = oozieHomeDir + "/data";

        LOCAL_FS.mkdirs(new Path(oozieHomeDir));
        LOCAL_FS.mkdirs(new Path(oozieConfDir));
        LOCAL_FS.mkdirs(new Path(oozieHadoopConfDir));
        LOCAL_FS.mkdirs(new Path(oozieActionConfDir));
        LOCAL_FS.mkdirs(new Path(oozieLogsDir));
        LOCAL_FS.close();

        setSystemProperty("oozie.home.dir", oozieHomeDir);
        setSystemProperty("oozie.data.dir", oozieDataDir);
        setSystemProperty("oozie.action.conf", oozieActionConfDir);
        setSystemProperty("oozie.log.dir", oozieLogsDir);
        setSystemProperty("oozie.log4j.file", "localoozie-log4j.properties");
        setSystemProperty("oozielocal.log", "oozieLogsDir/oozielocal.log");

        Configuration oozieSiteConf = new Configuration(false);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream oozieSiteInputStream = classLoader.getResourceAsStream(OOZIE_SITE_XML);
        XConfiguration configuration = new XConfiguration(oozieSiteInputStream);
        Properties props = configuration.toProperties();
        for (String propName : props.stringPropertyNames()) {
            oozieSiteConf.set(propName, props.getProperty(propName));
        }
        oozieSiteInputStream.close();

        InputStream oozieDefaultInputStream = classLoader.getResourceAsStream(OOZIE_DEFAULT_XML);
        configuration = new XConfiguration(oozieDefaultInputStream);
        String classes = configuration.get(Services.CONF_SERVICE_CLASSES);
        oozieSiteConf.set(Services.CONF_SERVICE_CLASSES, classes.replaceAll(
                "org.apache.oozie.service.ShareLibService,", ""));
        File target = new File(oozieConfDir, OOZIE_SITE_XML);
        FileOutputStream outStream = null;
        try {
            outStream = new FileOutputStream(target);
            oozieSiteConf.writeXml(outStream);
        } finally {
            if (outStream != null) {
                outStream.close();
            }
        }
        oozieDefaultInputStream.close();

        CurrentUser.authenticate(System.getProperty("user.name"));
    }

    public static synchronized void cleanup() throws Exception {
        STARTUP_SERVICES.destroy();
        if (isLocalMode) {
            cleanUpOozie();
            jailedFileSystem.close();
        }
        isFalconUnitActive = false;
    }

    private static void cleanUpOozie() throws IOException, FalconException {
        LocalOozie.stop();
        FileUtils.deleteDirectory(new File(OOZIE_HOME_DIR));
        // Need to explicitly clean this as Oozie Launcher leaves this behind.
        FileUtils.deleteQuietly(new File(LauncherMapper.PROPAGATION_CONF_XML));
        resetSystemProperties();
        System.setSecurityManager(null);
    }

    public static synchronized FalconUnitClient getClient() throws FalconException {
        if (!isFalconUnitActive) {
            throw new IllegalStateException("Falcon Unit is not initialized");
        }
        if (falconUnitClient == null) {
            falconUnitClient = new FalconUnitClient();
        }
        return falconUnitClient;
    }

    public static FileSystem getFileSystem() throws IOException {
        if (!isFalconUnitActive) {
            throw new IllegalStateException("Falcon Unit is not initialized");
        }
        return jailedFileSystem;
    }

    // Setting System properties and store their actual values
    private static void setSystemProperty(String name, String value) {
        if (!sysProps.containsKey(name)) {
            String currentValue = System.getProperty(name);
            sysProps.put(name, currentValue);
        }
        if (value != null) {
            System.setProperty(name, value);
        } else {
            System.getProperties().remove(name);
        }
    }


    /**
     * Reset changed system properties to their original values.
     */
    private static void resetSystemProperties() {
        if (sysProps != null) {
            for (Map.Entry<String, String> entry : sysProps.entrySet()) {
                if (entry.getValue() != null) {
                    System.setProperty(entry.getKey(), entry.getValue());
                } else {
                    System.getProperties().remove(entry.getKey());
                }
            }
            sysProps.clear();
        }
    }
}
