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

package org.apache.falcon.regression.core.enumsAndConstants;

import org.apache.falcon.regression.core.util.Config;
import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.apache.log4j.Logger;

import java.util.HashMap;

/**
 * Class for test constants.
 */
public final class MerlinConstants {
    private MerlinConstants() {
    }

    private static final Logger LOGGER = Logger.getLogger(MerlinConstants.class);

    public static final String PRISM_URL = Config.getProperty("prism.hostname");

    public static final String HELP_URL =
        Config.getProperty("falcon.help.url", "http://falcon.apache.org/");

    public static final boolean IS_SECURE =
        "kerberos".equals(new Configuration().get("hadoop.security.authentication", "simple"));

    /** Staging location to use in cluster xml. */
    public static final String STAGING_LOCATION = Config.getProperty("merlin.staging.location",
        "/tmp/falcon-regression-staging");
    /** Working location to use in cluster xml. */
    public static final String WORKING_LOCATION = Config.getProperty("merlin.working.location",
        "/tmp/falcon-regression-working");
    public static final String TEMP_LOCATION = Config.getProperty("merlin.temp.location", "/tmp");

    public static final String OOZIE_EXAMPLE_LIB = Config.getProperty("merlin.oozie_example_lib",
            "https://repo1.maven.org/maven2/org/apache/oozie/oozie-examples/4.1.0/oozie-examples-4.1.0.jar");

    /** the user that is going to run tests. */
    public static final String CURRENT_USER_NAME = Config.getProperty("current_user_name",
        System.getProperty("user.name"));
    /** keytab of current user. */
    private static final String CURRENT_USER_KEYTAB_STR = "current_user_keytab";
    /** group of the current user. */
    public static final String CURRENT_USER_GROUP =
        Config.getProperty("current_user.group.name", "users");

    /** a user that does not belong to the group of current user. */
    public static final String DIFFERENT_USER_NAME = Config.getProperty("other.user.name", "root");

    /** a user that does not belong to the group of current user. */
    public static final String DIFFERENT_USER_GROUP = Config.getProperty("other.user.group", "root");

    /** falcon super user. */
    public static final String FALCON_SUPER_USER_NAME =
            Config.getProperty("falcon.super.user.name", "falcon");

    /** a user that belongs to falcon super user group but is not FALCON_SUPER_USER_NAME. */
    public static final String FALCON_SUPER_USER2_NAME =
            Config.getProperty("falcon.super.user2.name", "falcon2");
    /** a user that has same group as that of current user. */
    private static final String USER_2_NAME_STR = "user2_name";
    private static final String USER_2_KEYTAB_STR = "user2_keytab";
    public static final String USER2_NAME;
    private static HashMap<String, String> keyTabMap;
    private static HashMap<String, String> passwordMap;
    public static final String USER_REALM = Config.getProperty("USER.REALM", "");
    public static final String WASB_CONTAINER = Config.getProperty("wasb.container", "");
    public static final String WASB_SECRET = Config.getProperty("wasb.secret", "");
    public static final String WASB_ACCOUNT  = Config.getProperty("wasb.account", "");

    public static final boolean CLEAN_TESTS_DIR =
        Boolean.valueOf(Config.getProperty("clean_tests_dir", "true"));

    public static final boolean IS_DEPRECATE=
            Boolean.valueOf(Config.getProperty("is_deprecate", "false"));

    /* initialize keyTabMap */
    static {
        final String currentUserKeytab = Config.getProperty(CURRENT_USER_KEYTAB_STR);
        final String user2Name = Config.getProperty(USER_2_NAME_STR);
        final String user2Keytab = Config.getProperty(USER_2_KEYTAB_STR);
        LOGGER.info("CURRENT_USER_NAME: " + CURRENT_USER_NAME);
        LOGGER.info("currentUserKeytab: " + currentUserKeytab);
        LOGGER.info("user2Name: " + user2Name);
        LOGGER.info("user2Keytab: " + user2Keytab);
        USER2_NAME = user2Name;
        keyTabMap = new HashMap<>();
        keyTabMap.put(CURRENT_USER_NAME, currentUserKeytab);
        keyTabMap.put(user2Name, user2Keytab);
        keyTabMap.put(FALCON_SUPER_USER_NAME, Config.getProperty("falcon.super.user.keytab"));
        keyTabMap.put(FALCON_SUPER_USER2_NAME, Config.getProperty("falcon.super.user2.keytab"));
        keyTabMap.put(DIFFERENT_USER_NAME, Config.getProperty("other.user.keytab"));
        passwordMap = new HashMap<>();
        passwordMap.put(DIFFERENT_USER_NAME, Config.getProperty("other.user.password"));
    }

    public static String getKeytabForUser(String user) {
        Assert.assertTrue(keyTabMap.containsKey(user), "Unknown user: " + user);
        return keyTabMap.get(user);
    }

    public static String getPasswordForUser(String user) {
        Assert.assertTrue(passwordMap.containsKey(user), "Unknown user: " + user);
        return passwordMap.get(user);
    }
}
