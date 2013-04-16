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

import org.apache.falcon.entity.ColoClusterRelation;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Set;

public class DeploymentUtil {
    private static final Logger LOG = Logger.getLogger(DeploymentUtil.class);

    protected static final String DEFAULT_COLO = "default";
    protected static final String EMBEDDED = "embedded";
    protected static final String DEPLOY_MODE = "deploy.mode";
    private static final Set<String> DEFAULT_ALL_COLOS = new HashSet<String>();

    protected final static String currentColo;
    protected final static boolean embeddedMode;
    protected static boolean prism = false;

    static {
        DEFAULT_ALL_COLOS.add(DEFAULT_COLO);
        embeddedMode = DeploymentProperties.get().
                getProperty(DEPLOY_MODE, EMBEDDED).equals(EMBEDDED);
        if (embeddedMode) {
            currentColo = DEFAULT_COLO;
        } else {
            currentColo = StartupProperties.get().
                    getProperty("current.colo", DEFAULT_COLO);
        }
        LOG.info("Running in embedded mode? " + embeddedMode);
        LOG.info("Current colo: " + currentColo);
    }

    public static void setPrismMode() {
        prism = true;
    }

    public static boolean isPrism() {
        return !embeddedMode && prism;
    }

    public static String getCurrentColo() {
        return currentColo;
    }

    public static Set<String> getCurrentClusters() {
        String colo = getCurrentColo();
        return ColoClusterRelation.get().getClusters(colo);
    }

    public static boolean isEmbeddedMode() {
        return embeddedMode;
    }

    public static String getDefaultColo() {
        return DEFAULT_COLO;
    }

    public static Set<String> getDefaultColos() {
        DEFAULT_ALL_COLOS.add(DEFAULT_COLO);
        return DEFAULT_ALL_COLOS;
    }
}
