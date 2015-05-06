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

package org.apache.falcon.regression.core.util;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.entity.AbstractEntityHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.resource.EntityList;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * util methods related to conf.
 */
public final class CleanupUtil {
    private CleanupUtil() {
        throw new AssertionError("Instantiating utility class...");
    }
    private static final Logger LOGGER = Logger.getLogger(CleanupUtil.class);

    public static List<String> getAllEntitiesOfOneType(AbstractEntityHelper entityManagerHelper,
                                                       String user) {
        return getEntitiesWithPrefix(entityManagerHelper, user, "");
    }

    public static List<String> getEntitiesWithPrefix(AbstractEntityHelper entityHelper,
                                                       String user, String namePrefix) {
        final EntityList entityList;
        try {
            entityList = getEntitiesResultOfOneType(entityHelper, user);
        } catch (Exception e) {
            LOGGER.error("Caught exception: " + ExceptionUtils.getStackTrace(e));
            return null;
        }
        List<String> entities = new ArrayList<>();
        if (entityList.getElements() != null) {
            for (EntityList.EntityElement entity : entityList.getElements()) {
                if (entity.name.startsWith(namePrefix)) {
                    entities.add(entity.name);
                }
            }
        }
        return entities;
    }

    private static EntityList getEntitiesResultOfOneType(
        AbstractEntityHelper entityManagerHelper, String user)
        throws IOException, URISyntaxException, AuthenticationException, JAXBException,
        InterruptedException {
        final ServiceResponse clusterResponse = entityManagerHelper.listAllEntities(null, user);
        JAXBContext jc = JAXBContext.newInstance(EntityList.class);
        Unmarshaller u = jc.createUnmarshaller();
        return (EntityList) u.unmarshal(
            new StringReader(clusterResponse.getMessage()));
    }



    public static void cleanEntitiesWithPrefix(ColoHelper prism, String namePrefix) {
        final List<String> processes = getEntitiesWithPrefix(prism.getProcessHelper(), null, namePrefix);
        final List<String> feeds = getEntitiesWithPrefix(prism.getFeedHelper(), null, namePrefix);
        final List<String> clusters = getEntitiesWithPrefix(prism.getClusterHelper(), null, namePrefix);

        for (String process : processes) {
            deleteQuietlyByName(prism.getProcessHelper(), process);
        }
        for (String feed : feeds) {
            deleteQuietlyByName(prism.getFeedHelper(), feed);
        }

        for (String cluster : clusters) {
            deleteQuietlyByName(prism.getClusterHelper(), cluster);
        }
    }

    private static void deleteQuietlyByName(AbstractEntityHelper helper, String entityName) {
        try {
            helper.deleteByName(entityName, null);
        } catch (Exception e) {
            LOGGER.info("Caught exception: " + ExceptionUtils.getStackTrace(e));
        }
    }
}
