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

package org.apache.falcon.regression.Entities;

import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.log4j.Logger;
import org.testng.Assert;

import javax.xml.bind.Unmarshaller;
import java.io.StringReader;
import java.util.UUID;

/**
 * Util class for merlin entities.
 */
final class TestEntityUtil {

    private static final Logger LOGGER = Logger.getLogger(TestEntityUtil.class);
    private TestEntityUtil() {
        throw new AssertionError("Instantiating utility class...");
    }

    public static Entity fromString(EntityType type, String str) {
        try {
            Unmarshaller unmarshaller = type.getUnmarshaller();
            unmarshaller.setSchema(null);
            return (Entity) unmarshaller.unmarshal(new StringReader(str));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /*
    Deprecating entity name if is_deprecate is set.
    Useful when oozie uses embedded database(derby)
     */
    public static String generateUniqueName(String prefix, String oldName) {
        Assert.assertNotNull(prefix, "name prefix shouldn't be null!");
        String randomPart = UUID.randomUUID().toString().split("-")[0];
        if (MerlinConstants.IS_DEPRECATE) {
            return  'A' + Integer.toHexString(prefix.hashCode()) + '-' + randomPart;
        } else {
            return prefix + '-' + oldName + '-' + randomPart;
        }
    }
}
