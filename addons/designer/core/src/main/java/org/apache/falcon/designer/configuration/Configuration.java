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
package org.apache.falcon.designer.configuration;

import java.io.IOException;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * This is the configuration that primitives will use to configure their
 * instance.
 */
public abstract class Configuration<T extends Configuration> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public abstract String getName();

    public abstract String getCategory();

    /**
     * Serialize it from a action type to string type.
     * @param act
     *            actual Action Configuration data
     * @return
     */
    public String serialize() throws SerdeException {
        String returnJsonString;
        try {
            returnJsonString = MAPPER.writeValueAsString(this);
        } catch (JsonGenerationException e) {
            throw new SerdeException("Failed serializing object ", e);
        } catch (JsonMappingException e) {
            throw new SerdeException("Failed serializing object ", e);
        } catch (IOException e) {
            throw new SerdeException("Failed serializing object ", e);
        }
        return returnJsonString;
    }

    /**
     * Deserialize from string to object of Action type.
     * @param actString
     *            actual Data
     * @return
     */
    public T deserialize(String actString) throws SerdeException {
        T returnEmailAction;
        try {
            returnEmailAction = MAPPER.readValue(actString, getConfigClass());
        } catch (JsonParseException e) {
            throw new SerdeException("Failed deserialize string " + actString,
                e);
        } catch (JsonMappingException e) {
            throw new SerdeException("Failed deserialize string " + actString,
                e);
        } catch (IOException e) {
            throw new SerdeException("Failed deserialize string " + actString,
                e);
        }
        return returnEmailAction;
    }

    public abstract Class<T> getConfigClass();
}
