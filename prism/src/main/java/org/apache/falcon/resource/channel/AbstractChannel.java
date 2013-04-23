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

package org.apache.falcon.resource.channel;

import org.apache.falcon.FalconException;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A base class for Channel.
 */
public abstract class AbstractChannel implements Channel {

    private final ConcurrentHashMap<MethodKey, Method> methods = new ConcurrentHashMap<MethodKey, Method>();

    protected Method getMethod(Class service, String methodName, Object... args)
        throws FalconException {
        MethodKey methodKey = new MethodKey(methodName, args);
        Method method = methods.get(methodKey);
        if (method == null) {
            for (Method item : service.getDeclaredMethods()) {
                MethodKey itemKey = new MethodKey(item.getName(),
                        item.getParameterTypes());
                if (methodKey.equals(itemKey)) {
                    methods.putIfAbsent(methodKey, item);
                    return item;
                }
            }
            throw new FalconException("Lookup for " + methodKey
                    + " in service " + service.getName() + " found no match");
        }
        return method;
    }
}
