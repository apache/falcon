/*
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

package org.apache.ivory.util;

import java.lang.reflect.Method;

import org.apache.ivory.IvoryException;

public final class ReflectionUtils {

    public static <T> T getInstance(String classKey) throws IvoryException {
        String clazzName = StartupProperties.get().getProperty(classKey);
        try {
            return getInstanceByClassName(clazzName);
        } catch (IvoryException e) {
            throw new IvoryException("Unable to get instance for key: " + classKey, e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T getInstanceByClassName(String clazzName) throws IvoryException {
        try {
            Class<T> clazz = (Class<T>) ReflectionUtils.class.getClassLoader().loadClass(clazzName);
            try {
                return clazz.newInstance();
            } catch (IllegalAccessException e) {
                Method method = clazz.getMethod("get");
                return (T) method.invoke(null);
            }
        } catch (Exception e) {
            throw new IvoryException("Unable to get instance for " + clazzName, e);
        }
    }
}
