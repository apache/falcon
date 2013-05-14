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

import java.util.Arrays;

/**
 * This class represents a tuple of method name and class objects.
 */
public class MethodKey {

    private final String name;
    private final Class[] argClasses;

    public MethodKey(String name, Object[] args) {
        this.name = name;
        argClasses = new Class[args.length];
        for (int index = 0; index < args.length; index++) {
            if (args[index] != null) {
                argClasses[index] = args[index].getClass();
            }
        }
    }

    public MethodKey(String name, Class[] args) {
        this.name = name;
        argClasses = args.clone();
    }

    @Override
    public boolean equals(Object methodRHS) {
        if (this == methodRHS) {
            return true;
        }

        if (methodRHS == null || getClass() != methodRHS.getClass()) {
            return false;
        }

        MethodKey methodKey = (MethodKey) methodRHS;

        if (name != null ? !name.equals(methodKey.name) : methodKey.name != null) {
            return false;
        }

        boolean matching = true;
        for (int index = 0; index < argClasses.length; index++) {
            if (argClasses[index] != null && methodKey.argClasses[index] != null
                    && !methodKey.argClasses[index].isAssignableFrom(argClasses[index])) {
                matching = false;
            }
        }

        return matching;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (argClasses.length);
        return result;
    }

    @Override
    public String toString() {
        return "MethodKey{name='" + name + '\'' + ", argClasses="
                + (argClasses == null ? null : Arrays.asList(argClasses)) + '}';
    }
}
