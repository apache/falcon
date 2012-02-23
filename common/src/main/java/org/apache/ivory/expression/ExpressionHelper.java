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

package org.apache.ivory.expression;

import javax.servlet.jsp.el.ELException;
import javax.servlet.jsp.el.FunctionMapper;
import javax.servlet.jsp.el.VariableResolver;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

public final class ExpressionHelper implements FunctionMapper, VariableResolver {

    private static final ExpressionHelper instance = new ExpressionHelper();

    public static ExpressionHelper get() {
        return instance;
    }

    private ExpressionHelper() {}

    @Override
    public Method resolveFunction(String prefix, String name) {
        for (Method method : ExpressionHelper.class.getDeclaredMethods()) {
            if (method.getName().equals(name))
                return method;
        }
        throw new UnsupportedOperationException("Not found " + prefix + ":" + name);
    }

    @Override
    public Object resolveVariable(String field) throws ELException {
        throw new UnsupportedOperationException("Cant resolve " + field);
    }

    public static long hours(int val) {
        return TimeUnit.HOURS.toMillis(val);
    }

    public static long minutes(int val) {
        return TimeUnit.MINUTES.toMillis(val);
    }

    public static long days(int val) {
        return TimeUnit.DAYS.toMillis(val);
    }

    public static long months(int val) {
        return val * days(31);
    }

    public static long years(int val) {
        return val * days(366);
    }
}
