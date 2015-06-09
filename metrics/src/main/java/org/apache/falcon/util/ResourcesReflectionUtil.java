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

import org.apache.falcon.monitors.Alert;
import org.apache.falcon.monitors.Monitored;
import org.apache.falcon.monitors.TimeTaken;


import java.awt.Dimension;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Builds a cached of methods annotated with Monitored and params of methods
 * annotated with Dimension.
 */
public final class ResourcesReflectionUtil {

    private static final Map<String, MethodAnnotation> METHODS = new HashMap<String, MethodAnnotation>();

    private ResourcesReflectionUtil() { }

    static {
        //TODO load these classes from properties file
        buildAnnotationsMapForClass("org.apache.falcon.resource.proxy.SchedulableEntityManagerProxy");
        buildAnnotationsMapForClass("org.apache.falcon.resource.proxy.InstanceManagerProxy");
        buildAnnotationsMapForClass("org.apache.falcon.resource.AbstractInstanceManager");
        buildAnnotationsMapForClass("org.apache.falcon.messaging.JMSMessageConsumer");
        buildAnnotationsMapForClass("org.apache.falcon.aspect.GenericAlert");
    }

    public static Map<Integer, String> getResourceDimensionsName(String methodName) {
        return METHODS.get(methodName) != null
                ? Collections.unmodifiableMap(METHODS.get(methodName).params) : null;
    }

    public static String getResourceMonitorName(String methodName) {
        return METHODS.get(methodName) != null ? METHODS.get(methodName).monitoredName : null;
    }

    public static Integer getResourceTimeTakenName(String methodName) {
        return METHODS.get(methodName) != null ? METHODS.get(methodName).timeTakenArgIndex : null;
    }

    /**
     * Holder class for method annotation.
     */
    public static class MethodAnnotation {
        private String monitoredName;
        // argument <index,DimensionValue>
        private Map<Integer, String> params = new HashMap<Integer, String>();

        //to override time taken by an api
        private Integer timeTakenArgIndex;

        @Override
        public String toString() {
            return "{" + monitoredName + "[" + params.toString() + "]" + "}";
        }

    }

    private static void buildAnnotationsMapForClass(String className) {
        Class clazz;
        try {
            clazz = ResourcesReflectionUtil.class.getClassLoader().loadClass(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Unable to load class " + className, e);
        }

        Method[] declMethods = clazz.getMethods();

        // scan every method
        for (Method declMethod : declMethods) {
            Annotation[] methodAnnots = declMethod.getDeclaredAnnotations();
            // scan every annotation on method
            for (Annotation methodAnnot : methodAnnots) {
                final String simpleName = methodAnnot.annotationType().getSimpleName();
                if (simpleName.equals(Monitored.class.getSimpleName())
                        || simpleName.equals(Alert.class.getSimpleName())) {
                    MethodAnnotation annotation = new MethodAnnotation();
                    annotation.monitoredName = getAnnotationValue(methodAnnot, "event");
                    Annotation[][] paramAnnots = declMethod.getParameterAnnotations();

                    // scan every param
                    annotation.params = getDeclaredParamAnnots(paramAnnots, annotation);
                    METHODS.put(clazz.getSimpleName() + "." + declMethod.getName(), annotation);
                }
            }
        }
    }

    private static Map<Integer, String> getDeclaredParamAnnots(
            Annotation[][] paramAnnots, MethodAnnotation annotation) {
        Map<Integer, String> params = new HashMap<Integer, String>();
        for (int i = 0; i < paramAnnots.length; i++) {
            for (int j = 0; j < paramAnnots[i].length; j++) {
                if (paramAnnots[i][j].annotationType().getSimpleName()
                        .equals(Dimension.class.getSimpleName())) {
                    params.put(i, getAnnotationValue(paramAnnots[i][j], "value"));
                }
                if (paramAnnots[i][j].annotationType().getSimpleName()
                        .equals(TimeTaken.class.getSimpleName())) {
                    annotation.timeTakenArgIndex = i;
                }
            }
        }

        return params;
    }

    private static String getAnnotationValue(Annotation annotation,
                                             String attributeName) {
        String value = null;

        if (annotation != null) {
            try {
                value = (String) annotation.annotationType()
                        .getMethod(attributeName).invoke(annotation);
            } catch (Exception ignore) {
                ignore.printStackTrace();
            }
        }

        return value;
    }
}
