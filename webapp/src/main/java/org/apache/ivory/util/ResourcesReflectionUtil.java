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

import java.awt.Dimension;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.ivory.monitors.Monitored;
import org.apache.ivory.resource.EntityManager;
import org.apache.ivory.resource.ProcessInstanceManager;
import org.apache.ivory.resource.SchedulableEntityManager;

/**
 * Builds a cached of methods annotated with Monitored and params of methods
 * annotated with Dimension.
 * 
 */
public class ResourcesReflectionUtil {

	private static final Map<String, MethodAnnotation> methods = new HashMap<String, MethodAnnotation>();
	
	private ResourcesReflectionUtil(){
		
	}

	static {
		//TODO load these classes from properties file
		buildAnnotationsMapForClass(EntityManager.class);
		buildAnnotationsMapForClass(SchedulableEntityManager.class);
		buildAnnotationsMapForClass(ProcessInstanceManager.class);
	}

	public static Map<Integer, String> getResourceDimensionsName(String methodName) {
		return methods.get(methodName)!=null?Collections.unmodifiableMap(methods.get(methodName).params):null;
	}

	public static String getResourceMonitorName(String methodName) {
		return methods.get(methodName)!=null?methods.get(methodName).monitoredName:null;
	}

	public static class MethodAnnotation {
		private String monitoredName;
		// argument <index,DimensionValue>
		private Map<Integer, String> params = new HashMap<Integer, String>();
		
		@Override
		public String toString() {
			return "{" + monitoredName + "[" + params.toString() + "]" + "}";
		}

	}

	private static void buildAnnotationsMapForClass(
			Class<? extends EntityManager> clazz) {
		Method[] declMethods = clazz.getMethods();

		// scan every method
		for (int i = 0; i < declMethods.length; i++) {
			Annotation[] methodAnnots = declMethods[i].getDeclaredAnnotations();
			// scan every annotation on method
			for (int j = 0; j < methodAnnots.length; j++) {
				if (methodAnnots[j].annotationType().getSimpleName()
						.equals(Monitored.class.getSimpleName())) {
					MethodAnnotation annotation = new MethodAnnotation();
					annotation.monitoredName = getAnnotationValue(
							methodAnnots[j], "event");
					Annotation[][] paramAnnots = declMethods[i]
							.getParameterAnnotations();
					// scan every param
					annotation.params = getDeclaredParamAnnots(paramAnnots);
					methods.put(
							clazz.getSimpleName() + "."
									+ declMethods[i].getName(), annotation);
				}

			}
		}
	}

	private static Map<Integer, String> getDeclaredParamAnnots(
			Annotation[][] paramAnnots) {
		Map<Integer, String> params = new HashMap<Integer, String>();
		for (int i = 0; i < paramAnnots.length; i++) {
			for (int j = 0; j < paramAnnots[i].length; j++) {
				if (paramAnnots[i][j].annotationType().getSimpleName()
						.equals(Dimension.class.getSimpleName())) {
					params.put(Integer.valueOf(i),
							getAnnotationValue(paramAnnots[i][j], "value"));
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
