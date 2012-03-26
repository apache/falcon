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

package org.apache.ivory.aspect;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;

import org.apache.ivory.util.ResourcesReflectionUtil;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;

/**
 * Abstract Ivory Aspect,
 * which intercept methods annotated with Monitored
 * and publishes messages.
 * Subclasses should override publishMessage Method.
 */
@Aspect
public abstract class AbstractIvoryAspect {

	@Around("@annotation(org.apache.ivory.monitors.Monitored)")
	public Object logAround(ProceedingJoinPoint joinPoint) throws Throwable {

		String methodName = joinPoint.getSignature().getName();
		Object[] args = joinPoint.getArgs();
		Object result = null;
		ResourceMessage.Status status;

		long startTime = System.nanoTime();
		long endTime;
		try {
			result = joinPoint.proceed();
		} catch (Exception e) {
			endTime = System.nanoTime();
			status = ResourceMessage.Status.FAILED;
			publishMessage(getResourceMessage(joinPoint.getSignature()
					.getDeclaringType().getSimpleName()
					+ "." + methodName, args, status, endTime - startTime));
			throw e;
		}
		endTime = System.nanoTime();
		status = ResourceMessage.Status.SUCCEEDED;
		publishMessage(getResourceMessage(joinPoint.getSignature().getDeclaringType()
				.getSimpleName()
				+ "." + methodName, args, status, endTime - startTime));
		return result;
	}

	private ResourceMessage getResourceMessage(String methodName,
			Object[] args, ResourceMessage.Status status, long executionTime) {
		String action = ResourcesReflectionUtil
				.getResourceMonitorName(methodName);

		assert action != null : "Method :" + methodName
				+ " not parsed by reflection util";
		Map<String, String> dimensions = new HashMap<String, String>();

		for (Map.Entry<Integer, String> param : ResourcesReflectionUtil
				.getResourceDimensionsName(methodName).entrySet()) {
			dimensions.put(param.getValue(), args[param.getKey()].toString());
		}

		return new ResourceMessage(action, dimensions, status, executionTime);

	}

	private String getClassAnnotationValue(Class classType,
			Class annotationType, String attributeName) {
		String value = null;

		Annotation annotation = classType.getAnnotation(annotationType);
		if (annotation != null) {
			try {
				value = (String) annotation.annotationType()
						.getMethod(attributeName).invoke(annotation);
			} catch (Exception ex) {
			}
		}

		return value;
	}

	abstract public void publishMessage(ResourceMessage message);
}
