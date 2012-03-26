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

import junit.framework.Assert;

import org.testng.annotations.Test;

public class ResourcesReflectionUtilTest {
	
	@Test
	public void testMonitoredResources(){
		Assert.assertEquals("submit",ResourcesReflectionUtil.getResourceMonitorName("EntityManager.submit"));
		Assert.assertEquals("entityType", ResourcesReflectionUtil.getResourceDimensionsName("EntityManager.submit").get(1));
		
		Assert.assertEquals("submitAndSchedule",ResourcesReflectionUtil.getResourceMonitorName("SchedulableEntityManager.submitAndSchedule"));
		Assert.assertEquals("entityType", ResourcesReflectionUtil.getResourceDimensionsName("EntityManager.submit").get(1));		
		
		Assert.assertEquals("kill",ResourcesReflectionUtil.getResourceMonitorName("ProcessInstanceManager.killProcessInstance"));
		Assert.assertEquals("processName", ResourcesReflectionUtil.getResourceDimensionsName("ProcessInstanceManager.killProcessInstance").get(1));		
	
		Assert.assertEquals("process-instance",ResourcesReflectionUtil.getResourceMonitorName("IvoryTopicSubscriber.instrumentWithAspect"));
		Assert.assertEquals("process", ResourcesReflectionUtil.getResourceDimensionsName("IvoryTopicSubscriber.instrumentWithAspect").get(0));		

	}

}
