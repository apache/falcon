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

package org.apache.falcon.util;

import junit.framework.Assert;

import org.testng.annotations.Test;

public class ResourcesReflectionUtilTest {
	
	@Test
	public void testMonitoredResources(){
		Assert.assertEquals("submit",ResourcesReflectionUtil.getResourceMonitorName("SchedulableEntityManagerProxy.submit"));
		Assert.assertEquals("entityType", ResourcesReflectionUtil.getResourceDimensionsName("SchedulableEntityManagerProxy.submit").get(1));
		
		Assert.assertEquals("submitAndSchedule",ResourcesReflectionUtil.getResourceMonitorName("SchedulableEntityManagerProxy.submitAndSchedule"));
		Assert.assertEquals("entityType", ResourcesReflectionUtil.getResourceDimensionsName("SchedulableEntityManagerProxy.submit").get(1));
		
		Assert.assertEquals("kill-instance",ResourcesReflectionUtil.getResourceMonitorName("InstanceManagerProxy.killInstance"));
		Assert.assertEquals("entityType", ResourcesReflectionUtil.getResourceDimensionsName("InstanceManagerProxy.killInstance").get(1));
		
		Assert.assertEquals("wf-instance-failed",ResourcesReflectionUtil.getResourceMonitorName("GenericAlert.instrumentFailedInstance"));
		Assert.assertEquals(new Integer(10), ResourcesReflectionUtil.getResourceTimeTakenName("GenericAlert.instrumentFailedInstance"));
		Assert.assertEquals(null, ResourcesReflectionUtil.getResourceTimeTakenName("GenericAlert.alertLateRerunFailed"));
	}
}
