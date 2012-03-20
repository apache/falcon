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

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.ivory.resource.EntityManager;
import org.testng.annotations.Test;

public class LoggingAspectTest {

	private EntityManager em = new EntityManager();
	private volatile Exception threadException;

	@Test
	public void testBeanLoading() {

		String result = em.getStatus("cluster", "corp");
		Assert.assertEquals("NOT_FOUND", result);
	}

	@Test
	public void testExceptionBeanLoading() {
		try {
			em.getStatus("cluster123", "corp");
			System.out.println();
			
		} catch (Exception e) {
			return;
		}
		Assert.fail("Exepected excpetion");
	}
	
	@Test
	public void testConcurrentRequests() throws Exception{
        List<Thread> threadList = new ArrayList<Thread>();
        for (int i = 0; i < 5; i++) {
            threadList.add(new Thread() {
                public void run() {
                    try {
                    	testBeanLoading();
                    } catch (Exception e) {
                    	e.printStackTrace();
                    	threadException =e;
                        throw new RuntimeException(e);
                    }
                }
            });
        }
        
        for(Thread thread:threadList) {
            thread.start();
            thread.join();
        }
		
		if (threadException != null) {
			throw threadException;
		}
	}

}
