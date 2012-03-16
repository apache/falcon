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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.aspectj.lang.annotation.Aspect;

@Aspect
public class ImonAspect extends AbstractIvoryAspect {

	// TODO get this value from system.properties file
	private static final int ALLOWED_QUEUE_SIZE = 1000;
	// Producer should consider as non-blocking
	private static final BlockingQueue<ResourceMessage> QUEUE = new ArrayBlockingQueue<ResourceMessage>(
			ALLOWED_QUEUE_SIZE + 1);

	public ImonAspect() {
		super();
		new Consumer().start();
		System.out.println("iMon constructor invoked");

	}

	@Override
	public void publishMessage(ResourceMessage message) {
		synchronized (QUEUE) {
			if (QUEUE.size() == ALLOWED_QUEUE_SIZE) {
				// dont block the queue on full size
				System.out.println("iMon queue full, returning");
				return;
			}
		}

		QUEUE.add(message);

	}

	public static class Consumer extends Thread {
		@Override
		public void run() {
			while (true) {
				ResourceMessage message = QUEUE.poll();
				try {
					Thread.sleep(200);
				} catch (InterruptedException e) {

				}
				if (message != null) {
					sendToImon(message);
				}
			}
		}

		private void sendToImon(ResourceMessage message) {
			System.out.println("iMon Queue has:" + QUEUE.size()+" messages");

		}
	}
}
