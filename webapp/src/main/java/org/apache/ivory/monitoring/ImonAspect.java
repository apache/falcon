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

package org.apache.ivory.monitoring;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.ivory.aspect.AbstractIvoryAspect;
import org.apache.ivory.aspect.ResourceMessage;
import org.apache.ivory.monitoring.MonDemandClient;
import org.aspectj.lang.annotation.Aspect;
import org.mondemand.Client;

/**
 * publishes captured messages to MonDemand
 */
@Aspect
public class ImonAspect extends AbstractIvoryAspect {

	// TODO get this value from properties file
	private static final int ALLOWED_QUEUE_SIZE = 1000;
	// Producer should consider as non-blocking and consumer as blocking queue
	private static final BlockingQueue<ResourceMessage> QUEUE = new ArrayBlockingQueue<ResourceMessage>(
			ALLOWED_QUEUE_SIZE);

	public ImonAspect() {
		super();
		Thread daemon = new Consumer();
		daemon.setName("iMonPublisher");
		daemon.setDaemon(true);
		daemon.start();
	}

	@Override
	public void publishMessage(ResourceMessage message) {
		QUEUE.offer(message);
	}

	public static class Consumer extends Thread {
		@Override
		public void run() {
			while (true) {
				ResourceMessage message = null;
				try {
					message = QUEUE.take();
				} catch (InterruptedException e) {
					System.out.println("iMon consumer interrupted");
					return;
				}
				sendToImon(message);
			}
		}

		private void sendToImon(ResourceMessage message) {
			Client client = MonDemandClient.getClient();

			client.removeAllContexts();
			client.addContext("hostname", MonDemandClient.getHostname());

			client.setKey(message.getAction(), message.getExecutionTime());
			for (Map.Entry<String, String> dimension : message.getDimensions()
					.entrySet()) {
				client.addContext(dimension.getKey(), dimension.getValue());
			}
			client.flushStats(true);
		}
	}
}
