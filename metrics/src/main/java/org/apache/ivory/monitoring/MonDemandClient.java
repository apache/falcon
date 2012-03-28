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

import java.net.InetAddress;

import org.mondemand.Client;
import org.mondemand.transport.LWESTransport;

public class MonDemandClient {

	// TODO get from properties file
	private static final String IMON_INET_ADD = "224.1.1.11";
	private static final int IMON_INET_PORT = 9191;

	private static final MonDemandClient monDemandClient = new MonDemandClient();
	private Client client;
	private String hostname;

	private MonDemandClient() {
		init();
	}

	private Client init() {

		client = new Client("IVORY");
		try {
			hostname = InetAddress.getLocalHost().getHostName();
			client.removeAllContexts();
			client.addContext("hostname", hostname);
			LWESTransport transport = new LWESTransport(
					InetAddress.getByName(IMON_INET_ADD), IMON_INET_PORT, null);
			client.addTransport(transport);
			System.out.println("Initialized client");
		} catch (Exception e) {
			throw new RuntimeException("Unable to get iMon client: "
					+ e.getMessage());
		}

		return client;
	}

	public static Client getClient() {
		return monDemandClient.client;
	}

	public static String getHostname() {
		return monDemandClient.hostname;
	}
}
