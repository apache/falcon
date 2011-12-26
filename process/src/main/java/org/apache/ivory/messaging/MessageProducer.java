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

package org.apache.ivory.messaging;

import javax.jms.Destination;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jms.core.JmsTemplate;

/**
 * Default Ivory Message Producer The configuration are loaded from
 * jms-beans.xml
 */
public class MessageProducer {

	private JmsTemplate template;

	private Destination[] destinations;

	private static final MessageProducer producer;

	private static final Logger LOG = Logger.getLogger(MessageProducer.class);

	static {

		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
				new String[] { "jms-beans.xml" });

		ActiveMQConnectionFactory factory = (ActiveMQConnectionFactory) context
				.getBean("jmsFactory");

		LOG.debug("Broker URL: " + factory.getBrokerURL());

		producer = (MessageProducer) context.getBean("ivoryProducer");
	}

	public JmsTemplate getTemplate() {
		return this.template;
	}

	public void setTemplate(JmsTemplate template) {
		this.template = template;
	}

	public Destination[] getDestinations() {
		return this.destinations;
	}

	public void setDestinations(Destination[] destinations) {
		this.destinations = destinations;
	}

	protected void sendMessage(String message) {

		for (Destination destination : this.destinations) {
			LOG.debug("Sending Message: " + message + " To " + destination);
			this.template.send(destination, new ProcessMessageCreator(message));
		}
	}

	/**
	 * The only argument to be passed is a String as message from Oozie.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		if (args.length != 1) {
			LOG.error("Argument lenth is not equal to 1");
			throw new IllegalArgumentException();
		}

		LOG.debug("Got main argument: " + args[0]);

		producer.sendMessage(args[0]);

	}

}
