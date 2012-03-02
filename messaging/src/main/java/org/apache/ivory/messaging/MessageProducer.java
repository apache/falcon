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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jms.core.JmsTemplate;

/**
 * Default Ivory Message Producer The configuration are loaded from
 * jms-beans.xml
 */
public class MessageProducer {

	private JmsTemplate template;
	private static ActiveMQConnectionFactory connectionFactory;
	private static final Logger LOG = Logger.getLogger(MessageProducer.class);

	public JmsTemplate getTemplate() {
		return this.template;
	}

	/**
	 * 
	 * @param template
	 *            - injected by spring JMS template
	 */
	public void setTemplate(JmsTemplate template) {
		this.template = template;
	}

	public ActiveMQConnectionFactory getConnectionFactory() {
		return MessageProducer.connectionFactory;
	}

	/**
	 * 
	 * @param connectionFactory
	 *            - Injected by Spring DI
	 */
	public void setConnectionFactory(ActiveMQConnectionFactory connectionFactory) {
		MessageProducer.connectionFactory = connectionFactory;
	}

	/**
	 * 
	 * @param arguments
	 *            - Accepts a Message to be send to JMS topic, creates a new
	 *            Topic based on topic name if it does not exist or else
	 *            existing topic with the same name is used to send the message.
	 */
	protected void sendMessage(ProcessMessage args) {

		ActiveMQTopic feedTopic = new ActiveMQTopic(args.getProcessTopicName());
		LOG.debug("Sending message to broker: "
				+ MessageProducer.connectionFactory.getBrokerURL());

		this.template.send(feedTopic, new ProcessMessageCreator(args));

	}

	/**
	 * 
	 * @param args
	 *            - array of Strings, which will be used to create TextMessage
	 */
	public static void main(String[] args) {

		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
				new String[] { "jms-beans.xml" });					
		
		MessageProducer messageProducer = (MessageProducer) context
				.getBean("ivoryProducer");
		ProcessMessage[] processMessages = ArgumentsResolver.resolveToMessage(args);
		MessageProducer.connectionFactory.setBrokerURL(processMessages[0].getBrokerUrl());
		for(ProcessMessage processMessage: processMessages){
			messageProducer.sendMessage(processMessage);
		}

	}
	

}
