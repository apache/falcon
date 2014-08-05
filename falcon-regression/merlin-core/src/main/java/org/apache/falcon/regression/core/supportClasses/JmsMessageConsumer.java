/**
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

package org.apache.falcon.regression.core.supportClasses;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.List;

/** Collects JMS messages in a separate thread. */
public class JmsMessageConsumer extends Thread {
    /*URL of the JMS server
    brokerURL = "tcp://host:61616?daemon=true";
    ActiveMQConnection.DEFAULT_BROKER_URL;
    Name of the queue we will receive messages from
    String subject = "IVORY.TOPIC";*/

    private static final Logger LOGGER = Logger.getLogger(JmsMessageConsumer.class);
    private static final int MAX_MESSAGE_COUNT = 1000;

    private final String brokerURL;
    private final String topicName;
    private final List<MapMessage> receivedMessages;

    public List<MapMessage> getReceivedMessages() {
        return receivedMessages;
    }

    public JmsMessageConsumer(String topicName, String brokerURL) {
        super(topicName);
        this.topicName = topicName;
        this.brokerURL = brokerURL;
        receivedMessages = new ArrayList<MapMessage>();
    }

    @Override
    public void run() {
        try {
            // Getting JMS connection from the server
            Connection connection = new ActiveMQConnectionFactory(brokerURL).createConnection();
            connection.start();

            // Creating session for sending messages
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination destination = session.createTopic(topicName);
            MessageConsumer consumer = session.createConsumer(destination);

            try {
                LOGGER.info("Starting to receive messages.");
                int count = 0;
                for (; count < MAX_MESSAGE_COUNT; ++count) {
                    Message message = consumer.receive(); //blocking call
                    if (message == null) {
                        LOGGER.info("Received empty message, count = " + count);
                    } else {
                        LOGGER.info("Received message, id = " + message.getJMSMessageID());
                        receivedMessages.add((MapMessage) message);
                    }
                }
                if (count >= MAX_MESSAGE_COUNT) {
                    LOGGER.warn("Not reading more messages, already read " + count + " messages.");
                }
            } finally {
                LOGGER.info("Stopping to receive messages.");
                connection.close();
            }
        } catch (Exception e) {
            LOGGER.info("caught exception: " + ExceptionUtils.getStackTrace(e));
        }
    }

}
