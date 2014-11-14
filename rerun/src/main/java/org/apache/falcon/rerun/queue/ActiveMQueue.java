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
package org.apache.falcon.rerun.queue;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ScheduledMessage;
import org.apache.falcon.FalconException;
import org.apache.falcon.messaging.util.MessagingUtil;
import org.apache.falcon.rerun.event.RerunEvent;
import org.apache.falcon.rerun.event.RerunEventFactory;

import javax.jms.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * An ActiveMQ implementation for DelayedQueue.
 *
 * @param <T>
 */
public class ActiveMQueue<T extends RerunEvent> extends DelayedQueue<T> {

    private ActiveMQConnection connection;
    private String brokerUrl;
    private String destinationName;
    private Destination destination;
    private MessageProducer producer;
    private MessageConsumer consumer;

    public ActiveMQueue(String brokerUrl, String destinationName) {
        this.brokerUrl = brokerUrl;
        this.destinationName = destinationName;
    }

    @Override
    public boolean offer(T event) throws FalconException {
        Session session;
        try {
            session = getSession();
            TextMessage msg = session.createTextMessage(event.toString());
            msg.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY,
                    event.getDelay(TimeUnit.MILLISECONDS));
            msg.setStringProperty("TYPE", event.getType().name());
            producer.send(msg);
            LOG.debug("Enqueued Message: {} with delay {} milli sec",
                    event.toString(), event.getDelay(TimeUnit.MILLISECONDS));
            return true;
        } catch (Exception e) {
            LOG.error("Unable to offer event: {} to ActiveMQ", event, e);
            throw new FalconException("Unable to offer event:" + event + " to ActiveMQ", e);
        }
    }

    private Session getSession() throws Exception {
        if (connection == null) {
            init();
        }

        return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Override
    public T take() throws FalconException {
        try {
            TextMessage textMessage = (TextMessage) consumer.receive();
            T event = new RerunEventFactory<T>().getRerunEvent(
                    textMessage.getStringProperty("TYPE"),
                    textMessage.getText());
            LOG.debug("Dequeued Message: {}", event.toString());
            return event;
        } catch (Exception e) {
            LOG.error("Error getting the message from ActiveMQ", e);
            throw new FalconException("Error getting the message from ActiveMQ: ", e);
        }
    }

    @Override
    public void populateQueue(List<T> events) {
    }

    @Override
    public void init() {
        try {
            createAndStartConnection("", "", brokerUrl);
            Session session = connection.createSession(false,
                    Session.AUTO_ACKNOWLEDGE);
            destination = session.createQueue(destinationName);
            producer = session.createProducer(destination);
            consumer = session.createConsumer(destination);
            LOG.info("Initialized Queue on ActiveMQ: {}", destinationName);
        } catch (Exception e) {
            LOG.error("Error starting ActiveMQ connection for delayed queue", e);
            throw new RuntimeException("Error starting ActiveMQ connection for delayed queue", e);
        }
    }

    private void createAndStartConnection(String userName, String password,
                                          String url) throws JMSException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                userName, password, url);
        connection = (ActiveMQConnection) connectionFactory.createConnection();
        connection.start();
        LOG.info("Connected successfully to {}", url);
    }

    @Override
    public void reconnect() throws FalconException {
        close();
        init();
    }

    public void close() {
        LOG.info("Closing queue for broker={}, destination{}", brokerUrl, destinationName);
        destination = null;

        MessagingUtil.closeQuietly(producer);
        MessagingUtil.closeQuietly(consumer);
        MessagingUtil.closeQuietly(connection);
    }
}
