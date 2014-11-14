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

package org.apache.falcon.messaging.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

/**
 * Utility class.
 */
public final class MessagingUtil {

    private static final Logger LOG = LoggerFactory.getLogger(MessagingUtil.class);

    private MessagingUtil() {
    }

    public static void closeQuietly(TopicSubscriber topicSubscriber) {
        if (topicSubscriber != null) {
            try {
                topicSubscriber.close();
            } catch (JMSException ignore) {
                LOG.error("Error closing JMS topic subscriber: " + topicSubscriber, ignore);
            }
        }
    }

    public static void closeQuietly(TopicSession topicSession, String clientId) {
        if (topicSession != null) { // unsubscribe the durable topic topicSubscriber
            try {
                topicSession.unsubscribe(clientId);
                topicSession.close();
            } catch (JMSException ignore) {
                LOG.error("Error closing JMS topic session: " + topicSession, ignore);
            }
        }
    }

    public static void closeQuietly(Connection connection) {
        if (connection != null) {
            try {
                LOG.info("Attempting to close connection");
                connection.close();
            } catch (JMSException ignore) {
                LOG.error("Error closing JMS connection: " + connection, ignore);
            }
        }
    }

    public static void closeQuietly(MessageProducer messageProducer) {
        if (messageProducer != null) {
            try {
                LOG.info("Attempting to close producer");
                messageProducer.close();
            } catch (JMSException ignore) {
                LOG.error("Error closing JMS messageProducer: " + messageProducer, ignore);
            }
        }
    }

    public static void closeQuietly(MessageConsumer messageConsumer) {
        if (messageConsumer != null) {
            try {
                LOG.info("Attempting to close consumer");
                messageConsumer.close();
            } catch (JMSException ignore) {
                LOG.error("Error closing JMS messageConsumer: " + messageConsumer, ignore);
            }
        }
    }
}
