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

package org.apache.falcon.messaging;

import org.apache.falcon.retention.EvictedInstanceSerDe;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.Topic;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Message producer used in the workflow to send a message to the queue/topic.
 */
public class JMSMessageProducer {

    private static final Logger LOG = LoggerFactory.getLogger(JMSMessageProducer.class);

    /**
     * Message messageType.
     */
    public enum MessageType {FALCON, USER}

    public static final String FALCON_TOPIC_PREFIX = "FALCON.";
    public static final String ENTITY_TOPIC_NAME = "ENTITY.TOPIC";

    private static final long DEFAULT_TTL = 3 * 24 * 60 * 60 * 1000;

    private final WorkflowExecutionContext context;
    private final MessageType messageType;

    protected JMSMessageProducer(WorkflowExecutionContext context, MessageType messageType) {
        this.context = context;
        this.messageType = messageType;
    }

    // convention over configuration
    public String getTopicName() {
        String topicNameValue = context.getValue(WorkflowExecutionArgs.TOPIC_NAME);
        return topicNameValue != null
                ? topicNameValue  // return if user has set a topic
                : FALCON_TOPIC_PREFIX // else falcon entity topic or user = FALCON.$entity_name
                + (messageType == MessageType.FALCON ? ENTITY_TOPIC_NAME : context.getEntityName());
    }

    public String getBrokerImplClass() {
        return messageType == MessageType.FALCON
                ? context.getValue(WorkflowExecutionArgs.BRKR_IMPL_CLASS)
                : context.getValue(WorkflowExecutionArgs.USER_BRKR_IMPL_CLASS);
    }

    public String getBrokerUrl() {
        return messageType == MessageType.FALCON
                ? context.getValue(WorkflowExecutionArgs.BRKR_URL)
                : context.getValue(WorkflowExecutionArgs.USER_BRKR_URL);
    }

    private long getBrokerTTL() {
        long messageTTL = DEFAULT_TTL;

        try {
            long messageTTLinMins = Long.parseLong(context.getValue(WorkflowExecutionArgs.BRKR_TTL));
            messageTTL = messageTTLinMins * 60 * 1000;
        } catch (NumberFormatException e) {
            LOG.error("Error in parsing broker.ttl, setting TTL to: {} milli-seconds", DEFAULT_TTL);
        }

        return messageTTL;
    }

    public static MessageBuilder builder(WorkflowExecutionContext context) {
        return new MessageBuilder(context);
    }

    /**
     * Builder for JMSMessageProducer.
     */
    public static final class MessageBuilder {
        private final WorkflowExecutionContext context;
        private MessageType type;

        private MessageBuilder(WorkflowExecutionContext context) {
            this.context = context;
        }

        public MessageBuilder type(MessageType aMessageType) {
            this.type = aMessageType;
            return this;
        }

        public JMSMessageProducer build() {
            if (type == null) {
                throw new IllegalArgumentException("Message messageType needs to be set.");
            }
            return new JMSMessageProducer(context, type);
        }
    }

    /**
     * Accepts a Message to be send to JMS topic, creates a new
     * Topic based on topic name if it does not exist or else
     * existing topic with the same name is used to send the message.
     * Sends all arguments.
     *
     * @return error code
     * @throws Exception
     */
    public int sendMessage() throws Exception {
        return sendMessage(WorkflowExecutionArgs.values());
    }

    /**
     * Accepts a Message to be send to JMS topic, creates a new
     * Topic based on topic name if it does not exist or else
     * existing topic with the same name is used to send the message.
     *
     * @param filteredArgs args sent in the message.
     * @return error code
     * @throws Exception
     */
    public int sendMessage(WorkflowExecutionArgs[] filteredArgs) throws Exception {
        List<Map<String, String>> messageList = buildMessageList(filteredArgs);

        if (messageList.isEmpty()) {
            LOG.warn("No operation on output feed");
            return 0;
        }

        Connection connection = null;
        try {
            connection = createAndStartConnection(getBrokerImplClass(), "", "", getBrokerUrl());

            for (Map<String, String> message : messageList) {
                LOG.info("Sending message: {}", message);
                sendMessage(connection, message);
            }
        } finally {
            closeQuietly(connection);
        }

        return 0;
    }

    private List<Map<String, String>> buildMessageList(WorkflowExecutionArgs[] filteredArgs) {
        String[] feedNames = context.getOutputFeedNamesList();
        if (feedNames == null) {
            return Collections.emptyList();
        }

        String[] feedPaths;
        try {
            feedPaths = getFeedPaths();
        } catch (IOException e) {
            LOG.error("Error getting instance paths: ", e);
            throw new RuntimeException(e);
        }

        List<Map<String, String>> messages = new ArrayList<Map<String, String>>(feedPaths.length);
        for (int i = 0; i < feedPaths.length; i++) {
            Map<String, String> message = buildMessage(filteredArgs);

            // override default values
            if (context.getEntityType().equalsIgnoreCase("PROCESS")) {
                change(message, WorkflowExecutionArgs.OUTPUT_FEED_NAMES, feedNames[i]);
            } else {
                change(message, WorkflowExecutionArgs.OUTPUT_FEED_NAMES,
                        message.get(WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName()));
            }

            change(message, WorkflowExecutionArgs.OUTPUT_FEED_PATHS, feedPaths[i]);
            messages.add(message);
        }

        return messages;
    }

    private void sendMessage(Connection connection,
                             Map<String, String> message) throws JMSException {
        Session session = null;
        javax.jms.MessageProducer producer = null;
        try {
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic entityTopic = session.createTopic(getTopicName());

            producer = session.createProducer(entityTopic);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            producer.setTimeToLive(getBrokerTTL());

            producer.send(createMessage(session, message));
        } finally {
            if (producer != null) {
                producer.close();
            }

            if (session != null) {
                session.close();
            }
        }
    }

    public Message createMessage(Session session,
                                 Map<String, String> message) throws JMSException {
        MapMessage mapMessage = session.createMapMessage();

        for (Map.Entry<String, String> entry : message.entrySet()) {
            mapMessage.setString(entry.getKey(), entry.getValue());
        }

        return mapMessage;
    }

    public void change(Map<String, String> message, WorkflowExecutionArgs key, String value) {
        message.remove(key.getName());
        message.put(key.getName(), value);
    }

    private String[] getFeedPaths() throws IOException {
        WorkflowExecutionContext.EntityOperations operation = context.getOperation();
        if (operation == WorkflowExecutionContext.EntityOperations.GENERATE
                || operation == WorkflowExecutionContext.EntityOperations.REPLICATE) {
            LOG.debug("Returning instance paths: " + context.getOutputFeedInstancePaths());
            return context.getOutputFeedInstancePathsList();
        }

        // else case of feed retention
        Path logFile = new Path(context.getLogFile());
        FileSystem fs = FileSystem.get(logFile.toUri(), new Configuration());

        if (!fs.exists(logFile)) {
            // Evictor Failed without deleting a single path
            return new String[0];
        }

        return EvictedInstanceSerDe.deserializeEvictedInstancePaths(fs, logFile);
    }

    private Map<String, String> buildMessage(final WorkflowExecutionArgs[] filter) {
        Map<String, String> message = new HashMap<String, String>(filter.length);
        for (WorkflowExecutionArgs arg : filter) {
            message.put(arg.getName(), context.getValue(arg));
        }

        // this is NOT useful since the file is deleted after message is sent
        message.remove(WorkflowExecutionArgs.LOG_FILE.getName());
        return message;
    }

    @SuppressWarnings("unchecked")
    private Connection createAndStartConnection(String implementation, String userName,
                                                String password, String url)
        throws JMSException, ClassNotFoundException, InstantiationException,
               IllegalAccessException, InvocationTargetException, NoSuchMethodException {

        Class<ConnectionFactory> clazz = (Class<ConnectionFactory>)
                JMSMessageProducer.class.getClassLoader().loadClass(implementation);

        ConnectionFactory connectionFactory = clazz
                .getConstructor(String.class, String.class, String.class)
                .newInstance(userName, password, url);

        Connection connection = connectionFactory.createConnection();
        connection.start();

        return connection;
    }

    private void closeQuietly(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (JMSException e) {
            LOG.error("Error in closing connection:", e);
        }
    }
}
