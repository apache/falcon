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

package org.apache.falcon.plugin;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.aspect.ResourceMessage;
import org.apache.falcon.entity.v0.EntityNotification;
import org.apache.falcon.util.EmailNotificationProps;
import org.apache.falcon.util.NotificationUtil;
import org.apache.falcon.util.StartupProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Map;
import java.util.Properties;

/**
 * Concrete class for email notification.
 */
public class EmailNotification implements NotificationPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(EmailNotification.class);

    private static final String SMTP_HOST = StartupProperties.get().getProperty(
            EmailNotificationProps.SMTP_HOST.getName(), "localhost");
    private static final String SMTP_PORT = StartupProperties.get().getProperty(
            EmailNotificationProps.SMTP_PORT.getName(), "3025");
    private static final String SMTP_FROM = StartupProperties.get().getProperty(
            EmailNotificationProps.SMTP_FROM.getName(), "falcon@localhost");
    private static final Boolean SMTP_AUTH = Boolean.valueOf(StartupProperties.get().getProperty(
            EmailNotificationProps.SMTP_AUTH.getName(), "false"));
    private static final String SMTP_USER = StartupProperties.get().getProperty(
            EmailNotificationProps.SMTP_USER.getName(), "");
    private static final String SMTP_PASSWORD = StartupProperties.get().getProperty(
            EmailNotificationProps.SMTP_PASSWORD.getName(), "");

    private static final String NEWLINE_DELIM = System.getProperty("line.separator");
    private static final String TAB_DELIM = "\t";

    private Message message;

    public EmailNotification() throws FalconException {
        initialize();
    }

    private void initialize() throws FalconException {
        Properties emailProperties = new Properties();
        if (StringUtils.isEmpty(SMTP_HOST) && StringUtils.isEmpty(SMTP_PORT)
                && StringUtils.isEmpty(SMTP_FROM)) {
            LOG.error("SMTP properties is not defined in startup.properties");
            return;
        }

        emailProperties.setProperty("mail.smtp.host", SMTP_HOST);
        emailProperties.setProperty("mail.smtp.port", SMTP_PORT);
        emailProperties.setProperty("mail.smtp.auth", SMTP_AUTH.toString());
        try {
            Session session;
            if (!SMTP_AUTH) {
                session = Session.getInstance(emailProperties);
            } else {
                session = Session.getInstance(emailProperties, new FalconMailAuthenticator(SMTP_USER, SMTP_PASSWORD));
            }
            message = new MimeMessage(session);
            message.setFrom(new InternetAddress(SMTP_FROM));
        } catch (MessagingException e) {
            throw new FalconException("Exception occurred in SMTP initialization:" +e);
        }
    }


    public void sendNotification(ResourceMessage resourceMessage, EntityNotification entityNotification)
        throws FalconException {
        try {
            message.addRecipients(Message.RecipientType.TO,
                    NotificationUtil.getToAddress(entityNotification.getTo()));

            if (resourceMessage.getAction().equals("wf-instance-succeeded")) {
                sendSuccessNotification(resourceMessage);
            } else if ((resourceMessage.getAction().equals("wf-instance-failed"))) {
                sendFailureNotification(resourceMessage);
            }

            // Send message
            Transport.send(message);
        } catch (MessagingException e) {
            throw new FalconException("Error occurred while sending email message using SMTP:" +e);
        }
    }

    private void sendSuccessNotification(ResourceMessage resourceMessage) throws FalconException {
        try {
            String subjectMessage = "Falcon Instance Succeeded : " + getSubjectMessage(resourceMessage);
            message.setSubject(subjectMessage);
            message.setText(subjectMessage + NEWLINE_DELIM + getBodyMessage(resourceMessage));
        } catch (MessagingException e) {
            throw new FalconException("Error in composing email notification:" +e);
        }
    }

    private void sendFailureNotification(ResourceMessage resourceMessage) throws FalconException {
        try {
            String subjectMessage = "Falcon Instance Failed : " + getSubjectMessage(resourceMessage);
            message.setSubject(subjectMessage);
            message.setText(subjectMessage + NEWLINE_DELIM + getBodyMessage(resourceMessage));
        } catch (MessagingException e) {
            throw new FalconException("Error in composing email notification:" +e);
        }
    }

    private String getSubjectMessage(ResourceMessage resourceMessage) throws FalconException {
        return "Workflow id:" + resourceMessage.getDimensions().get("wf-id")
                + " Name:" + resourceMessage.getDimensions().get("entity-name")
                + " Type:" + resourceMessage.getDimensions().get("entity-type");
    }

    private String getBodyMessage(ResourceMessage resourceMessage) throws FalconException {
        StringBuilder msg = new StringBuilder();
        Map<String, String> instanceMap = resourceMessage.getDimensions();
        for (Map.Entry<String, String> entry : instanceMap.entrySet()) {
            msg.append(entry.getKey() + TAB_DELIM + entry.getValue());
            msg.append(NEWLINE_DELIM);
        }
        msg.append("---");
        return msg.toString();
    }

    private static class FalconMailAuthenticator extends Authenticator {
        private String user;
        private String password;

        public FalconMailAuthenticator(String user, String password) {
            this.user = user;
            this.password = password;
        }

        @Override
        protected PasswordAuthentication getPasswordAuthentication() {
            return new PasswordAuthentication(user, password);
        }
    }
}
