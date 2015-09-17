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

import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.ServerSetup;
import org.apache.falcon.aspect.ResourceMessage;
import org.apache.falcon.entity.v0.process.Notification;
import org.apache.falcon.util.NotificationUtil;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.mail.Message;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit test to test Email Notification.
 */

public class EmailNotificationTest {
    private static final String EMAIL_FROM = "falcon@localhost";

    private GreenMail mailServer;
    private ResourceMessage resourceMessage;

    @BeforeClass
    protected void setUp() throws Exception {
        ServerSetup setup = new ServerSetup(3025, "localhost", "smtp");
        mailServer = new GreenMail(setup);
        mailServer.setUser(EMAIL_FROM, "", "");
        mailServer.start();
        resourceMessage = buildResourceMessage();
    }

    private ResourceMessage buildResourceMessage() {
        String action = "wf-instance-succeeded";
        ResourceMessage.Status status = ResourceMessage.Status.SUCCEEDED;
        long executionTime = 78819000000L;  //Time in nano seconds.
        Map<String, String> dimensions = new HashMap<String, String>();
        dimensions.put("entity-type", "process");
        dimensions.put("entity-name", "pig-process");
        dimensions.put("wf-id", "001-oozie-wf");
        dimensions.put("wf-user", "falcon");
        dimensions.put("run-id", "1");
        dimensions.put("operation", "GENERATE");

        return new ResourceMessage(action, dimensions, status, executionTime);
    }


    @Test
    public void testSendNotification() throws Exception {
        String notificationType = "email";
        String emailTo = "falcon_to@localhost";

        Notification notification = new Notification();
        notification.setType(notificationType);
        notification.setTo(emailTo);

        NotificationPlugin pluginType = NotificationHandler.getNotificationType(notification.getType());
        Assert.assertNotNull(pluginType);

        pluginType.sendNotification(resourceMessage, notification);
        mailServer.waitForIncomingEmail(5000, 1);
        Message[] messages = mailServer.getReceivedMessages();
        Assert.assertNotNull(messages);
        Assert.assertEquals(messages[0].getFrom()[0].toString(), EMAIL_FROM);
        Assert.assertEquals(messages[0].getAllRecipients()[0].toString(), emailTo);
        Assert.assertEquals(messages[0].getSubject(), "Falcon Instance Succeeded : Workflow id:001-oozie-wf "
                + "Name:pig-process Type:process");
    }

    @Test
    public void testNotificationType() throws Exception {
        String notificationType = "email";
        String emailTo = "falcon@localhost";

        Notification notification = new Notification();
        notification.setType(notificationType);
        notification.setTo(emailTo);

        NotificationPlugin pluginType = NotificationHandler.getNotificationType(notification.getType());
        Assert.assertNotNull(pluginType);

        notificationType = "eml";
        notification.setType(notificationType);
        pluginType = NotificationHandler.getNotificationType(notification.getType());
        Assert.assertNull(pluginType);

        notificationType = "";
        notification.setType(notificationType);
        pluginType = NotificationHandler.getNotificationType(notification.getType());
        Assert.assertNull(pluginType);
    }

    @Test
    public void testNotificationEmailAddress() throws Exception {
        String notificationType = "email";
        String emailAddress = "falcon@locahost";

        Notification notification = new Notification();
        notification.setType(notificationType);
        notification.setTo(emailAddress);

        Assert.assertTrue(NotificationUtil.isEmailAddressValid(notification));

        emailAddress = "falcon_123@localhost";
        notification.setTo(emailAddress);
        Assert.assertTrue(NotificationUtil.isEmailAddressValid(notification));

        emailAddress = "falcon-123@localhost-inc.com";
        notification.setTo(emailAddress);
        Assert.assertTrue(NotificationUtil.isEmailAddressValid(notification));

        emailAddress = "falcon@locahost,hive@localhost,user@abc.com";
        notification.setTo(emailAddress);
        Assert.assertTrue(NotificationUtil.isEmailAddressValid(notification));

        emailAddress = "falcon@locahost,,,";
        notification.setTo(emailAddress);
        Assert.assertTrue(NotificationUtil.isEmailAddressValid(notification));

        emailAddress = "falcon";
        notification.setTo(emailAddress);
        Assert.assertFalse(NotificationUtil.isEmailAddressValid(notification));

        emailAddress = "falcon#localhost";
        notification.setTo(emailAddress);
        Assert.assertFalse(NotificationUtil.isEmailAddressValid(notification));

        emailAddress = "";
        notification.setTo(emailAddress);
        Assert.assertFalse(NotificationUtil.isEmailAddressValid(notification));
    }


    @AfterClass
    public void tearDown() throws Exception {
        mailServer.stop();
    }
}
