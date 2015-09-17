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

import org.apache.falcon.aspect.ResourceMessage;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityNotification;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.util.NotificationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EmailNotification plugin implementation for sending email notification.
 */
public class EmailNotificationPlugin implements MonitoringPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(EmailNotificationPlugin.class);

    @Override
    public void monitor(ResourceMessage message) {
        try {
            String entityType = message.getDimensions().get("entity-type");
            String entityName = message.getDimensions().get("entity-name");
            Entity entity = null;
            if (entityType.equals("PROCESS")) {
                entity = ConfigurationStore.get().get(EntityType.PROCESS, entityName);
            } else if (entityType.equals("FEED")) {
                entity = ConfigurationStore.get().get(EntityType.FEED, entityName);
            }

            EntityNotification entityNotification = EntityUtil.getEntityNotification(entity);
            if (entityNotification == null) {
                LOG.info("Notification tag is not defined for entity: {}", entityName);
                return;
            }

            if (!(NotificationUtil.isEmailAddressValid(entityNotification))) {
                LOG.error("Notification email address is not valid: {}", entityNotification.getTo());
                return;
            }

            if ((message.getAction().equals("wf-instance-succeeded")
                    || message.getAction().equals("wf-instance-failed"))) {
                NotificationPlugin pluginType = NotificationHandler.getNotificationType(entityNotification.getType());
                if (pluginType != null) {
                    pluginType.sendNotification(message, entityNotification);
                } else {
                    LOG.error("Notification type is not supported: {}", entityNotification.getType());
                }
            }
        } catch (Exception e) {
            LOG.error("Exception in sending Notification from EmailNotificationPlugin:" +e);
        }
    }
}
