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

import org.apache.falcon.aspect.AlertMessage;
import org.apache.falcon.aspect.AuditMessage;
import org.apache.falcon.aspect.GenericAlert;
import org.apache.falcon.aspect.ResourceMessage;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.util.FalconTestUtil;
import org.apache.falcon.util.StartupProperties;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Date;

/**
 * Test for ChainableMonitoringPlugin.
 */
public class ChainableMonitoringPluginTest
        implements MonitoringPlugin, AlertingPlugin, AuditingPlugin {

    @BeforeClass
    public void setUp() throws Exception {
        StartupProperties.get().
                setProperty("monitoring.plugins", this.getClass().getName());
        StartupProperties.get().
                setProperty("alerting.plugins", this.getClass().getName());
    }

    @Test
    public void testPlugin() throws Exception {
        GenericAlert.instrumentFailedInstance("cluster", "process", "agg-coord", "120:df",
                "ef-id", "wf-user", "1", "DELETE", "now", "error", "none", 1242);
        GenericAlert.alertJMSMessageConsumerFailed("test-alert", new Exception("test"));
        GenericAlert.audit(FalconTestUtil.TEST_USER_1, "127.0.0.1", "localhost",  "test-action", "127.0.0.1",
                SchemaHelper.formatDateUTC(new Date()));
    }

    @Override
    public void monitor(ResourceMessage message) {
        Assert.assertNotNull(message);
        Assert.assertEquals(message.getAction(), "wf-instance-failed");
    }

    @Override
    public void alert(AlertMessage alertMessage) {
        Assert.assertNotNull(alertMessage);
        Assert.assertEquals(alertMessage.getEvent(), "jms-message-consumer-failed");
    }

    @Override
    public void audit(AuditMessage auditMessage) {
        Assert.assertNotNull(auditMessage);
        Assert.assertEquals(auditMessage.getRequestUrl(), "test-action");
    }
}
