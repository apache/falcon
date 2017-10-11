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
package org.apache.falcon.workflow;

import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.logging.JobLogMover;
import org.apache.falcon.messaging.JMSMessageProducer;
import org.apache.falcon.workflow.util.OozieActionConfigurationHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility called by oozie workflow engine post workflow execution in parent workflow.
 */
public class FalconPostProcessing extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(FalconPostProcessing.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = OozieActionConfigurationHelper.createActionConf();
        ToolRunner.run(conf, new FalconPostProcessing(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        WorkflowExecutionContext context = WorkflowExecutionContext.create(args,
                WorkflowExecutionContext.Type.POST_PROCESSING, getConf());
        LOG.info("Post workflow execution context created {}", context);
        // serialize the context to HDFS under logs dir before sending the message
        context.serialize();

        String userBrokerUrl = context.getValue(WorkflowExecutionArgs.USER_BRKR_URL);
        boolean userNotificationEnabled = Boolean.parseBoolean(context.
                getValue(WorkflowExecutionArgs.USER_JMS_NOTIFICATION_ENABLED, "true"));

        if (userBrokerUrl != null && !userBrokerUrl.equals(ClusterHelper.NO_USER_BROKER_URL)
                && userNotificationEnabled) {
            LOG.info("Sending user message {} ", context);
            invokeUserMessageProducer(context);
        }

        // JobLogMover doesn't throw exception, a failed log mover will not fail the user workflow
        LOG.info("Moving logs {}", context);
        new JobLogMover().moveLog(context);

        return 0;
    }

    private void invokeUserMessageProducer(WorkflowExecutionContext context) throws Exception {
        JMSMessageProducer jmsMessageProducer = JMSMessageProducer.builder(context)
                .type(JMSMessageProducer.MessageType.USER)
                .build();
        jmsMessageProducer.sendMessage(WorkflowExecutionContext.USER_MESSAGE_ARGS);
    }
}
