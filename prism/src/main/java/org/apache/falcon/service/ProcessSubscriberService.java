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
package org.apache.falcon.service;

import org.apache.falcon.FalconException;
import org.apache.falcon.util.StartupProperties;

/**
 * A Falcon Service that initializes and starts a topic subscriber.
 */
public class ProcessSubscriberService implements FalconService {

    private FalconTopicSubscriber subscriber;

    private static enum JMSProps {
        FalconBrokerImplClass("broker.impl.class", "org.apache.activemq.ActiveMQConnectionFactory"),
        FalconBrokerUrl("broker.url", "tcp://localhost:61616?daemon=true"),
        FalconEntityTopic("entity.topic", "FALCON.ENTITY.TOPIC");

        private String propName;
        private String defaultPropValue;

        private JMSProps(String propName, String defaultPropValue) {
            this.propName = propName;
            this.defaultPropValue = defaultPropValue;
        }
    }

    @Override
    public String getName() {
        return ProcessSubscriberService.class.getSimpleName();
    }

    @Override
    public void init() throws FalconException {
        String falconBrokerImplClass = getPropertyValue(JMSProps.FalconBrokerImplClass);
        String falconBrokerUrl = getPropertyValue(JMSProps.FalconBrokerUrl);
        String falconEntityTopic = getPropertyValue(JMSProps.FalconEntityTopic);

        subscriber = new FalconTopicSubscriber(falconBrokerImplClass, "", "", falconBrokerUrl, falconEntityTopic);
        subscriber.startSubscriber();
    }

    private String getPropertyValue(JMSProps prop) {
        return StartupProperties.get().getProperty(prop.propName, prop.defaultPropValue);
    }

    @Override
    public void destroy() throws FalconException {
        if (subscriber != null) { // in case there was an exception while starting subscriber
            subscriber.closeSubscriber();
        }
    }
}
