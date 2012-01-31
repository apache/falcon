/*
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

package org.apache.ivory.workflow.engine;

import java.util.Properties;

import org.apache.ivory.IvoryException;
import org.apache.ivory.util.StartupProperties;
import org.apache.oozie.client.OozieClientException;

public class OozieClient extends org.apache.oozie.client.OozieClient{
    private static final String PROPERTY_PREFIX = "oozie.";

    private static final String URL = "url";
    private static final String NAME_NODE = "nameNode";
    private static final String JOB_TRACKER = "jobTracker";
    private static final String QUEUE_NAME = "queueName";
    private static String oozieUrl = StartupProperties.get().getProperty(PROPERTY_PREFIX + URL);

    @Override
    public Properties createConfiguration() {
        Properties conf = new Properties();
        //TODO read from cluster definition?
        conf.setProperty(OozieClient.USER_NAME, StartupProperties.get().get(PROPERTY_PREFIX + OozieClient.USER_NAME).toString());
        conf.setProperty(NAME_NODE, StartupProperties.get().get(PROPERTY_PREFIX + NAME_NODE).toString());
        conf.setProperty(JOB_TRACKER, StartupProperties.get().get(PROPERTY_PREFIX + JOB_TRACKER).toString());
        conf.setProperty(QUEUE_NAME, StartupProperties.get().get(PROPERTY_PREFIX + QUEUE_NAME).toString());
        return conf;
    }

    public OozieClient() {
        super(oozieUrl);
    }

    public String schedule(Properties conf) throws IvoryException {
        try {
            return super.run(conf);
        } catch (OozieClientException e) {
            throw new IvoryException(e);
        }
    }
}
