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

package org.apache.falcon.resource.channel;

import org.apache.falcon.FalconException;
import org.apache.falcon.util.DeploymentProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * A factory implementation for doling out {@link Channel} objects.
 */
public final class ChannelFactory {

    private static Map<String, Channel> channels = new HashMap<String, Channel>();

    private static final String EMBEDDED = "embedded";
    private static final String MODE = "deploy.mode";

    private ChannelFactory() {
    }

    public static synchronized Channel get(String serviceName, String colo)
        throws FalconException {
        Channel channel = channels.get(colo + "/" + serviceName);
        if (channel == null) {
            channel = getChannel(DeploymentProperties.get().getProperty(MODE));
            channel.init(colo, serviceName);
            channels.put(colo + "/" + serviceName, channel);
        }

        return channel;
    }

    private static Channel getChannel(String mode) {
        Channel channel;
        if (mode.equals(EMBEDDED)) {
            channel = new IPCChannel();
        } else {
            channel = new SecureHTTPChannel();
        }

        return channel;
    }
}
