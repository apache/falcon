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

import java.lang.reflect.Method;

import org.apache.falcon.FalconException;
import org.apache.falcon.FalconRuntimException;
import org.apache.falcon.FalconWebException;
import org.apache.falcon.resource.AbstractEntityManager;
import org.apache.falcon.util.ReflectionUtils;
import org.apache.log4j.Logger;

public class IPCChannel extends AbstractChannel {
    private static final Logger LOG = Logger.getLogger(IPCChannel.class);
    private AbstractEntityManager service;

    public void init(String ignoreColo, String serviceName) throws FalconException {
        service = ReflectionUtils.getInstance(serviceName + ".impl");
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T invoke(String methodName, Object... args)
            throws FalconException {
        LOG.debug("Invoking method " + methodName + " on service " +
                service.getClass().getName());
        Method method = getMethod(service.getClass(), methodName, args);
        try {
            return (T) method.invoke(service, args);
        } catch (Exception e) {
            Throwable cause = e.getCause();
            if (cause != null)  {
                if (cause instanceof FalconWebException) throw (FalconWebException) cause;
                if (cause instanceof FalconRuntimException) throw (FalconRuntimException) cause;
                if (cause instanceof FalconException) throw (FalconException) cause;
            }
            throw new FalconException("Unable to invoke on the channel " + methodName +
                    " on service : " + service.getClass().getName() + cause);
        }
    }
}
