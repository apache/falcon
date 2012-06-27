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

package org.apache.ivory.service;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.ivory.IvoryException;
import org.apache.ivory.util.ReflectionUtils;
import org.apache.log4j.Logger;


public final class Services implements Iterable<IvoryService> {
    private static final Logger LOG = Logger.getLogger(Services.class);

    private static Services instance = new Services();

    private Services() { }

    public static Services get() {
        return instance;
    }

    private final Map<String, IvoryService> services =
            new LinkedHashMap<String, IvoryService>();

    public synchronized void register(IvoryService service)
            throws IvoryException {
        if (services.containsKey(service.getName())) {
            throw new IvoryException("Service " + service.getName() +
                    " already registered");
        } else {
            services.put(service.getName(), service);
        }
    }

    @SuppressWarnings("unchecked")
    public <T extends IvoryService> T getService(String serviceName) {
        if (services.containsKey(serviceName)) {
            return (T) services.get(serviceName);
        } else {
            throw new NoSuchElementException("Service " + serviceName +
                    " not registered with registry");
        }
    }

    public boolean isRegistered(String serviceName) {
        return services.containsKey(serviceName);
    }

    @Override
    public Iterator<IvoryService> iterator() {
        return services.values().iterator();
    }

    public IvoryService init(String serviceName) throws IvoryException {
        if (isRegistered(serviceName)) {
            throw new IvoryException("Service is already initialized " +
                    serviceName);
        }
        IvoryService service = ReflectionUtils.getInstance(serviceName + ".impl");
        register(service);
        return service;
    }
    
    public void reset(){
    	services.clear();
    }
}
