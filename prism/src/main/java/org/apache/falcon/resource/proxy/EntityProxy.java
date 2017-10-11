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

package org.apache.falcon.resource.proxy;

import org.apache.falcon.FalconException;
import org.apache.falcon.FalconRuntimException;
import org.apache.falcon.FalconWebException;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.AbstractEntityManager;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Entity Proxy class to talk to channels.
 */
public abstract class EntityProxy<T extends APIResult> extends AbstractEntityManager{
    private final Class<T> clazz;
    private String type;
    private String name;

    public EntityProxy(String type, String name, Class<T> resultClazz) {
        this.clazz = resultClazz;
        this.type = type;
        this.name = name;
    }


    private T getResultInstance(APIResult.Status status, String message) {
        try {
            Constructor<T> constructor = clazz.getConstructor(APIResult.Status.class, String.class);
            return constructor.newInstance(status, message);
        } catch (Exception e) {
            throw new FalconRuntimException("Unable to consolidate result.", e);
        }
    }

    public EntityProxy(String type, String name) {
        this(type, name, (Class<T>) APIResult.class);
    }

    public T execute() {
        Set<String> colos = getColosToApply();

        Map<String, T> results = new HashMap();

        for (String colo : colos) {
            try {
                results.put(colo, doExecute(colo));
            } catch (FalconWebException e) {
                String message = ((APIResult) e.getResponse().getEntity()).getMessage();
                results.put(colo, getResultInstance(APIResult.Status.FAILED, message));
            } catch (Throwable throwable) {
                results.put(colo, getResultInstance(APIResult.Status.FAILED, throwable.getClass().getName() + "::"
                        + throwable.getMessage()));
            }
        }

        T finalResult = consolidateResult(results, clazz);
        if (finalResult.getStatus() == APIResult.Status.FAILED) {
            throw FalconWebException.newAPIException(finalResult.getMessage());
        } else {
            return finalResult;
        }
    }

    protected Set<String> getColosToApply() {
        return getApplicableColos(type, name);
    }

    protected abstract T doExecute(String colo) throws FalconException;
}
