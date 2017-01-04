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
import org.apache.falcon.entity.EntityNotRegisteredException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.channel.Channel;
import org.apache.falcon.resource.channel.ChannelFactory;
import org.apache.falcon.util.DeploymentUtil;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.falcon.resource.AbstractEntityManager.getAllColos;
import static org.apache.falcon.resource.AbstractEntityManager.getApplicableColos;
import static org.apache.falcon.resource.proxy.SchedulableEntityManagerProxy.FALCON_TAG;

/**
 * Proxy Util class to proxy entity management apis from prism to servers.
 */
class EntityProxyUtil {
    private final Map<String, Channel> entityManagerChannels = new HashMap<>();
    private final Map<String, Channel> configSyncChannels = new HashMap<>();

    EntityProxyUtil() {
        try {
            Set<String> colos = getAllColos();

            for (String colo : colos) {
                initializeFor(colo);
            }

            DeploymentUtil.setPrismMode();
        } catch (FalconException e) {
            throw new FalconRuntimException("Unable to initialize channels", e);
        }
    }
    private void initializeFor(String colo) throws FalconException {
        entityManagerChannels.put(colo, ChannelFactory.get("SchedulableEntityManager", colo));
        configSyncChannels.put(colo, ChannelFactory.get("ConfigSyncService", colo));
    }

    Channel getConfigSyncChannel(String colo) throws FalconException {
        if (!configSyncChannels.containsKey(colo)) {
            initializeFor(colo);
        }
        return configSyncChannels.get(colo);
    }

    Channel getEntityManager(String colo) throws FalconException {
        if (!entityManagerChannels.containsKey(colo)) {
            initializeFor(colo);
        }
        return entityManagerChannels.get(colo);
    }

    Map<String, APIResult> proxySubmit(final String type, final HttpServletRequest bufferedRequest,
                                       final Entity entity, final Set<String> colos) {
        Map<String, APIResult> results = new HashMap<>();
        results.put(FALCON_TAG, new EntityProxy(type, entity.getName()) {
            @Override
            protected Set<String> getColosToApply() {
                return colos;
            }

            @Override
            protected APIResult doExecute(String colo) throws FalconException {
                return getConfigSyncChannel(colo).invoke("submit", bufferedRequest, type, colo);
            }
        }.execute());
        return results;
    }

    Map<String, APIResult> proxyDelete(final String type, final String entityName,
                                               final HttpServletRequest bufferedRequest) {
        Map<String, APIResult> results = new HashMap<>();
        results.put(FALCON_TAG, new EntityProxy(type, entityName) {
            @Override
            public APIResult execute() {
                try {
                    EntityUtil.getEntity(type, entityName);
                    return super.execute();
                } catch (EntityNotRegisteredException e) {
                    return new APIResult(APIResult.Status.SUCCEEDED,
                            entityName + "(" + type + ") doesn't exist. Nothing to do");
                } catch (FalconException e) {
                    throw FalconWebException.newAPIException(e);
                }
            }

            @Override
            protected APIResult doExecute(String colo) throws FalconException {
                return getConfigSyncChannel(colo).invoke("delete", bufferedRequest, type, entityName, colo);
            }
        }.execute());
        return results;
    }

    APIResult proxySchedule(final String type, final String entity, final String coloExpr,
                                    final Boolean skipDryRun, final String properties,
                                    final HttpServletRequest bufferedRequest) {
        return new EntityProxy(type, entity) {
            @Override
            protected Set<String> getColosToApply() {
                return getColosFromExpression(coloExpr, type, entity);
            }

            @Override
            protected APIResult doExecute(String colo) throws FalconException {
                return getEntityManager(colo).invoke("schedule", bufferedRequest, type, entity,
                        colo, skipDryRun, properties);
            }
        }.execute();
    }

    APIResult proxySuspend(final String type, final String entity, final String coloExpr,
                                   final HttpServletRequest bufferedRequest) {
        return new EntityProxy(type, entity) {
            @Override
            protected Set<String> getColosToApply() {
                return getColosFromExpression(coloExpr, type, entity);
            }

            @Override
            protected APIResult doExecute(String colo) throws FalconException {
                return getEntityManager(colo).invoke("suspend", bufferedRequest, type, entity,
                        colo);
            }
        }.execute();
    }

    APIResult proxyResume(final String type, final String entity, final String coloExpr,
                          final HttpServletRequest bufferedRequest) {
        return new EntityProxy(type, entity) {
            @Override
            protected Set<String> getColosToApply() {
                return getColosFromExpression(coloExpr, type, entity);
            }

            @Override
            protected APIResult doExecute(String colo) throws FalconException {
                return getEntityManager(colo).invoke("resume", bufferedRequest, type, entity,
                        colo);
            }
        }.execute();
    }

    Map<String, APIResult> proxyUpdate(final String type, final String entityName, final Boolean skipDryRun,
                                       final HttpServletRequest bufferedRequest, Entity newEntity) {
        final Set<String> oldColos = getApplicableColos(type, entityName);
        final Set<String> newColos = getApplicableColos(type, newEntity);
        final Set<String> mergedColos = new HashSet<>();
        mergedColos.addAll(oldColos);
        mergedColos.retainAll(newColos);    //Common colos where update should be called
        newColos.removeAll(oldColos);   //New colos where submit should be called
        oldColos.removeAll(mergedColos);   //Old colos where delete should be called

        Map<String, APIResult> results = new HashMap<>();
        if (!oldColos.isEmpty()) {
            results.put(FALCON_TAG + "/delete", new EntityProxy(type, entityName) {
                @Override
                protected Set<String> getColosToApply() {
                    return oldColos;
                }

                @Override
                protected APIResult doExecute(String colo) throws FalconException {
                    return getConfigSyncChannel(colo).invoke("delete", bufferedRequest,
                            type, entityName, colo);
                }
            }.execute());
        }

        if (!mergedColos.isEmpty()) {
            results.put(FALCON_TAG + "/update", new EntityProxy(type, entityName) {
                @Override
                protected Set<String> getColosToApply() {
                    return mergedColos;
                }

                @Override
                protected APIResult doExecute(String colo) throws FalconException {
                    return getConfigSyncChannel(colo).invoke("update", bufferedRequest,
                            type, entityName,
                            colo, skipDryRun);
                }
            }.execute());
        }

        if (!newColos.isEmpty()) {
            results.put(FALCON_TAG + "/submit", new EntityProxy(type, entityName) {
                @Override
                protected Set<String> getColosToApply() {
                    return newColos;
                }

                @Override
                protected APIResult doExecute(String colo) throws FalconException {
                    return getConfigSyncChannel(colo).invoke("submit", bufferedRequest, type,
                            colo);
                }
            }.execute());
        }
        return results;
    }
}
