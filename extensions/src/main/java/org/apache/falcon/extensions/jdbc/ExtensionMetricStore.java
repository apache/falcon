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
package org.apache.falcon.extensions.jdbc;

import org.apache.falcon.persistence.ExtensionMetadataBean;
import org.apache.falcon.persistence.PersistenceConstants;
import org.apache.falcon.service.FalconJPAService;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.Date;
import java.util.List;

/**
 * Statestore for recipe framework.
 */
public class ExtensionMetricStore {

    private EntityManager getEntityManager() {
        return FalconJPAService.get().getEntityManager();
    }

    public void pubMetadataBean(String recipeName, String location, String extensionType, String description){
        ExtensionMetadataBean extensionMetadataBean = new ExtensionMetadataBean();
        extensionMetadataBean.setLocation(location);
        extensionMetadataBean.setRecipieName(recipeName);
        extensionMetadataBean.setExtensionType(extensionType);
        extensionMetadataBean.setCreationTime(new Date(System.currentTimeMillis()));
        extensionMetadataBean.setDescription(description);
        EntityManager entityManager = getEntityManager();
        try {
            beginTransaction(entityManager);
            entityManager.persist(extensionMetadataBean);
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public List<ExtensionMetadataBean> getAllRecipe(){
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_ALL_RECIPES);
        try {
            return q.getResultList();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public void deleteMetadata(){
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.DELETE_ALL_RECIPES);
        try{
            q.executeUpdate();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public String getLocation(String recipeName){
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_RECIPE_LOCATION);
        q.setParameter("recipeName", recipeName);
        try {
            return (String)q.getSingleResult();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    private void beginTransaction(EntityManager entityManager) {
        entityManager.getTransaction().begin();
    }

    private void commitAndCloseTransaction(EntityManager entityManager) {
        if (entityManager != null) {
            entityManager.getTransaction().commit();
            entityManager.close();
        }
    }
}
