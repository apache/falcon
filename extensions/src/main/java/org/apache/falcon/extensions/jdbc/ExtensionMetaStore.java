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

import org.apache.falcon.extensions.ExtensionType;
import org.apache.falcon.persistence.ExtensionMetadataBean;
import org.apache.falcon.persistence.PersistenceConstants;
import org.apache.falcon.service.FalconJPAService;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.Date;
import java.util.List;

/**
 * Statestore for extension framework.
 */
public class ExtensionMetaStore {

    private EntityManager getEntityManager() {
        return FalconJPAService.get().getEntityManager();
    }

    public void storeExtensionMetadataBean(String extensionName, String location, ExtensionType extensionType,
                                           String description){
        ExtensionMetadataBean extensionMetadataBean = new ExtensionMetadataBean();
        extensionMetadataBean.setLocation(location);
        extensionMetadataBean.setExtensionName(extensionName);
        extensionMetadataBean.setExtensionType(extensionType.toString());
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

    public Boolean checkIfExtensionExists(String extensionName){
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_EXTENSION);
        q.setParameter("extensionName", extensionName);
        if (q.getResultList().size() > 0){
            return true;
        }
        return false;
    }

    public List<ExtensionMetadataBean> getAllExtensions(){
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_ALL_EXTENSIONS);
        try {
            return q.getResultList();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public void deleteExtensionsOfType(ExtensionType extensionType){
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.DELETE_EXTENSIONS_OF_TYPE);
        q.setParameter("extensionType", extensionType.toString());
        try{
            q.executeUpdate();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public ExtensionMetadataBean getDetail(String extensionName){
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_EXTENSION);
        q.setParameter("extensionName", extensionName);
        try {
            return (ExtensionMetadataBean)q.getSingleResult();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public void deleteExtensionMetadata(String extensionName){
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.DELETE_EXTENSION);
        q.setParameter("extensionName", extensionName);
        try{
            q.executeUpdate();
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
