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
import org.apache.falcon.persistence.ExtensionBean;
import org.apache.falcon.persistence.ExtensionJobsBean;
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

    private static final String EXTENSION_NAME = "extensionName";
    private static final String JOB_NAME = "jobName";
    private static final String EXTENSION_TYPE = "extensionType";

    private EntityManager getEntityManager() {
        return FalconJPAService.get().getEntityManager();
    }

    public void storeExtensionBean(String extensionName, String location, ExtensionType extensionType,
                                   String description){
        ExtensionBean extensionBean = new ExtensionBean();
        extensionBean.setLocation(location);
        extensionBean.setExtensionName(extensionName);
        extensionBean.setExtensionType(extensionType);
        extensionBean.setCreationTime(new Date(System.currentTimeMillis()));
        extensionBean.setDescription(description);
        EntityManager entityManager = getEntityManager();
        try {
            beginTransaction(entityManager);
            entityManager.persist(extensionBean);
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public Boolean checkIfExtensionExists(String extensionName) {
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_EXTENSION);
        q.setParameter(EXTENSION_NAME, extensionName);
        if (q.getResultList().size() > 0){
            return true;
        }
        return false;
    }

    public List<ExtensionBean> getAllExtensions() {
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_ALL_EXTENSIONS);
        try {
            return q.getResultList();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public void deleteExtensionsOfType(ExtensionType extensionType) {
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.DELETE_EXTENSIONS_OF_TYPE);
        q.setParameter(EXTENSION_TYPE, extensionType);
        try{
            q.executeUpdate();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public ExtensionBean getDetail(String extensionName) {
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_EXTENSION);
        q.setParameter(EXTENSION_NAME, extensionName);
        try {
            return (ExtensionBean)q.getSingleResult();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public void deleteExtension(String extensionName){
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.DELETE_EXTENSION);
        q.setParameter(EXTENSION_NAME, extensionName);
        try{
            q.executeUpdate();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public void storeExtensionJob(String jobName, String extensionName, List<String> feeds, List<String> processes,
                                  byte[] config) {
        ExtensionJobsBean extensionJobsBean = new ExtensionJobsBean();
        Date currentTime = new Date(System.currentTimeMillis());
        extensionJobsBean.setJobName(jobName);
        extensionJobsBean.setExtensionName(extensionName);
        extensionJobsBean.setCreationTime(currentTime);
        extensionJobsBean.setFeeds(feeds);
        extensionJobsBean.setProcesses(processes);
        extensionJobsBean.setConfig(config);
        extensionJobsBean.setLastUpdatedTime(currentTime);
        EntityManager entityManager = getEntityManager();
        try {
            beginTransaction(entityManager);
            entityManager.persist(extensionJobsBean);
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public void deleteExtensionJob(String jobName) {
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query query = entityManager.createNamedQuery(PersistenceConstants.DELETE_EXTENSION_JOB);
        query.setParameter(JOB_NAME, jobName);
        try{
            query.executeUpdate();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public List<ExtensionJobsBean> getAllExtensionJobs() {
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_ALL_EXTENSION_JOBS);
        try {
            return q.getResultList();
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
