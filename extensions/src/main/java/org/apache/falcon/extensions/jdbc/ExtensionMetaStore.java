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

import org.apache.falcon.extensions.ExtensionStatus;
import org.apache.falcon.extensions.ExtensionType;
import org.apache.falcon.extensions.store.ExtensionStore;
import org.apache.falcon.persistence.ExtensionBean;
import org.apache.falcon.persistence.ExtensionJobsBean;
import org.apache.falcon.persistence.PersistenceConstants;
import org.apache.falcon.service.FalconJPAService;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Statestore for extension framework.
 */
public class ExtensionMetaStore {

    private static final String EXTENSION_NAME = "extensionName";
    private static final String JOB_NAME = "jobName";
    private static final String EXTENSION_TYPE = "extensionType";
    private static final String EXTENSION_STATUS = "extensionStatus";

    private EntityManager getEntityManager() {
        return FalconJPAService.get().getEntityManager();
    }

    public void storeExtensionBean(String extensionName, String location, ExtensionType extensionType,
                                   String description, String extensionOwner) {
        ExtensionBean extensionBean = new ExtensionBean();
        extensionBean.setLocation(location);
        extensionBean.setExtensionName(extensionName);
        extensionBean.setExtensionType(extensionType);
        extensionBean.setCreationTime(new Date(System.currentTimeMillis()));
        extensionBean.setDescription(description);
        extensionBean.setExtensionOwner(extensionOwner);
        extensionBean.setStatus(ExtensionStatus.ENABLED);
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
        int resultSize = 0;
        try {
            resultSize = q.getResultList().size();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
        return resultSize > 0;
    }

    public Boolean checkIfExtensionJobExists(String jobName) {
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_EXTENSION_JOB);
        q.setParameter(JOB_NAME, jobName);
        int resultSize = 0;
        try {
            resultSize = q.getResultList().size();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
        return resultSize > 0;
    }

    public List<ExtensionBean> getAllExtensions() {
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_ALL_EXTENSIONS);
        try {
            return (List<ExtensionBean>) q.getResultList();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public void deleteExtensionsOfType(ExtensionType extensionType) {
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.DELETE_EXTENSIONS_OF_TYPE);
        q.setParameter(EXTENSION_TYPE, extensionType);
        try {
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
            List resultList = q.getResultList();
            if (!resultList.isEmpty()) {
                return (ExtensionBean) resultList.get(0);
            } else {
                return null;
            }
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public List<ExtensionJobsBean> getJobsForAnExtension(String extensionName) {
        List<ExtensionJobsBean> extensionJobs = new ArrayList<>();
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query query = entityManager.createNamedQuery(PersistenceConstants.GET_JOBS_FOR_AN_EXTENSION);
        query.setParameter(EXTENSION_NAME, extensionName);
        try {
            extensionJobs.addAll(query.getResultList());
            return extensionJobs;
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public void deleteExtension(String extensionName) {
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.DELETE_EXTENSION);
        q.setParameter(EXTENSION_NAME, extensionName);
        try {
            q.executeUpdate();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public void storeExtensionJob(String jobName, String extensionName, List<String> feeds, List<String> processes,
                                  byte[] config) {
        ExtensionMetaStore metaStore = ExtensionStore.getMetaStore();
        boolean alreadySubmitted = false;
        if (metaStore.getExtensionJobDetails(jobName) != null) {
            alreadySubmitted = true;
        }
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
            if (alreadySubmitted) {
                entityManager.merge(extensionJobsBean);
            } else {
                entityManager.persist(extensionJobsBean);
            }
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public void deleteExtensionJob(String jobName) {
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query query = entityManager.createNamedQuery(PersistenceConstants.DELETE_EXTENSION_JOB);
        query.setParameter(JOB_NAME, jobName);
        try {
            query.executeUpdate();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public void updateExtensionJob(String jobName, String extensionName, List<String> feedNames,
                                   List<String> processNames, byte[] configBytes) {
        EntityManager entityManager = getEntityManager();
        ExtensionJobsBean extensionJobsBean = new ExtensionJobsBean();
        extensionJobsBean.setJobName(jobName);
        extensionJobsBean.setExtensionName(extensionName);
        extensionJobsBean.setFeeds(feedNames);
        extensionJobsBean.setProcesses(processNames);
        extensionJobsBean.setConfig(configBytes);
        try {
            beginTransaction(entityManager);
            entityManager.merge(extensionJobsBean);
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public ExtensionJobsBean getExtensionJobDetails(String jobName) {
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query query = entityManager.createNamedQuery(PersistenceConstants.GET_EXTENSION_JOB);
        query.setParameter(JOB_NAME, jobName);
        List<ExtensionJobsBean> jobsBeanList;
        try {
            jobsBeanList = query.getResultList();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
        if (jobsBeanList != null && !jobsBeanList.isEmpty()) {
            return jobsBeanList.get(0);
        } else {
            return null;
        }
    }

    public List<ExtensionJobsBean> getAllExtensionJobs() {
        List<ExtensionJobsBean> extensionJobs = new ArrayList<>();
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_ALL_EXTENSION_JOBS);
        try {
            extensionJobs.addAll(q.getResultList());
            return extensionJobs;
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

    public void updateExtensionStatus(String extensionName, ExtensionStatus status) {
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.CHANGE_EXTENSION_STATUS);
        q.setParameter(EXTENSION_NAME, extensionName).setParameter(EXTENSION_STATUS, status);
        try {
            q.executeUpdate();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }
}
