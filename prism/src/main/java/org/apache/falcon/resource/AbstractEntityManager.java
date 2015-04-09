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

package org.apache.falcon.resource;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.FalconRuntimException;
import org.apache.falcon.FalconWebException;
import org.apache.falcon.Pair;
import org.apache.falcon.entity.EntityNotRegisteredException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.lock.MemoryLocks;
import org.apache.falcon.entity.parser.EntityParser;
import org.apache.falcon.entity.parser.EntityParserFactory;
import org.apache.falcon.entity.parser.ValidationException;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.store.EntityAlreadyExistsException;
import org.apache.falcon.entity.store.FeedLocationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityGraph;
import org.apache.falcon.entity.v0.EntityIntegrityChecker;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.resource.APIResult.Status;
import org.apache.falcon.resource.EntityList.EntityElement;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.security.SecurityUtil;
import org.apache.falcon.util.DeploymentUtil;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.falcon.workflow.WorkflowEngineFactory;
import org.apache.falcon.workflow.engine.AbstractWorkflowEngine;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A base class for managing Entity operations.
 */
public abstract class AbstractEntityManager {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractEntityManager.class);
    private static MemoryLocks memoryLocks = MemoryLocks.getInstance();

    protected static final int XML_DEBUG_LEN = 10 * 1024;
    protected static final String DEFAULT_NUM_RESULTS = "10";
    protected static final int MAX_RESULTS = getMaxResultsPerPage();

    private AbstractWorkflowEngine workflowEngine;
    protected ConfigurationStore configStore = ConfigurationStore.get();

    public AbstractEntityManager() {
        try {
            workflowEngine = WorkflowEngineFactory.getWorkflowEngine();
        } catch (FalconException e) {
            throw new FalconRuntimException(e);
        }
    }

    private static int getMaxResultsPerPage() {
        Integer result = 100;
        final String key = "webservices.default.max.results.per.page";
        String value = RuntimeProperties.get().getProperty(key, result.toString());
        try {
            result = Integer.valueOf(value);
        } catch (NumberFormatException e) {
            LOG.warn("Invalid value:{} for key:{} in runtime.properties", value, key);
        }
        return result;
    }

    protected void checkColo(String colo) {
        if (!DeploymentUtil.getCurrentColo().equals(colo)) {
            throw FalconWebException.newException(
                    "Current colo (" + DeploymentUtil.getCurrentColo() + ") is not " + colo,
                    Response.Status.BAD_REQUEST);
        }
    }

    protected Set<String> getAllColos() {
        if (DeploymentUtil.isEmbeddedMode()) {
            return DeploymentUtil.getDefaultColos();
        }
        String[] colos = RuntimeProperties.get().getProperty("all.colos", DeploymentUtil.getDefaultColo()).split(",");
        for (int i = 0; i < colos.length; i++) {
            colos[i] = colos[i].trim();
        }
        return new HashSet<String>(Arrays.asList(colos));
    }

    protected Set<String> getColosFromExpression(String coloExpr, String type, String entity) {
        Set<String> colos;
        final Set<String> applicableColos = getApplicableColos(type, entity);
        if (coloExpr == null || coloExpr.equals("*") || coloExpr.isEmpty()) {
            colos = applicableColos;
        } else {
            colos = new HashSet<String>(Arrays.asList(coloExpr.split(",")));
            if (!applicableColos.containsAll(colos)) {
                throw FalconWebException.newException("Given colos not applicable for entity operation",
                        Response.Status.BAD_REQUEST);
            }
        }
        return colos;
    }

    protected Set<String> getApplicableColos(String type, String name) {
        try {
            if (DeploymentUtil.isEmbeddedMode()) {
                return DeploymentUtil.getDefaultColos();
            }

            if (EntityType.getEnum(type) == EntityType.CLUSTER) {
                return getAllColos();
            }

            return getApplicableColos(type, EntityUtil.getEntity(type, name));
        } catch (FalconException e) {
            throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    protected Set<String> getApplicableColos(String type, Entity entity) {
        try {
            if (DeploymentUtil.isEmbeddedMode()) {
                return DeploymentUtil.getDefaultColos();
            }

            if (EntityType.getEnum(type) == EntityType.CLUSTER) {
                return getAllColos();
            }

            Set<String> clusters = EntityUtil.getClustersDefined(entity);
            Set<String> colos = new HashSet<String>();
            for (String cluster : clusters) {
                Cluster clusterEntity = EntityUtil.getEntity(EntityType.CLUSTER, cluster);
                colos.add(clusterEntity.getColo());
            }
            return colos;
        } catch (FalconException e) {
            throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    /**
     * Submit a new entity. Entities can be of type feed, process or data end
     * points. Entity definitions are validated structurally against schema and
     * subsequently for other rules before they are admitted into the system
     * <p/>
     * Entity name acts as the key and an entity once added, can't be added
     * again unless deleted.
     *
     * @param request - Servlet Request
     * @param type    - entity type - feed, process or data end point
     * @param colo    - applicable colo
     * @return result of the operation
     */
    public APIResult submit(HttpServletRequest request, String type, String colo) {

        checkColo(colo);
        try {
            Entity entity = submitInternal(request, type);
            return new APIResult(APIResult.Status.SUCCEEDED, "Submit successful (" + type + ") " + entity.getName());
        } catch (Throwable e) {
            LOG.error("Unable to persist entity object", e);
            throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    /**
     * Post an entity XML with entity type. Validates the XML which can be
     * Process, Feed or Dataendpoint
     *
     * @param type entity type
     * @return APIResule -Succeeded or Failed
     */
    public APIResult validate(HttpServletRequest request, String type) {
        try {
            EntityType entityType = EntityType.getEnum(type);
            Entity entity = deserializeEntity(request, entityType);
            validate(entity);

            //Validate that the entity can be scheduled in the cluster
            if (entity.getEntityType().isSchedulable()) {
                Set<String> clusters = EntityUtil.getClustersDefinedInColos(entity);
                for (String cluster : clusters) {
                    try {
                        getWorkflowEngine().dryRun(entity, cluster);
                    } catch (FalconException e) {
                        throw new FalconException("dryRun failed on cluster " + cluster, e);
                    }
                }
            }
            return new APIResult(APIResult.Status.SUCCEEDED,
                    "Validated successfully (" + entityType + ") " + entity.getName());
        } catch (Throwable e) {
            LOG.error("Validation failed for entity ({})", type, e);
            throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    /**
     * Deletes a scheduled entity, a deleted entity is removed completely from
     * execution pool.
     *
     * @param type   entity type
     * @param entity entity name
     * @return APIResult
     */
    public APIResult delete(HttpServletRequest request, String type, String entity, String colo) {
        checkColo(colo);
        try {
            EntityType entityType = EntityType.getEnum(type);
            String removedFromEngine = "";
            try {
                Entity entityObj = EntityUtil.getEntity(type, entity);

                canRemove(entityObj);
                if (entityType.isSchedulable() && !DeploymentUtil.isPrism()) {
                    getWorkflowEngine().delete(entityObj);
                    removedFromEngine = "(KILLED in ENGINE)";
                }

                configStore.remove(entityType, entity);
            } catch (EntityNotRegisteredException e) { // already deleted
                return new APIResult(APIResult.Status.SUCCEEDED,
                        entity + "(" + type + ") doesn't exist. Nothing to do");
            }

            return new APIResult(APIResult.Status.SUCCEEDED,
                    entity + "(" + type + ") removed successfully " + removedFromEngine);
        } catch (Throwable e) {
            LOG.error("Unable to reach workflow engine for deletion or deletion failed", e);
            throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    public APIResult update(HttpServletRequest request, String type, String entityName, String colo) {
        checkColo(colo);
        List<Entity> tokenList = null;
        try {
            EntityType entityType = EntityType.getEnum(type);
            Entity oldEntity = EntityUtil.getEntity(type, entityName);
            Entity newEntity = deserializeEntity(request, entityType);
            // KLUDGE - Until ACL is mandated entity passed should be decorated for equals check to pass
            decorateEntityWithACL(newEntity);
            validate(newEntity);

            validateUpdate(oldEntity, newEntity);
            configStore.initiateUpdate(newEntity);

            tokenList = obtainUpdateEntityLocks(oldEntity);

            StringBuilder result = new StringBuilder("Updated successfully");
            //Update in workflow engine
            if (!DeploymentUtil.isPrism()) {
                Set<String> oldClusters = EntityUtil.getClustersDefinedInColos(oldEntity);
                Set<String> newClusters = EntityUtil.getClustersDefinedInColos(newEntity);
                newClusters.retainAll(oldClusters); //common clusters for update
                oldClusters.removeAll(newClusters); //deleted clusters

                for (String cluster : newClusters) {
                    result.append(getWorkflowEngine().update(oldEntity, newEntity, cluster));
                }
                for (String cluster : oldClusters) {
                    getWorkflowEngine().delete(oldEntity, cluster);
                }
            }

            configStore.update(entityType, newEntity);

            return new APIResult(APIResult.Status.SUCCEEDED, result.toString());
        } catch (Throwable e) {
            LOG.error("Update failed", e);
            throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);
        } finally {
            ConfigurationStore.get().cleanupUpdateInit();
            releaseUpdateEntityLocks(entityName, tokenList);
        }
    }

    private List<Entity> obtainUpdateEntityLocks(Entity entity)
        throws FalconException {
        List<Entity> tokenList = new ArrayList<Entity>();

        //first obtain lock for the entity for which update is issued.
        if (memoryLocks.acquireLock(entity)) {
            tokenList.add(entity);
        } else {
            throw new FalconException("Looks like an update command is already issued for " + entity.toShortString());
        }

        //now obtain locks for all dependent entities.
        Set<Entity> affectedEntities = EntityGraph.get().getDependents(entity);
        for (Entity e : affectedEntities) {
            if (memoryLocks.acquireLock(e)) {
                tokenList.add(e);
            } else {
                LOG.error("Error while trying to acquire lock for {}. Releasing already obtained locks",
                        e.toShortString());
                throw new FalconException("There are multiple update commands running for dependent entity "
                        + e.toShortString());
            }
        }
        return tokenList;
    }

    private void releaseUpdateEntityLocks(String entityName, List<Entity> tokenList) {
        if (tokenList != null && !tokenList.isEmpty()) {
            for (Entity entity : tokenList) {
                memoryLocks.releaseLock(entity);
            }
            LOG.info("All update locks released for {}", entityName);
        } else {
            LOG.info("No locks to release for " + entityName);
        }

    }

    private void validateUpdate(Entity oldEntity, Entity newEntity) throws FalconException {
        if (oldEntity.getEntityType() != newEntity.getEntityType() || !oldEntity.equals(newEntity)) {
            throw new FalconException(
                    oldEntity.toShortString() + " can't be updated with " + newEntity.toShortString());
        }

        if (oldEntity.getEntityType() == EntityType.CLUSTER) {
            throw new FalconException("Update not supported for clusters");
        }

        String[] props = oldEntity.getEntityType().getImmutableProperties();
        for (String prop : props) {
            Object oldProp, newProp;
            try {
                oldProp = PropertyUtils.getProperty(oldEntity, prop);
                newProp = PropertyUtils.getProperty(newEntity, prop);
            } catch (Exception e) {
                throw new FalconException(e);
            }
            if (!ObjectUtils.equals(oldProp, newProp)) {
                throw new ValidationException(oldEntity.toShortString() + ": " + prop + " can't be changed");
            }
        }
    }

    private void canRemove(Entity entity) throws FalconException {
        Pair<String, EntityType>[] referencedBy = EntityIntegrityChecker.referencedBy(entity);
        if (referencedBy != null && referencedBy.length > 0) {
            StringBuilder messages = new StringBuilder();
            for (Pair<String, EntityType> ref : referencedBy) {
                messages.append(ref).append("\n");
            }
            throw new FalconException(
                    entity.getName() + "(" + entity.getEntityType() + ") cant " + "be removed as it is referred by "
                            + messages);
        }
    }

    protected synchronized Entity submitInternal(HttpServletRequest request, String type)
        throws IOException, FalconException {

        EntityType entityType = EntityType.getEnum(type);
        Entity entity = deserializeEntity(request, entityType);
        // KLUDGE - Until ACL is mandated entity passed should be decorated for equals check to pass
        decorateEntityWithACL(entity);

        Entity existingEntity = configStore.get(entityType, entity.getName());
        if (existingEntity != null) {
            if (EntityUtil.equals(existingEntity, entity)) {
                return existingEntity;
            }

            throw new EntityAlreadyExistsException(
                    entity.toShortString() + " already registered with configuration store. "
                            + "Can't be submitted again. Try removing before submitting.");
        }

        SecurityUtil.tryProxy(entity); // proxy before validating since FS/Oozie needs to be proxied
        validate(entity);
        configStore.publish(entityType, entity);
        LOG.info("Submit successful: ({}): {}", type, entity.getName());
        return entity;
    }

    /**
     * KLUDGE - Until ACL is mandated entity passed should be decorated for equals check to pass.
     * existingEntity in config store will have teh decoration and equals check fails
     * if entity passed is not decorated for checking if entity already exists.
     *
     * @param entity entity
     */
    private void decorateEntityWithACL(Entity entity) {
        if (SecurityUtil.isAuthorizationEnabled() || entity.getACL() != null) {
            return; // not necessary to decorate
        }

        final String proxyUser = CurrentUser.getUser();
        final String defaultGroupName = CurrentUser.getPrimaryGroupName();
        switch (entity.getEntityType()) {
        case CLUSTER:
            org.apache.falcon.entity.v0.cluster.ACL clusterACL =
                    new org.apache.falcon.entity.v0.cluster.ACL();
            clusterACL.setOwner(proxyUser);
            clusterACL.setGroup(defaultGroupName);
            ((org.apache.falcon.entity.v0.cluster.Cluster) entity).setACL(clusterACL);
            break;

        case FEED:
            org.apache.falcon.entity.v0.feed.ACL feedACL =
                    new org.apache.falcon.entity.v0.feed.ACL();
            feedACL.setOwner(proxyUser);
            feedACL.setGroup(defaultGroupName);
            ((org.apache.falcon.entity.v0.feed.Feed) entity).setACL(feedACL);
            break;

        case PROCESS:
            org.apache.falcon.entity.v0.process.ACL processACL =
                    new org.apache.falcon.entity.v0.process.ACL();
            processACL.setOwner(proxyUser);
            processACL.setGroup(defaultGroupName);
            ((org.apache.falcon.entity.v0.process.Process) entity).setACL(processACL);
            break;

        default:
            break;
        }
    }

    protected Entity deserializeEntity(HttpServletRequest request, EntityType entityType)
        throws IOException, FalconException {

        EntityParser<?> entityParser = EntityParserFactory.getParser(entityType);
        InputStream xmlStream = request.getInputStream();
        if (xmlStream.markSupported()) {
            xmlStream.mark(XML_DEBUG_LEN); // mark up to debug len
        }
        try {
            return entityParser.parse(xmlStream);
        } catch (FalconException e) {
            if (LOG.isDebugEnabled() && xmlStream.markSupported()) {
                try {
                    xmlStream.reset();
                    String xmlData = getAsString(xmlStream);
                    LOG.debug("XML DUMP for ({}): {}", entityType, xmlData, e);
                } catch (IOException ignore) {
                    // ignore
                }
            }
            throw e;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void validate(Entity entity) throws FalconException {
        EntityParser entityParser = EntityParserFactory.getParser(entity.getEntityType());
        entityParser.validate(entity);
    }

    private String getAsString(InputStream xmlStream) throws IOException {
        byte[] data = new byte[XML_DEBUG_LEN];
        IOUtils.readFully(xmlStream, data, 0, XML_DEBUG_LEN);
        return new String(data);
    }

    private enum EntityStatus {
        SUBMITTED, SUSPENDED, RUNNING
    }

    /**
     * Returns the status of requested entity.
     *
     * @param type  entity type
     * @param entity entity name
     * @return String
     */
    public APIResult getStatus(String type, String entity, String colo) {

        checkColo(colo);
        Entity entityObj;
        try {
            entityObj = EntityUtil.getEntity(type, entity);
            EntityType entityType = EntityType.getEnum(type);
            EntityStatus status = getStatus(entityObj, entityType);
            return new APIResult(Status.SUCCEEDED, status.name());
        } catch (FalconWebException e) {
            throw e;
        } catch (Exception e) {

            LOG.error("Unable to get status for entity {} ({})", entity, type, e);
            throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    protected EntityStatus getStatus(Entity entity, EntityType type) throws FalconException {
        EntityStatus status;

        if (type.isSchedulable()) {
            if (workflowEngine.isActive(entity)) {
                if (workflowEngine.isSuspended(entity)) {
                    status = EntityStatus.SUSPENDED;
                } else {
                    status = EntityStatus.RUNNING;
                }
            } else {
                status = EntityStatus.SUBMITTED;
            }
        } else {
            status = EntityStatus.SUBMITTED;
        }
        return status;
    }

    /**
     * Returns dependencies.
     *
     * @param type entity type
     * @param entityName entity name
     * @return EntityList
     */
    public EntityList getDependencies(String type, String entityName) {

        try {
            Entity entityObj = EntityUtil.getEntity(type, entityName);
            Set<Entity> dependents = EntityGraph.get().getDependents(entityObj);
            Entity[] dependentEntities = dependents.toArray(new Entity[dependents.size()]);
            return new EntityList(dependentEntities, entityObj);
        } catch (Exception e) {
            LOG.error("Unable to get dependencies for entityName {} ({})", entityName, type, e);
            throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);
        }
    }

    //SUSPEND CHECKSTYLE CHECK ParameterNumberCheck
    /**
     * Returns the list of entities registered of a given type.
     *
     * @param type           Only return entities of this type
     * @param fieldStr       fields that the query is interested in, separated by comma
     * @param filterBy       filter by a specific field.
     * @param filterTags     filter by these tags.
     * @param orderBy        order result by these fields.
     * @param offset         Pagination offset.
     * @param resultsPerPage Number of results that should be returned starting at the offset.
     * @return EntityList
     */
    public EntityList getEntityList(String type, String fieldStr, String filterBy, String filterTags,
                                    String orderBy, String sortOrder, Integer offset, Integer resultsPerPage,
                                    String pattern) {

        HashSet<String> fields = new HashSet<String>(Arrays.asList(fieldStr.toLowerCase().split(",")));
        validateEntityFilterByClause(filterBy);
        List<Entity> entities;
        try {
            entities = getEntities(type, "", "", "", filterBy, filterTags, orderBy, sortOrder, offset,
                    resultsPerPage, pattern);
        } catch (Exception e) {
            LOG.error("Failed to get entity list", e);
            throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);
        }

        return entities.size() == 0
                ? new EntityList(new Entity[]{})
                : new EntityList(buildEntityElements(fields, entities));
    }

    protected void validateEntityFilterByClause(String entityFilterByClause) {
        Map<String, String> filterByFieldsValues = getFilterByFieldsValues(entityFilterByClause);
        for (Map.Entry<String, String> entry : filterByFieldsValues.entrySet()) {
            try {
                EntityList.EntityFilterByFields.valueOf(entry.getKey().toUpperCase());
            } catch (IllegalArgumentException e) {
                throw FalconWebException.newInstanceException(
                        "Invalid filter key: " + entry.getKey(), Response.Status.BAD_REQUEST);
            }
        }
    }

    protected List<Entity> getEntities(String type, String startDate, String endDate, String cluster,
                                       String filterBy, String filterTags, String orderBy, String sortOrder, int offset,
                                       int resultsPerPage, String pattern) throws FalconException, IOException {
        final Map<String, String> filterByFieldsValues = getFilterByFieldsValues(filterBy);
        final List<String> filterByTags = getFilterByTags(filterTags);

        EntityType entityType = EntityType.getEnum(type);
        Collection<String> entityNames = configStore.getEntities(entityType);
        if (entityNames.isEmpty()) {
            return Collections.emptyList();
        }

        List<Entity> entities = new ArrayList<Entity>();
        for (String entityName : entityNames) {
            Entity entity;
            try {
                entity = configStore.get(entityType, entityName);
                if (entity == null) {
                    continue;
                }
            } catch (FalconException e1) {
                LOG.error("Unable to get list for entities for ({})", type, e1);
                throw FalconWebException.newException(e1, Response.Status.BAD_REQUEST);
            }

            if (SecurityUtil.isAuthorizationEnabled() && !isEntityAuthorized(entity)
                || filterEntityByDatesAndCluster(entity, startDate, endDate, cluster)) {
                // the user who requested list query has no permission to access this entity. Skip this entity
                continue;
            }
            SecurityUtil.tryProxy(entity);

            List<String> tags = EntityUtil.getTags(entity);
            List<String> pipelines = EntityUtil.getPipelines(entity);
            String entityStatus = getStatusString(entity);

            if (filterEntity(entity, entityStatus,
                    filterByFieldsValues, filterByTags, tags, pipelines)) {
                continue;
            }

            if (StringUtils.isNotBlank(pattern) && !fuzzySearch(entity.getName(), pattern)) {
                continue;
            }
            entities.add(entity);
        }
        // Sort entities before returning a subset of entity elements.
        entities = sortEntities(entities, orderBy, sortOrder);

        int pageCount = getRequiredNumberOfResults(entities.size(), offset, resultsPerPage);
        if (pageCount == 0) {  // handle pagination
            return new ArrayList<Entity>();
        }

        return new ArrayList<Entity>(entities.subList(offset, (offset + pageCount)));
    }
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

    boolean fuzzySearch(String enityName, String pattern) {
        int currentIndex = 0; // current index in pattern which is to be matched
        char[] searchPattern = pattern.toLowerCase().toCharArray();
        String name = enityName.toLowerCase();

        for (Character c : name.toCharArray()) {
            if (currentIndex < searchPattern.length && c == searchPattern[currentIndex]) {
                currentIndex++;
            }
            if (currentIndex == searchPattern.length) {
                return true;
            }
        }
        return false;
    }

    private boolean filterEntityByDatesAndCluster(Entity entity, String startDate, String endDate, String cluster)
        throws FalconException {
        if (StringUtils.isEmpty(cluster)) {
            return false; // no filtering necessary on cluster
        }
        Set<String> clusters = EntityUtil.getClustersDefined(entity);
        if (!clusters.contains(cluster)) {
            return true; // entity does not have this cluster
        }

        if (!StringUtils.isEmpty(startDate)) {
            Date parsedDate = EntityUtil.parseDateUTC(startDate);
            if (parsedDate.after(EntityUtil.getEndTime(entity, cluster))) {
                return true;
            }
        }
        if (!StringUtils.isEmpty(endDate)) {
            Date parseDate = EntityUtil.parseDateUTC(endDate);
            if (parseDate.before(EntityUtil.getStartTime(entity, cluster))) {
                return true;
            }
        }

        return false;
    }

    protected static Map<String, String> getFilterByFieldsValues(String filterBy) {
        // Filter the results by specific field:value, eliminate empty values
        Map<String, String> filterByFieldValues = new HashMap<String, String>();
        if (!StringUtils.isEmpty(filterBy)) {
            String[] fieldValueArray = filterBy.split(",");
            for (String fieldValue : fieldValueArray) {
                String[] splits = fieldValue.split(":", 2);
                String filterByField = splits[0];
                if (splits.length == 2 && !splits[1].equals("")) {
                    filterByFieldValues.put(filterByField, splits[1]);
                }
            }
        }

        return filterByFieldValues;
    }

    private static List<String> getFilterByTags(String filterTags) {
        ArrayList<String> filterTagsList = new ArrayList<String>();
        if (!StringUtils.isEmpty(filterTags)) {
            String[] splits = filterTags.split(",");
            for (String tag : splits) {
                filterTagsList.add(tag.trim());
            }
        }
        return filterTagsList;
    }

    protected String getStatusString(Entity entity) {
        String statusString;
        try {
            statusString = getStatus(entity, entity.getEntityType()).name();
        } catch (Throwable throwable) {
            // Unable to fetch statusString, setting it to unknown for backwards compatibility
            statusString = "UNKNOWN";
        }
        return statusString;
    }

    private boolean filterEntity(Entity entity, String entityStatus,
                                 Map<String, String> filterByFieldsValues, List<String> filterByTags,
                                 List<String> tags, List<String> pipelines) {
        return filterEntityByTags(filterByTags, tags)
                || filterEntityByFields(entity, filterByFieldsValues, entityStatus, pipelines);
    }

    protected boolean isEntityAuthorized(Entity entity) {
        try {
            SecurityUtil.getAuthorizationProvider().authorizeEntity(entity.getName(),
                    entity.getEntityType().toString(), entity.getACL(),
                    "list", CurrentUser.getAuthenticatedUGI());
        } catch (Exception e) {
            LOG.info("Authorization failed for entity=" + entity.getName()
                + " for user=" + CurrentUser.getUser(), e);
            return false;
        }

        return true;
    }

    private boolean filterEntityByTags(List<String> filterTagsList, List<String> tags) {
        if (filterTagsList.isEmpty()) {
            return false;
        } else if (tags.isEmpty()) {
            return true;
        }

        for (String tag : filterTagsList) {
            if (!tags.contains(tag)) {
                return true;
            }
        }

        return false;
    }

    private boolean filterEntityByFields(Entity entity, Map<String, String> filterKeyVals,
                                         String status, List<String> pipelines) {
        if (filterKeyVals.isEmpty()) {
            return false;
        }

        for (Map.Entry<String, String> pair : filterKeyVals.entrySet()) {
            EntityList.EntityFilterByFields filter =
                    EntityList.EntityFilterByFields.valueOf(pair.getKey().toUpperCase());
            if (isEntityFiltered(entity, filter, pair, status, pipelines)) {
                return true;
            }
        }

        return false;
    }

    private boolean isEntityFiltered(Entity entity, EntityList.EntityFilterByFields filter,
                                     Map.Entry<String, String> pair,
                                     String status, List<String> pipelines) {
        switch (filter) {
        case TYPE:
            return !entity.getEntityType().toString().equalsIgnoreCase(pair.getValue());

        case NAME:
            return !entity.getName().equalsIgnoreCase(pair.getValue());

        case STATUS:
            return !status.equalsIgnoreCase(pair.getValue());

        case PIPELINES:
            if (!entity.getEntityType().equals(EntityType.PROCESS)) {
                throw FalconWebException.newException(
                        "Invalid filterBy key for non process entities " + pair.getKey(),
                        Response.Status.BAD_REQUEST);
            }
            return !pipelines.contains(pair.getValue());

        case CLUSTER:
            return !EntityUtil.getClustersDefined(entity).contains(pair.getValue());

        default:
            return false;
        }
    }

    private List<Entity> sortEntities(List<Entity> entities, String orderBy, String sortOrder) {
        // Sort the ArrayList using orderBy param
        if (!StringUtils.isEmpty(orderBy)) {
            EntityList.EntityFieldList orderByField = EntityList.EntityFieldList.valueOf(orderBy.toUpperCase());
            final String order = getValidSortOrder(sortOrder, orderBy);
            switch (orderByField) {

            case NAME:
                Collections.sort(entities, new Comparator<Entity>() {
                    @Override
                    public int compare(Entity e1, Entity e2) {
                        return (order.equalsIgnoreCase("asc")) ? e1.getName().compareTo(e2.getName())
                                : e2.getName().compareTo(e1.getName());
                    }
                });
                break;

            default:
                break;
            }
        } // else no sort

        return entities;
    }

    protected String getValidSortOrder(String sortOrder, String orderBy) {
        if (StringUtils.isEmpty(sortOrder)) {
            return (orderBy.equalsIgnoreCase("starttime")
                    || orderBy.equalsIgnoreCase("endtime")) ? "desc" : "asc";
        }

        if (sortOrder.equalsIgnoreCase("asc") || sortOrder.equalsIgnoreCase("desc")) {
            return sortOrder;
        }

        String err = "Value for param sortOrder should be \"asc\" or \"desc\". It is  : " + sortOrder;
        LOG.error(err);
        throw FalconWebException.newException(err, Response.Status.BAD_REQUEST);
    }

    protected int getRequiredNumberOfResults(int arraySize, int offset, int numresults) {
        /* Get a subset of elements based on offset and count. When returning subset of elements,
              elements[offset] is included. Size 10, offset 10, return empty list.
              Size 10, offset 5, count 3, return elements[5,6,7].
              Size 10, offset 5, count >= 5, return elements[5,6,7,8,9]
              return elements starting from elements[offset] until the end OR offset+numResults*/

        if (numresults < 1) {
            LOG.error("Value for param numResults should be > than 0  : {}", numresults);
            throw FalconWebException.newException("Value for param numResults should be > than 0  : " + numresults,
                    Response.Status.BAD_REQUEST);
        }

        if (offset < 0) { offset = 0; }

        if (offset >= arraySize || arraySize == 0) {
            // No elements to return
            return 0;
        }

        numresults = numresults <= MAX_RESULTS ? numresults : MAX_RESULTS;
        int retLen = arraySize - offset;
        if (retLen > numresults) {
            retLen = numresults;
        }
        return retLen;
    }

    private EntityElement[] buildEntityElements(HashSet<String> fields, List<Entity> entities) {
        EntityElement[] elements = new EntityElement[entities.size()];
        int elementIndex = 0;
        for (Entity entity : entities) {
            elements[elementIndex++] = getEntityElement(entity, fields);
        }
        return elements;
    }

    private EntityElement getEntityElement(Entity entity, HashSet<String> fields) {
        EntityElement elem = new EntityElement();
        elem.type = entity.getEntityType().toString();
        elem.name = entity.getName();
        if (fields.contains("status")) {
            elem.status = getStatusString(entity);
        }
        if (fields.contains("pipelines")) {
            elem.pipeline = EntityUtil.getPipelines(entity);
        }
        if (fields.contains("tags")) {
            elem.tag = EntityUtil.getTags(entity);
        }

        return elem;
    }

    /**
     * Returns the entity definition as an XML based on name.
     *
     * @param type       entity type
     * @param entityName entity name
     * @return String
     */
    public String getEntityDefinition(String type, String entityName) {
        try {
            EntityType entityType = EntityType.getEnum(type);
            Entity entity = configStore.get(entityType, entityName);
            if (entity == null) {
                throw new NoSuchElementException(entityName + " (" + type + ") not found");
            }
            return entity.toString();
        } catch (Throwable e) {
            LOG.error("Unable to get entity definition from config store for ({}): {}", type, entityName, e);
            throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);

        }
    }


    /**
     * Given the location of data, returns the feed.
     * @param type type of the entity, is valid only for feeds.
     * @param instancePath location of the data
     * @return Feed Name, type of the data and cluster name.
     */
    public FeedLookupResult reverseLookup(String type, String instancePath) {
        try {
            EntityType entityType = EntityType.getEnum(type);
            if (entityType != EntityType.FEED) {
                LOG.error("Reverse Lookup is not supported for entitytype: {}", type);
                throw new IllegalArgumentException("Reverse lookup is not supported for " + type);
            }

            instancePath = StringUtils.trim(instancePath);
            String instancePathWithoutSlash =
                    instancePath.endsWith("/") ? StringUtils.removeEnd(instancePath, "/") : instancePath;
            // treat strings with and without trailing slash as same for purpose of searching e.g.
            // /data/cas and /data/cas/ should be treated as same.
            String instancePathWithSlash = instancePathWithoutSlash + "/";
            FeedLocationStore store = FeedLocationStore.get();
            Collection<FeedLookupResult.FeedProperties> feeds = new ArrayList<>();
            Collection<FeedLookupResult.FeedProperties> res = store.reverseLookup(instancePathWithoutSlash);
            if (res != null) {
                feeds.addAll(res);
            }
            res = store.reverseLookup(instancePathWithSlash);
            if (res != null) {
                feeds.addAll(res);
            }
            FeedLookupResult result = new FeedLookupResult(APIResult.Status.SUCCEEDED, "SUCCESS");
            FeedLookupResult.FeedProperties[] props = feeds.toArray(new FeedLookupResult.FeedProperties[0]);
            result.setElements(props);
            return result;

        } catch (IllegalArgumentException e) {
            throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);
        } catch (Throwable throwable) {
            LOG.error("reverse look up failed", throwable);
            throw FalconWebException.newException(throwable, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }


    protected AbstractWorkflowEngine getWorkflowEngine() {
        return this.workflowEngine;
    }

    protected <T extends APIResult> T consolidateResult(Map<String, T> results, Class<T> clazz) {
        if (results == null || results.isEmpty()) {
            return null;
        }

        StringBuilder message = new StringBuilder();
        StringBuilder requestIds = new StringBuilder();
        List instances = new ArrayList();
        int statusCount = 0;
        for (Map.Entry<String, T> entry : results.entrySet()) {
            String colo = entry.getKey();
            T result = results.get(colo);
            message.append(colo).append('/').append(result.getMessage()).append('\n');
            requestIds.append(colo).append('/').append(result.getRequestId()).append('\n');
            statusCount += result.getStatus().ordinal();

            if (result.getCollection() == null) {
                continue;
            }
            Collections.addAll(instances, result.getCollection());
        }
        Object[] arrInstances = instances.toArray();
        APIResult.Status status = (statusCount == 0) ? APIResult.Status.SUCCEEDED
                : ((statusCount == results.size() * 2) ? APIResult.Status.FAILED : APIResult.Status.PARTIAL);
        try {
            Constructor<T> constructor = clazz.getConstructor(Status.class, String.class);
            T result = constructor.newInstance(status, message.toString());
            result.setCollection(arrInstances);
            result.setRequestId(requestIds.toString());
            return result;
        } catch (Exception e) {
            throw new FalconRuntimException("Unable to consolidate result.", e);
        }
    }
}
