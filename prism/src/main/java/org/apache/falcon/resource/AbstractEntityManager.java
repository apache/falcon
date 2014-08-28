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
import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.FalconRuntimException;
import org.apache.falcon.FalconWebException;
import org.apache.falcon.Pair;
import org.apache.falcon.entity.EntityNotRegisteredException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.parser.EntityParser;
import org.apache.falcon.entity.parser.EntityParserFactory;
import org.apache.falcon.entity.parser.ValidationException;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.store.EntityAlreadyExistsException;
import org.apache.falcon.entity.v0.*;
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
import java.util.*;

/**
 * A base class for managing Entity operations.
 */
public abstract class AbstractEntityManager {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractEntityManager.class);
    private static final Logger AUDIT = LoggerFactory.getLogger("AUDIT");

    protected static final int XML_DEBUG_LEN = 10 * 1024;
    protected static final String DEFAULT_NUM_RESULTS = "10";

    private AbstractWorkflowEngine workflowEngine;
    protected ConfigurationStore configStore = ConfigurationStore.get();

    public AbstractEntityManager() {
        try {
            workflowEngine = WorkflowEngineFactory.getWorkflowEngine();
        } catch (FalconException e) {
            throw new FalconRuntimException(e);
        }
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
        return new HashSet<String>(Arrays.asList(colos));
    }

    protected Set<String> getColosFromExpression(String coloExpr, String type, String entity) {
        Set<String> colos;
        if (coloExpr == null || coloExpr.equals("*") || coloExpr.isEmpty()) {
            colos = getApplicableColos(type, entity);
        } else {
            colos = new HashSet<String>(Arrays.asList(coloExpr.split(",")));
        }
        return colos;
    }

    protected Set<String> getApplicableColos(String type, String name) {
        try {
            if (DeploymentUtil.isEmbeddedMode()) {
                return DeploymentUtil.getDefaultColos();
            }

            if (EntityType.valueOf(type.toUpperCase()) == EntityType.CLUSTER) {
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

            if (EntityType.valueOf(type.toUpperCase()) == EntityType.CLUSTER) {
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
            audit(request, "STREAMED_DATA", type, "SUBMIT");
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
            EntityType entityType = EntityType.valueOf(type.toUpperCase());
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
            EntityType entityType = EntityType.valueOf(type.toUpperCase());
            audit(request, entity, type, "DELETE");
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

    // Parallel update can get very clumsy if two feeds are updated which
    // are referred by a single process. Sequencing them.
    public synchronized APIResult update(HttpServletRequest request, String type, String entityName, String colo,
                                         String effectiveTimeStr) {
        checkColo(colo);
        try {
            EntityType entityType = EntityType.valueOf(type.toUpperCase());
            audit(request, entityName, type, "UPDATE");
            Entity oldEntity = EntityUtil.getEntity(type, entityName);
            Entity newEntity = deserializeEntity(request, entityType);
            validate(newEntity);

            validateUpdate(oldEntity, newEntity);
            configStore.initiateUpdate(newEntity);

            Date effectiveTime =
                StringUtils.isEmpty(effectiveTimeStr) ? null : EntityUtil.parseDateUTC(effectiveTimeStr);
            StringBuilder result = new StringBuilder("Updated successfully");
            //Update in workflow engine
            if (!DeploymentUtil.isPrism()) {
                Set<String> oldClusters = EntityUtil.getClustersDefinedInColos(oldEntity);
                Set<String> newClusters = EntityUtil.getClustersDefinedInColos(newEntity);
                newClusters.retainAll(oldClusters); //common clusters for update
                oldClusters.removeAll(newClusters); //deleted clusters

                for (String cluster : newClusters) {
                    Date myEffectiveTime = validateEffectiveTime(newEntity, cluster, effectiveTime);
                    result.append(getWorkflowEngine().update(oldEntity, newEntity, cluster, myEffectiveTime));
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
        }
    }

    private Date validateEffectiveTime(Entity entity, String cluster, Date effectiveTime) {
        Date start = EntityUtil.getStartTime(entity, cluster);
        Date end = EntityUtil.getEndTime(entity, cluster);
        if (effectiveTime == null || effectiveTime.before(start) || effectiveTime.after(end)) {
            return null;
        }
        return effectiveTime;
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

        EntityType entityType = EntityType.valueOf(type.toUpperCase());
        Entity entity = deserializeEntity(request, entityType);

        Entity existingEntity = configStore.get(entityType, entity.getName());
        if (existingEntity != null) {
            if (EntityUtil.equals(existingEntity, entity)) {
                return existingEntity;
            }

            throw new EntityAlreadyExistsException(
                    entity.toShortString() + " already registered with configuration store. "
                            + "Can't be submitted again. Try removing before submitting.");
        }

        validate(entity);
        configStore.publish(entityType, entity);
        LOG.info("Submit successful: ({}): {}", type, entity.getName());
        return entity;
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

    protected void audit(HttpServletRequest request, String entity, String type, String action) {
        if (request == null) {
            return; // this must be internal call from Falcon
        }
        AUDIT.info("Performed {} on {} ({}) :: {}/{}",
                action, entity, type, request.getRemoteHost(), CurrentUser.getUser());
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
            EntityType entityType = EntityType.valueOf(type.toUpperCase());
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
                                    String orderBy, Integer offset, Integer resultsPerPage) {

        HashSet<String> fields = new HashSet<String>(Arrays.asList(fieldStr.toLowerCase().split(",")));
        List<Entity> entities;
        try {
            entities = getEntities(type, "", "", "", filterBy, filterTags, orderBy, offset, resultsPerPage);
        } catch (Exception e) {
            LOG.error("Failed to get entity list", e);
            throw FalconWebException.newException(e, Response.Status.BAD_REQUEST);
        }

        return entities.size() == 0
                ? new EntityList(new Entity[]{})
                : new EntityList(buildEntityElements(fields, entities));
    }

    protected List<Entity> getEntities(String type, String startDate, String endDate, String cluster,
                                       String filterBy, String filterTags, String orderBy,
                                       int offset, int resultsPerPage) throws FalconException {
        final HashMap<String, String> filterByFieldsValues = getFilterByFieldsValues(filterBy);
        final ArrayList<String> filterByTags = getFilterByTags(filterTags);

        EntityType entityType = EntityType.valueOf(type.toUpperCase());
        Collection<String> entityNames = configStore.getEntities(entityType);
        if (entityNames == null || entityNames.isEmpty()) {
            return Collections.emptyList();
        }

        ArrayList<Entity> entities = new ArrayList<Entity>();
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

            if (filterEntityByDatesAndCluster(entity, startDate, endDate, cluster)) {
                continue;
            }

            List<String> tags = EntityUtil.getTags(entity);
            List<String> pipelines = EntityUtil.getPipelines(entity);
            String entityStatus = getStatusString(entity);

            if (filterEntity(entity, entityStatus,
                    filterByFieldsValues, filterByTags, tags, pipelines)) {
                continue;
            }
            entities.add(entity);
        }
        // Sort entities before returning a subset of entity elements.
        entities = sortEntities(entities, orderBy);

        int pageCount = getRequiredNumberOfResults(entities.size(), offset, resultsPerPage);
        if (pageCount == 0) {  // handle pagination
            return new ArrayList<Entity>();
        }

        return new ArrayList<Entity>(entities.subList(offset, (offset + pageCount)));
    }
    //RESUME CHECKSTYLE CHECK ParameterNumberCheck

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

    protected static HashMap<String, String> getFilterByFieldsValues(String filterBy) {
        //Filter the results by specific field:value
        HashMap<String, String> filterByFieldValues = new HashMap<String, String>();
        if (!StringUtils.isEmpty(filterBy)) {
            String[] fieldValueArray = filterBy.split(",");
            for (String fieldValue : fieldValueArray) {
                String[] splits = fieldValue.split(":", 2);
                String filterByField = splits[0];
                if (splits.length == 2) {
                    filterByFieldValues.put(filterByField, splits[1]);
                } else {
                    filterByFieldValues.put(filterByField, "");
                }
            }
        }
        return filterByFieldValues;
    }

    private static ArrayList<String> getFilterByTags(String filterTags) {
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
                                 HashMap<String, String> filterByFieldsValues, ArrayList<String> filterByTags,
                                 List<String> tags, List<String> pipelines) {
        if (SecurityUtil.isAuthorizationEnabled() && !isEntityAuthorized(entity)) {
            // the user who requested list query has no permission to access this entity. Skip this entity
            return true;
        }

        return !((filterByTags.size() == 0 || tags.size() == 0 || !filterEntityByTags(filterByTags, tags))
                && (filterByFieldsValues.size() == 0
                || !filterEntityByFields(entity, filterByFieldsValues, entityStatus, pipelines)));

    }

    protected boolean isEntityAuthorized(Entity entity) {
        try {
            SecurityUtil.getAuthorizationProvider().authorizeResource("entities", "list",
                    entity.getEntityType().toString(), entity.getName(), CurrentUser.getProxyUgi());
        } catch (Exception e) {
            LOG.error("Authorization failed for entity=" + entity.getName()
                    + " for user=" + CurrentUser.getUser(), e);
            return false;
        }

        return true;
    }

    private boolean filterEntityByTags(ArrayList<String> filterTagsList, List<String> tags) {
        boolean filterEntity = false;
        for (String tag : filterTagsList) {
            if (!tags.contains(tag)) {
                filterEntity = true;
                break;
            }
        }

        return filterEntity;
    }

    private boolean filterEntityByFields(Entity entity, HashMap<String, String> filterKeyVals,
                                         String status, List<String> pipelines) {
        boolean filterEntity = false;

        if (filterKeyVals.size() != 0) {
            String filterValue;
            for (Map.Entry<String, String> pair : filterKeyVals.entrySet()) {
                filterValue = pair.getValue();
                if (StringUtils.isEmpty(filterValue)) {
                    continue; // nothing to filter
                }
                EntityList.EntityFilterByFields filter =
                        EntityList.EntityFilterByFields.valueOf(pair.getKey().toUpperCase());
                switch (filter) {

                case TYPE:
                    if (!entity.getEntityType().toString().equalsIgnoreCase(filterValue)) {
                        filterEntity = true;
                    }
                    break;

                case NAME:
                    if (!entity.getName().equalsIgnoreCase(filterValue)) {
                        filterEntity = true;
                    }
                    break;

                case STATUS:
                    if (!status.equalsIgnoreCase(filterValue)) {
                        filterEntity = true;
                    }
                    break;

                case PIPELINES:
                    if (entity.getEntityType().equals(EntityType.PROCESS)
                            && !pipelines.contains(filterValue)) {
                        filterEntity = true;
                    }
                    break;

                case CLUSTER:
                    Set<String> clusters = EntityUtil.getClustersDefined(entity);
                    if (!clusters.contains(filterValue)) {
                        filterEntity = true;
                    }

                default:
                    break;
                }
                if (filterEntity) {
                    break;
                }
            }
        }
        return filterEntity;
    }

    private ArrayList<Entity> sortEntities(ArrayList<Entity> entities, String orderBy) {
        // Sort the ArrayList using orderBy param
        if (!StringUtils.isEmpty(orderBy)) {
            EntityList.EntityFieldList orderByField = EntityList.EntityFieldList.valueOf(orderBy.toUpperCase());

            switch (orderByField) {

            case TYPE:
                Collections.sort(entities, new Comparator<Entity>() {
                    @Override
                    public int compare(Entity e1, Entity e2) {
                        return e1.getEntityType().compareTo(e2.getEntityType());
                    }
                });
                break;

            case NAME:
                Collections.sort(entities, new Comparator<Entity>() {
                    @Override
                    public int compare(Entity e1, Entity e2) {
                        return e1.getName().compareTo(e2.getName());
                    }
                });
                break;

            default:
                break;
            }
        } // else no sort

        return entities;
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
            elem.pipelines = EntityUtil.getPipelines(entity);
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
            EntityType entityType = EntityType.valueOf(type.toUpperCase());
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

    protected AbstractWorkflowEngine getWorkflowEngine() {
        return this.workflowEngine;
    }
}
