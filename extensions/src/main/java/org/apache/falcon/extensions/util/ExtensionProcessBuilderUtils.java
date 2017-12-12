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

package org.apache.falcon.extensions.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.process.ACL;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.EngineType;
import org.apache.falcon.entity.v0.process.Notification;
import org.apache.falcon.entity.v0.process.PolicyType;
import org.apache.falcon.entity.v0.process.Property;
import org.apache.falcon.entity.v0.process.Retry;
import org.apache.falcon.entity.v0.process.Workflow;
import org.apache.falcon.extensions.ExtensionProperties;
import org.apache.falcon.security.SecurityUtil;
import org.apache.falcon.util.NotificationType;

import javax.xml.bind.Unmarshaller;
import javax.xml.bind.ValidationEvent;
import javax.xml.bind.ValidationEventHandler;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Extension builder utility.
 */
public final class ExtensionProcessBuilderUtils {

    private static final Pattern EXTENSION_VAR_PATTERN = Pattern.compile("##[A-Za-z0-9_.]*##");

    private ExtensionProcessBuilderUtils() {
    }

    public static Entity createProcessFromTemplate(final String processTemplate,
                                                   final String extensionName,
                                                   final Properties extensionProperties,
                                                   final String wfPath,
                                                   final String wfLibPath) throws FalconException {
        if (StringUtils.isBlank(processTemplate) || StringUtils.isBlank(extensionName)
                || extensionProperties == null || StringUtils.isBlank(wfPath)) {
            throw new FalconException("Invalid arguments passed to extension builder");
        }
        org.apache.falcon.entity.v0.process.Process process = bindAttributesInTemplate(
                processTemplate, extensionProperties, extensionName, wfPath, wfLibPath);

        validateGeneratedProcess(process.toString());
        return process;
    }

    private static org.apache.falcon.entity.v0.process.Process
    bindAttributesInTemplate(final String processTemplate, final Properties extensionProperties,
                             final String extensionName, final String wfPath,
                             final String wfLibPath)
        throws FalconException {
        if (StringUtils.isBlank(processTemplate) || extensionProperties == null) {
            throw new FalconException("Process template or properties cannot be null");
        }

        org.apache.falcon.entity.v0.process.Process process;
        try {
            Unmarshaller unmarshaller = EntityType.PROCESS.getUnmarshaller();
            // Validation can be skipped for unmarshalling as we want to bind template with the properties.
            // Vaildation is handled as part of marshalling
            unmarshaller.setSchema(null);
            unmarshaller.setEventHandler(new ValidationEventHandler() {
                    public boolean handleEvent(ValidationEvent validationEvent) {
                        return true;
                    }
                }
            );
            XMLInputFactory xif = SchemaHelper.createXmlInputFactory();
            XMLStreamReader xsr = xif.createXMLStreamReader(new StringReader(processTemplate));
            process = (org.apache.falcon.entity.v0.process.Process)
                    unmarshaller.unmarshal(xsr);
        } catch (Exception e) {
            throw new FalconException(e);
        }

        /* For optional properties user might directly set them in the process xml and might not set it in properties
           file. Before doing the submission validation is done to confirm process xml doesn't have
           EXTENSION_VAR_PATTERN
        */

        String processName = extensionProperties.getProperty(ExtensionProperties.JOB_NAME.getName());
        if (StringUtils.isNotEmpty(processName)) {
            process.setName(processName);
        }

        // DR process template has only one cluster
        bindClusterProperties(process.getClusters().getClusters().get(0), extensionProperties);

        // bind scheduling properties
        String processFrequency = extensionProperties.getProperty(ExtensionProperties.FREQUENCY.getName());
        if (StringUtils.isNotEmpty(processFrequency)) {
            process.setFrequency(Frequency.fromString(processFrequency));
        }

        String zone = extensionProperties.getProperty(ExtensionProperties.TIMEZONE.getName());
        if (StringUtils.isNotBlank(zone)) {
            process.setTimezone(TimeZone.getTimeZone(zone));
        } else {
            process.setTimezone(TimeZone.getTimeZone("UTC"));
        }

        bindWorkflowProperties(process.getWorkflow(), extensionName, wfPath, wfLibPath);
        bindRetryProperties(process.getRetry(), extensionProperties);
        bindNotificationProperties(process.getNotification(), extensionProperties);
        bindACLProperties(process.getACL(), extensionProperties);
        bindTagsProperties(process, extensionProperties);
        bindCustomProperties(process.getProperties(), extensionProperties);

        return process;
    }

    private static void bindClusterProperties(final Cluster cluster,
                                              final Properties extensionProperties) {
        String clusterName = extensionProperties.getProperty(ExtensionProperties.CLUSTER_NAME.getName());
        if (StringUtils.isNotEmpty(clusterName)) {
            cluster.setName(clusterName);
        }
        String clusterStartValidity = extensionProperties.getProperty(ExtensionProperties.VALIDITY_START.getName());
        if (StringUtils.isNotEmpty(clusterStartValidity)) {
            cluster.getValidity().setStart(SchemaHelper.parseDateUTC(clusterStartValidity));
        }

        String clusterEndValidity = extensionProperties.getProperty(ExtensionProperties.VALIDITY_END.getName());
        if (StringUtils.isNotEmpty(clusterEndValidity)) {
            cluster.getValidity().setEnd(SchemaHelper.parseDateUTC(clusterEndValidity));
        }
    }

    private static void bindWorkflowProperties(final Workflow wf,
                                               final String extensionName,
                                               final String wfPath,
                                               final String wfLibPath) {
        final EngineType defaultEngineType = EngineType.OOZIE;
        final String workflowNameSuffix = "-workflow";

        wf.setName(extensionName + workflowNameSuffix);
        wf.setEngine(defaultEngineType);
        wf.setPath(wfPath);
        if (StringUtils.isNotEmpty(wfLibPath)) {
            wf.setLib(wfLibPath);
        } else {
            wf.setLib("");
        }
    }

    private static void bindRetryProperties(final Retry processRetry,
                                            final Properties extensionProperties) {
        final PolicyType defaultRetryPolicy = PolicyType.PERIODIC;
        final int defaultRetryAttempts = 3;
        final Frequency defaultRetryDelay = new Frequency("minutes(30)");

        String retryPolicy = extensionProperties.getProperty(ExtensionProperties.RETRY_POLICY.getName());
        if (StringUtils.isNotBlank(retryPolicy)) {
            processRetry.setPolicy(PolicyType.fromValue(retryPolicy));
        } else {
            processRetry.setPolicy(defaultRetryPolicy);
        }

        String retryAttempts = extensionProperties.getProperty(ExtensionProperties.RETRY_ATTEMPTS.getName());
        if (StringUtils.isNotBlank(retryAttempts)) {
            processRetry.setAttempts(Integer.parseInt(retryAttempts));
        } else {
            processRetry.setAttempts(defaultRetryAttempts);
        }

        String retryDelay = extensionProperties.getProperty(ExtensionProperties.RETRY_DELAY.getName());
        if (StringUtils.isNotBlank(retryDelay)) {
            processRetry.setDelay(Frequency.fromString(retryDelay));
        } else {
            processRetry.setDelay(defaultRetryDelay);
        }

        String retryOnTimeout = extensionProperties.getProperty(ExtensionProperties.RETRY_ON_TIMEOUT.getName());
        if (StringUtils.isNotBlank(retryOnTimeout)) {
            processRetry.setOnTimeout(Boolean.valueOf(retryOnTimeout));
        } else {
            processRetry.setOnTimeout(false);
        }
    }

    private static void bindNotificationProperties(final Notification processNotification,
                                                   final Properties extensionProperties) {
        final String defaultNotificationType = NotificationType.EMAIL.getName();

        String notificationType = extensionProperties.getProperty(
                ExtensionProperties.JOB_NOTIFICATION_TYPE.getName());
        if (StringUtils.isNotBlank(notificationType)) {
            processNotification.setType(notificationType);
        } else {
            processNotification.setType(defaultNotificationType);
        }

        String notificationAddress = extensionProperties.getProperty(
                ExtensionProperties.JOB_NOTIFICATION_ADDRESS.getName());
        if (StringUtils.isNotBlank(notificationAddress)) {
            processNotification.setTo(notificationAddress);
        } else {
            processNotification.setTo("NA");
        }
    }

    private static void bindACLProperties(final ACL acl,
                                          final Properties extensionProperties) throws FalconException {
        String aclOwner = extensionProperties.getProperty(ExtensionProperties.JOB_ACL_OWNER.getName());
        String aclGroup = extensionProperties.getProperty(ExtensionProperties.JOB_ACL_GROUP.getName());
        String aclPermission = extensionProperties.getProperty(ExtensionProperties.JOB_ACL_PERMISSION.getName());

        if (SecurityUtil.isAuthorizationEnabled() && (StringUtils.isEmpty(aclOwner) || StringUtils.isEmpty(aclGroup)
                || StringUtils.isEmpty(aclPermission))) {
            throw new FalconException("ACL extension properties cannot be null or empty when authorization is "
                    + "enabled");
        }

        if (StringUtils.isNotEmpty(aclOwner)) {
            acl.setOwner(aclOwner);
        }

        if (StringUtils.isNotEmpty(aclGroup)) {
            acl.setGroup(aclGroup);
        }

        if (StringUtils.isNotEmpty(aclPermission)) {
            acl.setPermission(aclPermission);
        }
    }

    private static void bindTagsProperties(final org.apache.falcon.entity.v0.process.Process process,
                                           final Properties extensionProperties) {
        String falconSystemTags = process.getTags();
        String tags = extensionProperties.getProperty(ExtensionProperties.JOB_TAGS.getName());
        if (StringUtils.isNotEmpty(tags)) {
            if (StringUtils.isNotEmpty(falconSystemTags)) {
                tags += ", " + falconSystemTags;
            }
            process.setTags(tags);
        }
    }


    private static void bindCustomProperties(final org.apache.falcon.entity.v0.process.Properties customProperties,
                                             final Properties extensionProperties) {
        List<Property> propertyList = new ArrayList<>();

        for (Map.Entry<Object, Object> extensionProperty : extensionProperties.entrySet()) {
            if (ExtensionProperties.getOptionsMap().get(extensionProperty.getKey().toString()) == null) {
                addProperty(propertyList, (String) extensionProperty.getKey(), (String) extensionProperty.getValue());
            }
        }

        customProperties.getProperties().addAll(propertyList);
    }

    private static void addProperty(List<Property> propertyList, String name, String value) {
        Property prop = new Property();
        prop.setName(name);
        prop.setValue(value);
        propertyList.add(prop);
    }

    private static void validateGeneratedProcess(final String generatedProcess) throws FalconException {
        if (StringUtils.isBlank(generatedProcess)) {
            throw new IllegalArgumentException("Invalid arguments passed");
        }

        Matcher matcher = EXTENSION_VAR_PATTERN.matcher(generatedProcess);
        if (matcher.find()) {
            String variable = generatedProcess.substring(matcher.start(), matcher.end());
            throw new FalconException("Match not found for the template: " + variable
                    + " in extension template file. Please add it in extension properties file");
        }
    }

}

