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
package org.apache.falcon.entity;

import org.apache.falcon.Pair;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Convenient builder for workflow name.
 * @param <T>
 */
public class WorkflowNameBuilder<T extends Entity> {
    private static final String PREFIX = "FALCON";

    // Oozie JMS message property name that holds the workflow app name
    private static final String OOZIE_JMS_MSG_APPNAME_PROP = "appName";

    private T entity;
    private Tag tag;
    private List<String> suffixes;

    public WorkflowNameBuilder(T entity) {
        this.entity = entity;
    }

    public void setTag(Tag tag) {
        this.tag = tag;
    }

    public void setSuffixes(List<String> suffixes) {
        this.suffixes = suffixes;
    }

    public WorkflowName getWorkflowName() {
        return new WorkflowName(PREFIX, entity.getEntityType().name(),
                tag == null ? null : tag.name(), entity.getName(),
                suffixes == null ? new ArrayList<String>() : suffixes);
    }

    public Tag getWorkflowTag(String workflowName) {
        return WorkflowName.getTagAndSuffixes(workflowName) == null ? null
                : WorkflowName.getTagAndSuffixes(workflowName).first;
    }

    public String getWorkflowSuffixes(String workflowName) {
        return WorkflowName.getTagAndSuffixes(workflowName) == null ? ""
                : WorkflowName.getTagAndSuffixes(workflowName).second;
    }

    /**
     * Workflow name.
     */
    public static class WorkflowName {
        private static final String SEPARATOR = "_";
        private static final Pattern WF_NAME_PATTERN;

        private String prefix;
        private String entityType;
        private String tag;
        private String entityName;
        private List<String> suffixes;

        static {
            StringBuilder typePattern = new StringBuilder("(");
            for (EntityType type : EntityType.values()) {
                typePattern.append(type.name());
                typePattern.append("|");
            }
            typePattern = typePattern.deleteCharAt(typePattern.length() - 1);
            typePattern.append(")");
            StringBuilder tagsPattern = new StringBuilder("(");
            for (Tag tag : Tag.values()) {
                tagsPattern.append(tag.name());
                tagsPattern.append("|");
            }
            tagsPattern = tagsPattern.deleteCharAt(tagsPattern.length() - 1);
            tagsPattern.append(")");

            String name = "([a-zA-Z][\\-a-zA-Z0-9]*)";

            String suffix = "([_A-Za-z0-9-.]*)";

            String namePattern = PREFIX + SEPARATOR + typePattern + SEPARATOR + tagsPattern
                    + SEPARATOR + name + suffix;

            WF_NAME_PATTERN = Pattern.compile(namePattern);
        }

        public WorkflowName(String prefix, String entityType, String tag,
                            String entityName, List<String> suffixes) {
            this.prefix = prefix;
            this.entityType = entityType;
            this.tag = tag;
            this.entityName = entityName;
            this.suffixes = suffixes;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append(prefix).append(SEPARATOR).append(entityType)
                    .append(tag == null ? "" : SEPARATOR + tag)
                    .append(SEPARATOR).append(entityName);

            for (String suffix : suffixes) {
                builder.append(SEPARATOR).append(suffix);
            }

            return builder.toString();
        }

        public static Pair<Tag, String> getTagAndSuffixes(String workflowName) {
            Matcher matcher = WF_NAME_PATTERN.matcher(workflowName);
            if (matcher.matches()) {
                matcher.reset();
                if (matcher.find()) {
                    String tag = matcher.group(2);
                    String suffixes = matcher.group(4);
                    return new Pair<>(Tag.valueOf(tag), suffixes);
                }
            }
            return null;
        }

        public static Pair<String, EntityType> getEntityNameAndType(String workflowName) {
            Matcher matcher = WF_NAME_PATTERN.matcher(workflowName);
            if (matcher.matches()) {
                matcher.reset();
                if (matcher.find()) {
                    String type = matcher.group(1);
                    String name = matcher.group(3);
                    return new Pair<>(name, EntityType.valueOf(type));
                }
            }
            return null;
        }

        public static String getJMSFalconSelector() {
            return String.format("%s like '%s%s%%'", OOZIE_JMS_MSG_APPNAME_PROP, PREFIX, SEPARATOR);
        }
    }
}
