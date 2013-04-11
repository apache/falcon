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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.falcon.Pair;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.v0.Entity;

public class WorkflowNameBuilder<T extends Entity> {
	private static final String PREFIX = "FALCON";

	T entity;
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
		return WorkflowName.getTagAndSuffixes(entity, workflowName) == null ? null
				: WorkflowName.getTagAndSuffixes(entity, workflowName).first;
	}

	public String getWorkflowSuffixes(String workflowName) {
		return WorkflowName.getTagAndSuffixes(entity, workflowName) == null ? ""
				: WorkflowName.getTagAndSuffixes(entity, workflowName).second;
	}

	public static class WorkflowName {
		private static final String SEPARATOR = "_";

		private String prefix;
		private String entityType;
		private String tag;
		private String entityName;
		private List<String> suffixes;

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

		public static Pair<Tag, String> getTagAndSuffixes(Entity entity,
				String workflowName) {

			StringBuilder namePattern = new StringBuilder(PREFIX + SEPARATOR
					+ entity.getEntityType().name() + SEPARATOR + "(");
			for (Tag tag : Tag.values()) {
				namePattern.append(tag.name());
				namePattern.append("|");
			}
			namePattern = namePattern.deleteCharAt(namePattern.length()-1);
			namePattern.append(")" + SEPARATOR + entity.getName()
					+ "([_A-Za-z0-9-.]*)");
			
			Pattern pattern = Pattern.compile(namePattern.toString());

			Matcher matcher = pattern.matcher(workflowName);
			if (matcher.matches()) {
				matcher.reset();
				if (matcher.find()) {
					String tag = matcher.group(1);
					String suffixes = matcher.group(2);
					return new Pair<Tag, String>(Tag.valueOf(tag), suffixes);
				}
			}
			return null;
		}
	}
}
