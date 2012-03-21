/*
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

package org.apache.ivory.aspect;

import java.util.Map;

//Message to be sent to logging system
public class ResourceMessage {

	private String action;
	private Map<String, String> dimensions;
	private Status status;
	private long executionTime;
	
	public enum Status{
		SUCCEEDED, FAILED
	}

	public ResourceMessage(String action, Map<String, String> dimensions,
			Status status, long executionTime) {
		this.action = action;
		this.dimensions = dimensions;
		this.status = status;
		this.executionTime = executionTime;
	}
	
	public String getAction() {
		return action;
	}

	public Map<String, String> getDimensions() {
		return dimensions;
	}

	public Status getStatus() {
		return status;
	}

	public long getExecutionTime() {
		return executionTime;
	}

	@Override
	public String toString() {
		return "{Action:"+action+", Dimensions:"+dimensions+", Status: "+status.name()+", Time-taken:"+executionTime+" ns}";
	}


}
