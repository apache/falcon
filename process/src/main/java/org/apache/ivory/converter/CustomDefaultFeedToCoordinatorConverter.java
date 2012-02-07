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

package org.apache.ivory.converter;

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


import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.feed.LocationType;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.apache.ivory.oozie.coordinator.DATASETS;
import org.apache.ivory.oozie.coordinator.SYNCDATASET;
import org.dozer.DozerConverter;

public class CustomDefaultFeedToCoordinatorConverter extends
		DozerConverter<Feed, COORDINATORAPP> {
	
	private static final String PATH_TYPE = "data";

	public CustomDefaultFeedToCoordinatorConverter() {
		super(Feed.class, COORDINATORAPP.class);
	}

	//TODO revisit this section to populate more stuff
	@Override
	public COORDINATORAPP convertTo(Feed feed,
			COORDINATORAPP coordinatorapp) {
		
		SYNCDATASET syncdataset = new SYNCDATASET();
		syncdataset.setName(feed.getName());
		syncdataset.setUriTemplate("${nameNode}"+feed.getLocations().get(LocationType.DATA).getPath());
		syncdataset.setFrequency("${coord:" + feed.getFrequency()
				+ "(" + feed.getPeriodicity() + ")}");
		//TODO get this value based on the referenced cluster in process
		syncdataset.setInitialInstance(feed.getClusters().getCluster().get(0).getValidity().getStart());
		syncdataset.setTimezone(feed.getClusters().getCluster().get(0).getValidity().getTimezone());
		//syncdataset.setDoneFlag("");
		if(coordinatorapp.getDatasets()==null){
			coordinatorapp.setDatasets(new DATASETS());
		}
		coordinatorapp.getDatasets().getDatasetOrAsyncDataset().add(syncdataset);
		return coordinatorapp;
	}

	@Override
	public Feed convertFrom(COORDINATORAPP arg0, Feed arg1) {
		// TODO Auto-generated method stub
		return null;
	}

}
