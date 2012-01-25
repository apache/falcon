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


import org.apache.ivory.entity.v0.dataset.Dataset;
import org.apache.ivory.oozie.coordinator.COORDINATORAPP;
import org.apache.ivory.oozie.coordinator.DATASETS;
import org.apache.ivory.oozie.coordinator.SYNCDATASET;
import org.dozer.DozerConverter;

public class CustomDefaultDatasetToCoordinatorConverter extends
		DozerConverter<Dataset, COORDINATORAPP> {
	
	private static final String PATH_TYPE = "data";

	public CustomDefaultDatasetToCoordinatorConverter() {
		super(Dataset.class, COORDINATORAPP.class);
	}

	@Override
	public COORDINATORAPP convertTo(Dataset dataset,
			COORDINATORAPP coordinatorapp) {
		
		SYNCDATASET syncdataset = new SYNCDATASET();
		syncdataset.setName(dataset.getName());
		syncdataset.setUriTemplate("${nameNode}"+dataset.getDefaults().getPaths().get(PATH_TYPE).getLocation());
		syncdataset.setFrequency("${coord:" + dataset.getDefaults().getFrequency()
				+ "(" + dataset.getDefaults().getPeriodicity() + ")}");
		syncdataset.setInitialInstance( dataset.getDefaults().getDateRange().getStart());
		syncdataset.setTimezone(dataset.getDefaults().getTimezone());
		//syncdataset.setDoneFlag("");
		if(coordinatorapp.getDatasets()==null){
			coordinatorapp.setDatasets(new DATASETS());
		}
		coordinatorapp.getDatasets().getDatasetOrAsyncDataset().add(syncdataset);
		return coordinatorapp;
	}

	@Override
	public Dataset convertFrom(COORDINATORAPP arg0, Dataset arg1) {
		// TODO Auto-generated method stub
		return null;
	}

}
