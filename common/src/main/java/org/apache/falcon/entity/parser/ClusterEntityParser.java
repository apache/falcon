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

package org.apache.falcon.entity.parser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.store.StoreAccessException;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.log4j.Logger;

public class ClusterEntityParser extends EntityParser<Cluster> {

    private static final Logger LOG = Logger.getLogger(ProcessEntityParser.class);

    public ClusterEntityParser() {
        super(EntityType.CLUSTER);
    }

    @Override
	public void validate(Cluster cluster) throws StoreAccessException,
			ValidationException { 
		if (new Path(ClusterHelper.getStorageUrl(cluster)).toUri().getScheme()==null) {
			throw new ValidationException(
					"Cannot get valid scheme for namenode from write interface of cluster: "
							+ cluster.getName());
		}
		try {
			Configuration conf = new Configuration();
			conf.set("fs.default.name", ClusterHelper.getStorageUrl(cluster));
			conf.setInt("ipc.client.connect.max.retries", 10);
			FileSystem.get(conf);
		} catch (Exception e) {
			throw new ValidationException("Invalid HDFS server or port:"
					+ ClusterHelper.getStorageUrl(cluster), e);
		}
	}

}
