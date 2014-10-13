# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

HDFS Directory Replication Recipe

Overview
This recipe implements replicating arbitrary directories on HDFS from one
Hadoop cluster to another Hadoop cluster.
This piggy backs on replication solution in Falcon which uses the DistCp tool.

Use Case
* Copy directories between HDFS clusters with out dated partitions
* Archive directories from HDFS to Cloud. Ex: S3, Azure WASB

Limitations
As the data volume and number of files grow, this can get inefficient.
