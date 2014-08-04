--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
DROP TABLE IF EXISTS ${falcon_outputData_database}.temp_table_on_raw_data;

CREATE EXTERNAL TABLE ${falcon_outputData_database}.temp_table_on_raw_data(id STRING, value STRING)
 LOCATION '${inputData}';

INSERT OVERWRITE TABLE ${falcon_outputData_database}.${falcon_outputData_table}
 PARTITION (dt='${falcon_outputData_dated_partition_value}')
 SELECT id, value FROM temp_table_on_raw_data;

DROP TABLE IF EXISTS ${falcon_outputData_database}.temp_table_on_raw_data;