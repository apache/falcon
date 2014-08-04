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

INSERT OVERWRITE TABLE ${falcon_outputData_database}.${falcon_outputData_table} PARTITION ${falcon_inputData_filter}
 SELECT id, value FROM ${falcon_inputData_database}.${falcon_inputData_table} WHERE ${falcon_inputData_filter} UNION ALL
 SELECT id, value FROM ${falcon_inputData2_database}.${falcon_inputData2_table} WHERE ${falcon_inputData2_filter};
INSERT OVERWRITE TABLE ${falcon_outputData2_database}.${falcon_outputData2_table} PARTITION ${falcon_inputData_filter}
 SELECT id, value FROM ${falcon_inputData_database}.${falcon_inputData_table} WHERE ${falcon_inputData_filter} UNION ALL
 SELECT id, value FROM ${falcon_inputData2_database}.${falcon_inputData2_table} WHERE ${falcon_inputData2_filter};
