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
package org.apache.ivory.logging;

import org.testng.annotations.Test;


public class LoggingParserTest {

	@Test(enabled=false)
	public void testLogParser() throws Exception{

		LogMover mover = new LogMover();
		mover.run(new String[]{"http://10.14.117.33:11000/oozie/","0000060-120424164133213-oozie-rish-W@user-workflow","0","/log/","agregator-coord16-11d4ae80-6d2a-4a55-92b6-c214983856ef/LATE1/2012-04-24T17:25Z","SUCCESS"} );
		//mover.run(new String[]{"rmc-daily-wf","2012-04-24-04-10","http://oozie.red.ua2.inmobi.com:11000/oozie/","0003080-120412101016717-oozie-oozi-W@user-workflow","1","hdfs://nn.red.ua2.inmobi.com:54310/projects/ivory/staging/ivory/workflows/process/rmc-daily/2012-04-19T10.02.39.688/tmp"} );

	}
	
}
