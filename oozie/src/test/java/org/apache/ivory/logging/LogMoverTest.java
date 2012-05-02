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

public class LogMoverTest {

	@Test(enabled=false)
	public void testLogMover() throws Exception {
		LogMover.main(new String[] {
				"http://10.14.117.33:11000/oozie/",
				"0000715-120501035446291-oozie-rish-W@user-workflow",
				"0",
				"/user/shaik.idris/log/",
				"agregator-coord16-d0dd2a19-d517-4513-bb1d-07233ba03808-510c900c-e3fe-4a8a-9313-b1d55178f20a/DEFAULT/2010-01-02T01:00Z",
				"SUCCESS" });
	}

}
