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
package org.apache.ivory.rerun.policy;

import org.apache.ivory.IvoryException;

public abstract class AbstractRerunPolicy {

	private static final long MINUTES = 60 * 1000L;
	private static final long HOURS = 60 * MINUTES;
	private static final long DAYS = 24 * HOURS;
	private static final long MONTHS = 31 * DAYS;

	private static enum DELAYS {
		minutes, hours, days, months
	};
	
	public long getDurationInMilliSec(String delayUnit, int delay)
			throws IvoryException {

		if (delayUnit.equals(DELAYS.minutes.name())) {
			return MINUTES * delay;
		} else if (delayUnit.equals(DELAYS.hours.name())) {
			return HOURS * delay;
		} else if (delayUnit.equals(DELAYS.days.name())) {
			return DAYS * delay;
		} else if (delayUnit.equals(DELAYS.months.name())) {
			return MONTHS * delay;
		} else {
			throw new IvoryException("Unknown delayUnit:" + delayUnit);
		}
	}
	
	public abstract long getDelay(String delayUnit, int delay, int eventNumber) throws IvoryException;


}
