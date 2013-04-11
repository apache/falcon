/**
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
package org.apache.falcon.rerun.policy;

import java.util.Date;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.expression.ExpressionHelper;

public class ExpBackoffPolicy extends AbstractRerunPolicy {

	@Override
	public long getDelay(Frequency delay, int eventNumber)
			throws FalconException {
		return (long) (getDurationInMilliSec(delay) * Math.pow(getPower(),
				eventNumber));
	}

	@Override
	public long getDelay(Frequency delay, Date nominalTime, Date cutOffTime)
			throws FalconException {
		ExpressionHelper evaluator = ExpressionHelper.get();
		Date now = new Date();
		Date lateTime = nominalTime;
		long delayMilliSeconds = evaluator.evaluate(delay.toString(),
				Long.class);
		int factor = 1;
		// TODO we can get rid of this using formula
		while (lateTime.compareTo(now)<=0) {
			lateTime = addTime(lateTime, (int) (factor * delayMilliSeconds));
			factor *= getPower();
		}
		if (lateTime.after(cutOffTime))
			lateTime = cutOffTime;
		return (lateTime.getTime() - nominalTime.getTime());

	}

	protected int getPower() {
		return 2;
	}

}
