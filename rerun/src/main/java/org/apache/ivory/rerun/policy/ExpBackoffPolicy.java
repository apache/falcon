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

import java.util.Date;

import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.v0.Frequency;
import org.apache.ivory.expression.ExpressionHelper;

public class ExpBackoffPolicy extends AbstractRerunPolicy {
	
	protected int power = 2;
	@Override
	public long getDelay(Frequency delay, int eventNumber)
			throws IvoryException {
		return (long) (getDurationInMilliSec(delay) * Math.pow(power,
				eventNumber));
	}

	@Override
	public long getDelay(Frequency delay, Date nominalTime, Date cutOffTime)
			throws IvoryException {
		ExpressionHelper evaluator = ExpressionHelper.get();
		Date now = new Date(System.currentTimeMillis());
		Date lateTime = new Date();
		lateTime = nominalTime;
		long delayMilliSeconds = evaluator.evaluate(delay.toString(), Long.class);
		int factor = 1;
		while(lateTime.before(now)){
			lateTime = addTime(lateTime , (int) (factor * delayMilliSeconds));
			factor*= power;
		}
		if(lateTime.after(cutOffTime))
			lateTime = cutOffTime;
		
		return  (lateTime.getTime() - nominalTime.getTime());
		
	}

}
