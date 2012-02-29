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

package org.apache.ivory.entity.common;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.ivory.IvoryException;

/**
 * ELparser parses a EL expression and computes number of minutes elapsed
 */
public class ELParser {

	private static final String EL_NOW = "now\\(([-]?[\\d]+),([-]?[\\d]+)\\)";
	private static final String EL_TODAY = "today\\(([-]?[\\d]+),([-]?[\\d]+)\\)";
	private static final String EL_YESTERDAY = "yesterday\\(([-]?[\\d]+),([-]?[\\d]+)\\)";
	private static final String EL_CURRENT_MONTH = "currentMonth\\(([-]?[\\d]+),([-]?[\\d]+),([-]?[\\d]+)\\)";
	private static final String EL_LAST_MONTH = "lastMonth\\(([-]?[\\d]+),([-]?[\\d]+),([-]?[\\d]+)\\)";
	private static final String EL_CURRENT_YEAR = "currentYear\\(([-]?[\\d]+),([-]?[\\d]+),([-]?[\\d]+),([-]?[\\d]+)\\)";
	private static final String EL_LAST_YEAR = "lastYear\\(([-]?[\\d]+),([-]?[\\d]+),([-]?[\\d]+),([-]?[\\d]+)\\)";

	//frequency can't be negative
	private static final String EL_OOZIE_MINUTES = "minutes\\(([\\d]+)\\)";
	private static final String EL_OOZIE_HOURS = "hours\\(([\\d]+)\\)";
	private static final String EL_OOZIE_DAYS = "days\\(([\\d]+)\\)";
	private static final String EL_OOZIE_MONTHS = "months\\(([\\d]+)\\)";

	private final Pattern nowPattern = Pattern.compile(EL_NOW);
	private final Pattern todayPattern = Pattern.compile(EL_TODAY);
	private final Pattern yesterdayPattern = Pattern.compile(EL_YESTERDAY);
	private final Pattern currentMonthPattern = Pattern
			.compile(EL_CURRENT_MONTH);
	private final Pattern lastMonthPattern = Pattern.compile(EL_LAST_MONTH);
	private final Pattern currentYearPattern = Pattern.compile(EL_CURRENT_YEAR);
	private final Pattern lastYearPattern = Pattern.compile(EL_LAST_YEAR);

	private final Pattern minutesPattern = Pattern.compile(EL_OOZIE_MINUTES);
	private final Pattern hoursPattern = Pattern.compile(EL_OOZIE_HOURS);
	private final Pattern daysPattern = Pattern.compile(EL_OOZIE_DAYS);
	private final Pattern monthsPattern = Pattern.compile(EL_OOZIE_MONTHS);

	private String month;
	private String day;
	private String hour;
	private String minute;

	private long inputDuration;
	private long requiredInputDuration;
	private long feedDuration;

	public void parseElExpression(String elExpression) throws IvoryException {

		if (nowPattern.matcher(elExpression).matches()) {
			Matcher matcher = nowPattern.matcher(elExpression);
			populate2paramsELFields(matcher);
			this.requiredInputDuration = -inputDuration;
		} else if (todayPattern.matcher(elExpression).matches()) {
			Matcher matcher = todayPattern.matcher(elExpression);
			populate2paramsELFields(matcher);
			this.requiredInputDuration = 24 * 60 - inputDuration;
		} else if (yesterdayPattern.matcher(elExpression).matches()) {
			Matcher matcher = yesterdayPattern.matcher(elExpression);
			populate2paramsELFields(matcher);
			this.requiredInputDuration = 2 * 24 * 60 - inputDuration;
		} else if (currentMonthPattern.matcher(elExpression).matches()) {
			Matcher matcher = currentMonthPattern.matcher(elExpression);
			populate3paramsELFields(matcher);
			this.requiredInputDuration = 31 * 24 * 60 - inputDuration;
		} else if (lastMonthPattern.matcher(elExpression).matches()) {
			Matcher matcher = lastMonthPattern.matcher(elExpression);
			populate3paramsELFields(matcher);
			this.requiredInputDuration = 2 * 31 * 24 * 60 - inputDuration;
		} else if (currentYearPattern.matcher(elExpression).matches()) {
			Matcher matcher = currentYearPattern.matcher(elExpression);
			populate4paramsELFields(matcher);
			this.requiredInputDuration = 12 * 31 * 24 * 60 - inputDuration;
		} else if (lastYearPattern.matcher(elExpression).matches()) {
			Matcher matcher = lastYearPattern.matcher(elExpression);
			populate4paramsELFields(matcher);
			this.requiredInputDuration = 2 * 12 * 31 * 24 * 60 - inputDuration;
		} else {
			throw new IvoryException("Unable to parse EL expression: "
					+ elExpression);
		}
	}

	private void populate2paramsELFields(Matcher matcher) {
		matcher.reset();
		if (matcher.find()) {
			this.hour = matcher.group(1);
			this.minute = matcher.group(2);
			this.inputDuration = Integer.parseInt(hour) * 60
					+ Integer.parseInt(minute);
		}
	}

	private void populate3paramsELFields(Matcher matcher) {
		matcher.reset();
		if (matcher.find()) {
			this.day = matcher.group(1);
			this.hour = matcher.group(2);
			this.minute = matcher.group(3);
			this.inputDuration = Integer.parseInt(day) * 24 * 60
					+ Integer.parseInt(hour) * 60 + Integer.parseInt(minute);
		}
	}

	private void populate4paramsELFields(Matcher matcher) {
		matcher.reset();
		if (matcher.find()) {
			this.month = matcher.group(1);
			this.day = matcher.group(2);
			this.hour = matcher.group(3);
			this.minute = matcher.group(4);
			this.inputDuration = Integer.parseInt(month) * 31 * 24 * 60
					+ Integer.parseInt(day) * 24 * 60 + Integer.parseInt(hour)
					* 60 + Integer.parseInt(minute);
		}
	}

	public String getMonth() {
		return month;
	}

	public String getDay() {
		return day;
	}

	public String getHour() {
		return hour;
	}

	public String getMinute() {
		return minute;
	}

	public long getInputDuration() {
		return inputDuration;
	}

	public long getFeedDuration() {
		return feedDuration;
	}

	public long getRequiredInputDuration() {
		return requiredInputDuration;
	}

	public static void main(String[] args) throws IvoryException {
		ELParser parser = new ELParser();
		parser.parseElExpression("now(-15,-12)");
		System.out.println(parser.hour);
		System.out.println(parser.minute);

	}

	public void parseOozieELExpression(String elExpression)
			throws IvoryException {
		if (minutesPattern.matcher(elExpression).matches()) {
			Matcher matcher = minutesPattern.matcher(elExpression);
			matcher.reset();
			if (matcher.find()) {
				this.minute = matcher.group(1);
				this.feedDuration = Integer.parseInt(minute);
			}
		} else if (hoursPattern.matcher(elExpression).matches()) {
			Matcher matcher = hoursPattern.matcher(elExpression);
			matcher.reset();
			if (matcher.find()) {
				this.hour = matcher.group(1);
				this.feedDuration = Integer.parseInt(hour) * 60;
			}
		} else if (daysPattern.matcher(elExpression).matches()) {
			Matcher matcher = daysPattern.matcher(elExpression);
			matcher.reset();
			if (matcher.find()) {
				this.day = matcher.group(1);
				this.feedDuration = Integer.parseInt(day) * 24 * 60;
			}
		} else if (monthsPattern.matcher(elExpression).matches()) {
			Matcher matcher = monthsPattern.matcher(elExpression);
			matcher.reset();
			if (matcher.find()) {
				this.month = matcher.group(1);
				this.feedDuration = Integer.parseInt(month) * 31 * 24 * 60;
			}
		} else {
			throw new IvoryException("Unable to parse oozie EL expression: "
					+ elExpression);
		}
	}
}
