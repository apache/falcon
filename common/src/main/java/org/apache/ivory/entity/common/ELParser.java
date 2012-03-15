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
	
	private static final String L_P = "\\s*\\(\\s*";
	private static final String R_P = "\\s*\\)";
	private static final String NUM = "([-]?[\\d]+)";
	private static final String COMMA = "\\s*,\\s*";

	private static final Pattern nowPattern = Pattern.compile("now"+L_P+NUM+COMMA+NUM+R_P);
	private static final Pattern todayPattern = Pattern.compile("today"+L_P+NUM+COMMA+NUM+R_P);
	private static final Pattern yesterdayPattern = Pattern.compile("yesterday"+L_P+NUM+COMMA+NUM+R_P);
	private static final Pattern currentMonthPattern = Pattern
			.compile("currentMonth"+L_P+NUM+COMMA+NUM+COMMA+NUM+R_P);
	private static final Pattern lastMonthPattern = Pattern.compile("lastMonth"+L_P+NUM+COMMA+NUM+COMMA+NUM+R_P);
	private static final Pattern currentYearPattern = Pattern.compile("currentYear"+L_P+NUM+COMMA+NUM+COMMA+NUM+COMMA+NUM+R_P);
	private final Pattern lastYearPattern = Pattern.compile("lastYear"+L_P+NUM+COMMA+NUM+COMMA+NUM+COMMA+NUM+R_P);
	//frequency cant be negative
	private static final Pattern minutesPattern = Pattern.compile("minutes\\(([\\d]+)\\)");
	private static final Pattern hoursPattern = Pattern.compile("hours\\(([\\d]+)\\)");
	private static final Pattern daysPattern = Pattern.compile("days\\(([\\d]+)\\)");
	private static final Pattern monthsPattern = Pattern.compile("months\\(([\\d]+)\\)");

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
