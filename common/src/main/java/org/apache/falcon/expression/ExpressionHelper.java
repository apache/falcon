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

package org.apache.falcon.expression;

import org.apache.commons.el.ExpressionEvaluatorImpl;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.common.FeedDataPath;
import org.apache.falcon.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
import javax.servlet.jsp.el.ExpressionEvaluator;
import javax.servlet.jsp.el.FunctionMapper;
import javax.servlet.jsp.el.VariableResolver;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper for evaluating expressions.
 */
public final class ExpressionHelper implements FunctionMapper, VariableResolver {

    private static final Logger LOG = LoggerFactory.getLogger(ExpressionHelper.class);
    private static final ExpressionHelper INSTANCE = new ExpressionHelper();

    private static final ThreadLocal<Properties> THREAD_VARIABLES = new ThreadLocal<Properties>();

    private static final Pattern SYS_PROPERTY_PATTERN = Pattern.compile("\\$\\{[A-Za-z0-9_.]+\\}");

    private static final ExpressionEvaluator EVALUATOR = new ExpressionEvaluatorImpl();
    private static final ExpressionHelper RESOLVER = ExpressionHelper.get();


    public static final ThreadLocal<SimpleDateFormat> FORMATTER = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'");
            format.setTimeZone(TimeZone.getTimeZone("UTC"));
            return format;
        }
    };

    public static ExpressionHelper get() {
        return INSTANCE;
    }

    private enum DayOfWeek {
        SUN, MON, TUE, WED, THU, FRI, SAT
    }

    private ExpressionHelper() {
    }

    public <T> T evaluate(String expression, Class<T> clazz) throws FalconException {
        return evaluateFullExpression("${" + expression + "}", clazz);
    }

    @SuppressWarnings("unchecked")
    public <T> T evaluateFullExpression(String expression, Class<T> clazz) throws FalconException {
        try {
            return (T) EVALUATOR.evaluate(expression, clazz, RESOLVER, RESOLVER);
        } catch (ELException e) {
            throw new FalconException("Unable to evaluate " + expression, e);
        }
    }

    @Override
    public Method resolveFunction(String prefix, String name) {
        for (Method method : ExpressionHelper.class.getDeclaredMethods()) {
            if (method.getName().equals(name)) {
                return method;
            }
        }
        throw new UnsupportedOperationException("Not found " + prefix + ":" + name);
    }

    public void setPropertiesForVariable(Properties properties) {
        THREAD_VARIABLES.set(properties);
    }

    @Override
    public Object resolveVariable(String field) {
        return THREAD_VARIABLES.get().get(field);
    }

    private static ThreadLocal<Date> referenceDate = new ThreadLocal<Date>();

    public static void setReferenceDate(Date date) {
        referenceDate.set(date);
        Properties variables = getTimeVariables(date, TimeZone.getTimeZone("UTC"));
        THREAD_VARIABLES.set(variables);
    }

    public static Properties getTimeVariables(Date date, TimeZone tz) {
        Properties vars = new Properties();
        Calendar cal = Calendar.getInstance(tz);
        cal.setTime(date);
        vars.put(FeedDataPath.VARS.YEAR.name(), String.format("%04d", cal.get(Calendar.YEAR)));
        vars.put(FeedDataPath.VARS.MONTH.name(), String.format("%02d", (cal.get(Calendar.MONTH) + 1)));
        vars.put(FeedDataPath.VARS.DAY.name(), String.format("%02d", cal.get(Calendar.DAY_OF_MONTH)));
        vars.put(FeedDataPath.VARS.HOUR.name(), String.format("%02d", cal.get(Calendar.HOUR_OF_DAY)));
        vars.put(FeedDataPath.VARS.MINUTE.name(), String.format("%02d", cal.get(Calendar.MINUTE)));
        return vars;
    }

    private static int getDayOffset(String weekDayName) {
        int day;
        Calendar nominalTime = Calendar.getInstance();
        nominalTime.setTimeZone(TimeZone.getTimeZone("UTC"));
        nominalTime.setTime(referenceDate.get());
        int currentWeekDay = nominalTime.get(Calendar.DAY_OF_WEEK);
        int weekDay = DayOfWeek.valueOf(weekDayName).ordinal() + 1; //to map to Calendar.SUNDAY ...
        day = weekDay - currentWeekDay;
        if (weekDay > currentWeekDay) {
            day = day - 7;
        }
        return day;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings({"SF_SWITCH_FALLTHROUGH"})
    private static Date getRelative(Date date, int boundary, int month, int day, int hour, int minute) {
        Calendar dsInstanceCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        dsInstanceCal.setTime(date);
        switch (boundary) {
        case Calendar.YEAR:
            dsInstanceCal.set(Calendar.MONTH, 0);
        case Calendar.MONTH:
            dsInstanceCal.set(Calendar.DAY_OF_MONTH, 1);
        case Calendar.DAY_OF_MONTH:
            dsInstanceCal.set(Calendar.HOUR_OF_DAY, 0);
        case Calendar.HOUR:
            dsInstanceCal.set(Calendar.MINUTE, 0);
            dsInstanceCal.set(Calendar.SECOND, 0);
            dsInstanceCal.set(Calendar.MILLISECOND, 0);
            break;
        case Calendar.SECOND:
            break;
        default:
            throw new IllegalArgumentException("Invalid boundary " + boundary);
        }

        dsInstanceCal.add(Calendar.YEAR, 0);
        dsInstanceCal.add(Calendar.MONTH, month);
        dsInstanceCal.add(Calendar.DAY_OF_MONTH, day);
        dsInstanceCal.add(Calendar.HOUR_OF_DAY, hour);
        dsInstanceCal.add(Calendar.MINUTE, minute);
        return dsInstanceCal.getTime();
    }

    public static Date now(int hour, int minute) {
        return getRelative(referenceDate.get(), Calendar.SECOND, 0, 0, hour, minute);
    }

    public static Date today(int hour, int minute) {
        return getRelative(referenceDate.get(), Calendar.DAY_OF_MONTH, 0, 0, hour, minute);
    }

    public static Date yesterday(int hour, int minute) {
        return getRelative(referenceDate.get(), Calendar.DAY_OF_MONTH, 0, -1, hour, minute);
    }

    public static Date currentMonth(int day, int hour, int minute) {
        return getRelative(referenceDate.get(), Calendar.MONTH, 0, day, hour, minute);
    }

    public static Date lastMonth(int day, int hour, int minute) {
        return getRelative(referenceDate.get(), Calendar.MONTH, -1, day, hour, minute);
    }

    public static Date currentWeek(String weekDay, int hour, int minute) {
        int day = getDayOffset(weekDay);
        return getRelative(referenceDate.get(), Calendar.DAY_OF_MONTH, 0, day, hour, minute);
    }

    public static Date lastWeek(String weekDay, int hour, int minute) {
        int day = getDayOffset(weekDay);
        return getRelative(referenceDate.get(), Calendar.DAY_OF_MONTH, 0, day - 7, hour, minute);
    }

    public static Date currentYear(int month, int day, int hour, int minute) {
        return getRelative(referenceDate.get(), Calendar.YEAR, month, day, hour, minute);
    }

    public static Date lastYear(int month, int day, int hour, int minute) {
        return getRelative(referenceDate.get(), Calendar.YEAR, month - 12, day, hour, minute);
    }

    public static Date latest(int n) {
        //by pass Falcon validations
        return referenceDate.get();
    }

    public static Date future(int n, int limit) {
        //by pass Falcon validations
        return referenceDate.get();
    }

    public static long hours(int val) {
        return TimeUnit.HOURS.toMillis(val);
    }

    public static long minutes(int val) {
        return TimeUnit.MINUTES.toMillis(val);
    }

    public static long days(int val) {
        return TimeUnit.DAYS.toMillis(val);
    }

    public static long months(int val) {
        return val * days(31);
    }

    public static long years(int val) {
        return val * days(366);
    }

    public static String substitute(String originalValue) {
        return substitute(originalValue, System.getProperties());
    }

    public static String substitute(String originalValue, Properties properties) {
        Matcher envVarMatcher = SYS_PROPERTY_PATTERN.matcher(originalValue);
        while (envVarMatcher.find()) {
            String envVar = originalValue.substring(envVarMatcher.start() + 2,
                    envVarMatcher.end() - 1);
            String envVal = properties.getProperty(envVar, System.getenv(envVar));

            envVar = "\\$\\{" + envVar + "\\}";
            if (envVal != null) {
                originalValue = originalValue.replaceAll(envVar, Matcher.quoteReplacement(envVal));
                envVarMatcher = SYS_PROPERTY_PATTERN.matcher(originalValue);
            }
        }
        return originalValue;
    }

    public static String formatTime(String dateTimeStr, String format) throws ParseException {
        Date dateTime = DateUtil.parseDateOozieTZ(dateTimeStr);
        return DateUtil.formatDateCustom(dateTime, format);
    }

    public static String instanceTime() {
        return DateUtil.formatDateOozieTZ(referenceDate.get());
    }

    public static String dateOffset(String strBaseDate, int offset, String unit) throws Exception {
        Calendar baseCalDate = DateUtil.getCalendar(strBaseDate);
        StringBuilder buffer = new StringBuilder();
        baseCalDate.add(org.apache.falcon.util.TimeUnit.valueOf(unit).getCalendarUnit(), offset);
        buffer.append(DateUtil.formatDateOozieTZ(baseCalDate));
        return buffer.toString();
    }

    public static String user() {
        return "${user.name}";
    }

}
