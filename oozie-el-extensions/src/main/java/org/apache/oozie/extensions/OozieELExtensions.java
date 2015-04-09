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

package org.apache.oozie.extensions;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.command.coord.CoordCommandUtils;
import org.apache.oozie.coord.CoordELEvaluator;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.coord.SyncCoordAction;
import org.apache.oozie.coord.SyncCoordDataset;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.jdom.Attribute;
import org.jdom.Element;
import org.jdom.Text;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * Oozie EL Extensions for falcon.
 */
@SuppressWarnings("unchecked")
//SUSPEND CHECKSTYLE CHECK MethodName
public final class OozieELExtensions {

    public static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm'Z'";
    public static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    private enum TruncateBoundary {
        NONE, DAY, MONTH, QUARTER, YEAR
    }

    private enum DayOfWeek {
        SUN, MON, TUE, WED, THU, FRI, SAT
    }

    public static final String COORD_CURRENT = "coord:current";

    private OozieELExtensions() {
    }

    public static String ph1_dataIn_echo(String dataInName, String part) {
        return "dataIn('" + dataInName + "', '" + part + "')";
    }

    public static String ph3_dataIn(String dataInName, String part) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        String uristr = (String) eval.getVariable(".datain." + dataInName);

        //optional input
        if (uristr == null) {
            Element dsEle = getDSElement(eval, dataInName);
            Configuration conf = new Configuration();
            SyncCoordAction appInst = (SyncCoordAction) eval.getVariable(CoordELFunctions.COORD_ACTION);
            try {
                ELEvaluator instEval = CoordELEvaluator.createInstancesELEvaluator(dsEle, appInst, conf);
                StringBuilder instances = new StringBuilder();
                CoordCommandUtils.resolveInstanceRange(dsEle, instances , appInst, conf, instEval);
                uristr = CoordCommandUtils.createEarlyURIs(dsEle, instances.toString(),
                        new StringBuilder(), new StringBuilder());
                uristr = uristr.replace(CoordELFunctions.INSTANCE_SEPARATOR, ",");
            } catch (Exception e) {
                throw new RuntimeException("Failed to resolve instance range for " + dataInName, e);
            }
        } else {
            Boolean unresolved = (Boolean) eval.getVariable(".datain." + dataInName + ".unresolved");
            if (unresolved != null && unresolved) {
                throw new RuntimeException("There are unresolved instances in " + uristr);
            }
        }

        if (StringUtils.isNotEmpty(uristr) && StringUtils.isNotEmpty(part) && !part.equals("null")) {
            String[] uris = uristr.split(",");
            StringBuilder mappedUris = new StringBuilder();
            for (String uri : uris) {
                if (uri.trim().length() == 0) {
                    continue;
                }
                if (mappedUris.length() > 0) {
                    mappedUris.append(",");
                }
                mappedUris.append(uri).append("/").append(part);
            }
            return mappedUris.toString();
        }
        return uristr;
    }

    private static Element getDSElement(ELEvaluator eval, String dataInName) {
        Element ele = new Element("datain");
        Element dsEle = new Element("dataset");
        ele.getChildren().add(dsEle);

        String[] attrs = {"initial-instance", "frequency", "freq_timeunit", "timezone", "end_of_duration"};
        for (String attr : attrs) {
            dsEle.getAttributes().add(new Attribute(attr, (String) eval.getVariable(dataInName + "." + attr)));
        }

        String[] children = {"done-flag", "uri-template"};
        for (String child : children) {
            Element childEle = new Element(child);
            childEle.setContent(new Text(((String) eval.getVariable(dataInName + "." + child)).replace('%', '$')));
            dsEle.getChildren().add(childEle);
        }

        String[] eleChildren = {"start-instance", "end-instance"};
        for (String child : eleChildren) {
            Element childEle = new Element(child);
            childEle.setContent(new Text("${" + ((String) eval.getVariable(dataInName + "." + child)) + "}"));
            ele.getChildren().add(childEle);
        }

        return ele;
    }

    public static String ph1_now_echo(int hr, int min) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable(".wrap", "true");
        return "now(" + hr + "," + min + ")"; // Unresolved
    }

    public static String ph1_today_echo(int hr, int min) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable(".wrap", "true");
        return "today(" + hr + ", " + min + ")"; // Unresolved
    }

    public static String ph1_yesterday_echo(int hr, int min) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable(".wrap", "true");
        return "yesterday(" + hr + ", " + min + ")"; // Unresolved
    }

    public static String ph1_currentWeek_echo(String weekDayName, int hr, int min) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable(".wrap", "true");
        return "currentWeek('" + weekDayName + "', " + hr + ", " + min + ")"; // Unresolved
    }

    public static String ph1_lastWeek_echo(String weekDayName, int hr, int min) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable(".wrap", "true");
        return "lastWeek('" + weekDayName + "', " + hr + ", " + min + ")"; // Unresolved
    }

    public static String ph1_currentMonth_echo(int day, int hr, int min) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable(".wrap", "true");
        return "currentMonth(" + day + ", " + hr + ", " + min + ")"; // Unresolved
    }

    public static String ph1_lastMonth_echo(int day, int hr, int min) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable(".wrap", "true");
        return "lastMonth(" + day + ", " + hr + ", " + min + ")"; // Unresolved
    }

    public static String ph1_currentYear_echo(int month, int day, int hr, int min) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable(".wrap", "true");
        return "currentYear(" + month + ", " + day + ", " + hr + ", " + min + ")"; // Unresolved
    }

    public static String ph1_lastYear_echo(int month, int day, int hr, int min) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable(".wrap", "true");
        return "lastYear(" + month + ", " + day + ", " + hr + ", " + min + ")"; // Unresolved
    }

    public static String ph2_now_inst(int hr, int min) {
        return mapToCurrentInstance(TruncateBoundary.NONE, 0, 0, 0, hr, min);
    }

    public static String ph2_today_inst(int hr, int min) {
        return mapToCurrentInstance(TruncateBoundary.DAY, 0, 0, 0, hr, min);
    }

    public static String ph2_yesterday_inst(int hr, int min) {
        return mapToCurrentInstance(TruncateBoundary.DAY, 0, 0, -1, hr, min);
    }

    public static String ph2_currentWeek_inst(String weekDayName, int hr, int min) {
        int day = getDayOffset(weekDayName);
        return mapToCurrentInstance(TruncateBoundary.DAY, 0, 0, day, hr, min);
    }

    public static String ph2_lastWeek_inst(String weekDayName, int hr, int min) {
        int day = getDayOffset(weekDayName) - 7;
        return mapToCurrentInstance(TruncateBoundary.DAY, 0, 0, day, hr, min);
    }

    public static String ph2_currentMonth_inst(int day, int hr, int min) {
        return mapToCurrentInstance(TruncateBoundary.MONTH, 0, 0, day, hr, min);
    }

    public static String ph2_lastMonth_inst(int day, int hr, int min) {
        return mapToCurrentInstance(TruncateBoundary.MONTH, 0, -1, day, hr, min);
    }

    public static String ph2_currentYear_inst(int month, int day, int hr, int min) {
        return mapToCurrentInstance(TruncateBoundary.YEAR, 0, month, day, hr, min);
    }

    public static String ph2_lastYear_inst(int month, int day, int hr, int min) {
        return mapToCurrentInstance(TruncateBoundary.YEAR, -1, month, day, hr, min);
    }

    private static String evaluateCurrent(String curExpr) throws Exception {
        if (curExpr.equals("")) {
            return curExpr;
        }

        int inst = CoordCommandUtils.parseOneArg(curExpr);
        return CoordELFunctions.ph2_coord_current(inst);
    }

    public static String ph2_now(int hr, int min) throws Exception {
        if (isDatasetContext()) {
            String inst = ph2_now_inst(hr, min);
            return evaluateCurrent(inst);
        } else {
            return getEffectiveTimeStr(TruncateBoundary.NONE, 0, 0, 0, hr, min);
        }
    }

    private static boolean isActionContext() {
        return !isDatasetContext();
    }

    private static boolean isDatasetContext() {
        ELEvaluator eval = ELEvaluator.getCurrent();
        SyncCoordDataset ds = (SyncCoordDataset) eval.getVariable(CoordELFunctions.DATASET);
        return ds != null;
    }

    private static String getEffectiveTimeStr(TruncateBoundary trunc, int yr, int mon,
                                              int day, int hr, int min) throws Exception {
        Calendar time = getEffectiveTime(trunc, yr, mon, day, hr, min);
        return formatDateUTC(time);
    }

    private static DateFormat getISO8601DateFormat(TimeZone tz) {
        DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
        // Stricter parsing to prevent dates such as 2011-12-50T01:00Z (December 50th) from matching
        dateFormat.setLenient(false);
        dateFormat.setTimeZone(tz);
        return dateFormat;
    }

    public static String formatDateUTC(Date d) throws Exception {
        return (d != null) ? getISO8601DateFormat(UTC).format(d) : "NULL";
    }

    public static String formatDateUTC(Calendar c) throws Exception {
        return (c != null) ? formatDateUTC(c.getTime()) : "NULL";
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings({"SF_SWITCH_FALLTHROUGH"})
    private static Calendar getEffectiveTime(TruncateBoundary trunc, int yr, int mon, int day, int hr, int min) {
        Calendar cal;
        if (isActionContext()) {
            ELEvaluator eval = ELEvaluator.getCurrent();
            SyncCoordAction action = ParamChecker.notNull((SyncCoordAction)
                    eval.getVariable(CoordELFunctions.COORD_ACTION),
                    "Coordinator Action");
            cal = Calendar.getInstance(action.getTimeZone());
            cal.setTime(action.getNominalTime());
        } else {
            Calendar tmp = CoordELFunctions.getEffectiveNominalTime();
            if (tmp == null) {
                return null;
            }
            cal = Calendar.getInstance(CoordELFunctions.getDatasetTZ());
            cal.setTimeInMillis(tmp.getTimeInMillis());
        }

        // truncate
        switch (trunc) {
        case YEAR:
            cal.set(Calendar.MONTH, 0);

        case MONTH:
            cal.set(Calendar.DAY_OF_MONTH, 1);

        case DAY:
            cal.set(Calendar.HOUR_OF_DAY, 0);
            cal.set(Calendar.MINUTE, 0);
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
            break;

        case NONE: // don't truncate
            break;

        default:
            throw new IllegalArgumentException("Truncation boundary " + trunc + " is not supported");
        }

        // add
        cal.add(Calendar.YEAR, yr);
        cal.add(Calendar.MONTH, mon);
        cal.add(Calendar.DAY_OF_MONTH, day);
        cal.add(Calendar.HOUR_OF_DAY, hr);
        cal.add(Calendar.MINUTE, min);
        return cal;
    }

    public static String ph2_today(int hr, int min) throws Exception {
        if (isDatasetContext()) {
            String inst = ph2_today_inst(hr, min);
            return evaluateCurrent(inst);
        } else {
            return getEffectiveTimeStr(TruncateBoundary.DAY, 0, 0, 0, hr, min);
        }
    }

    public static String ph2_yesterday(int hr, int min) throws Exception {
        if (isDatasetContext()) {
            String inst = ph2_yesterday_inst(hr, min);
            return evaluateCurrent(inst);
        } else {
            return getEffectiveTimeStr(TruncateBoundary.DAY, 0, 0, -1, hr, min);
        }
    }

    public static String ph2_currentMonth(int day, int hr, int min) throws Exception {
        if (isDatasetContext()) {
            String inst = ph2_currentMonth_inst(day, hr, min);
            return evaluateCurrent(inst);
        } else {
            return getEffectiveTimeStr(TruncateBoundary.MONTH, 0, 0, day, hr, min);
        }
    }

    public static String ph2_lastMonth(int day, int hr, int min) throws Exception {
        if (isDatasetContext()) {
            String inst = ph2_lastMonth_inst(day, hr, min);
            return evaluateCurrent(inst);
        } else {
            return getEffectiveTimeStr(TruncateBoundary.MONTH, 0, -1, day, hr, min);
        }
    }

    private static int getDayOffset(String weekDayName) {
        int day;
        Calendar effectiveTime;
        if (isDatasetContext()) {
            effectiveTime = CoordELFunctions.getEffectiveNominalTime();
        } else {
            effectiveTime = getEffectiveTime(TruncateBoundary.DAY, 0, 0, 0, 0, 0);
        }
        int currentWeekDay = effectiveTime.get(Calendar.DAY_OF_WEEK);
        int weekDay = DayOfWeek.valueOf(weekDayName).ordinal() + 1; //to map to Calendar.SUNDAY ...
        day = weekDay - currentWeekDay;
        if (weekDay > currentWeekDay) {
            day = day - 7;
        }
        return day;
    }

    public static String ph2_currentWeek(String weekDayName, int hr, int min) throws Exception {
        int day = getDayOffset(weekDayName);
        if (isDatasetContext()) {
            String inst = ph2_currentMonth_inst(day, hr, min);
            return evaluateCurrent(inst);
        } else {
            return getEffectiveTimeStr(TruncateBoundary.DAY, 0, 0, day, hr, min);
        }
    }

    public static String ph2_lastWeek(String weekDayName, int hr, int min) throws Exception {
        int day = getDayOffset(weekDayName) - 7;
        if (isDatasetContext()) {
            String inst = ph2_lastMonth_inst(day, hr, min);
            return evaluateCurrent(inst);
        } else {
            return getEffectiveTimeStr(TruncateBoundary.DAY, 0, 0, day, hr, min);
        }
    }

    public static String ph2_currentYear(int month, int day, int hr, int min) throws Exception {
        if (isDatasetContext()) {
            String inst = ph2_currentYear_inst(month, day, hr, min);
            return evaluateCurrent(inst);
        } else {
            return getEffectiveTimeStr(TruncateBoundary.YEAR, 0, month, day, hr, min);
        }
    }

    public static String ph2_lastYear(int month, int day, int hr, int min) throws Exception {
        if (isDatasetContext()) {
            String inst = ph2_lastYear_inst(month, day, hr, min);
            return evaluateCurrent(inst);
        } else {
            return getEffectiveTimeStr(TruncateBoundary.YEAR, -1, month, day, hr, min);
        }
    }

    /**
     * Maps the dataset time to coord:current(n) with respect to action's
     * nominal time dataset time = truncate(nominal time) + yr + day + month +
     * hr + min.
     *
     * @param trunc
     *            : Truncate resolution
     * @param yr
     *            : Year to add (can be -ve)
     * @param month
     *            : month to add (can be -ve)
     * @param day
     *            : day to add (can be -ve)
     * @param hr
     *            : hr to add (can be -ve)
     * @param min
     *            : min to add (can be -ve)
     * @return coord:current(n)
     * @throws Exception
     *             : If encountered an exception while evaluating
     */
    private static String mapToCurrentInstance(TruncateBoundary trunc, int yr, int month, int day, int hr, int min) {
        Calendar nominalInstanceCal = CoordELFunctions.getEffectiveNominalTime();
        if (nominalInstanceCal == null) {
            XLog.getLog(OozieELExtensions.class).warn(
                    "If the initial instance of the dataset is later than the nominal time, "
                            + "an empty string is returned. This means that no data is available "
                            + "at the current-instance specified by the user and the user could "
                            + "try modifying his initial-instance to an earlier time.");
            return "";
        }

        Calendar dsInstanceCal = getEffectiveTime(trunc, yr, month, day, hr, min);

        int[] instCnt = new int[1];
        Calendar compInstCal = CoordELFunctions.getCurrentInstance(dsInstanceCal.getTime(), instCnt);
        if (compInstCal == null) {
            return "";
        }
        int dsInstanceCnt = instCnt[0];

        compInstCal = CoordELFunctions.getCurrentInstance(nominalInstanceCal.getTime(), instCnt);
        if (compInstCal == null) {
            return "";
        }
        int nominalInstanceCnt = instCnt[0];

        return COORD_CURRENT + "(" + (dsInstanceCnt - nominalInstanceCnt) + ")";
    }
}
//RESUME CHECKSTYLE CHECK MethodName
