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

package org.apache.ivory.oozie;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.coord.SyncCoordAction;
import org.apache.oozie.coord.SyncCoordDataset;
import org.apache.oozie.coord.TimeUnit;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.XLog;

public class OozieELExtensions {

    private enum TruncateBoundary {
        NONE, DAY, MONTH, QUARTER, YEAR;
    }

    private static final String PREFIX = "ivory:";
    private static String COORD_CURRENT = "coord:current";

    public static String ph1_now_echo(int hr, int min) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable(".wrap", "true");
        return PREFIX + "now("+ hr + "," + min + ")"; // Unresolved
    }

    public static String ph1_today_echo(int hr, int min) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable(".wrap", "true");
        return PREFIX + "today(" + hr + ", " + min + ")"; // Unresolved
    }

    public static String ph1_yesterday_echo(int hr, int min) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable(".wrap", "true");
        return PREFIX + "yesterday(" + hr + ", " + min + ")"; // Unresolved
    }

    public static String ph1_currentMonth_echo(int day, int hr, int min) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable(".wrap", "true");
        return PREFIX + "currentMonth(" + day + ", " + hr + ", " + min + ")"; // Unresolved
    }

    public static String ph1_lastMonth_echo(int day, int hr, int min) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable(".wrap", "true");
        return PREFIX + "lastMonth(" + day + ", " + hr + ", " + min + ")"; // Unresolved
    }

    public static String ph1_currentYear_echo(int month, int day, int hr, int min) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable(".wrap", "true");
        return PREFIX + "currentYear(" + month + ", " + day + ", " + hr + ", " + min + ")"; // Unresolved
    }

    public static String ph1_lastYear_echo(int month, int day, int hr, int min) {
        ELEvaluator eval = ELEvaluator.getCurrent();
        eval.setVariable(".wrap", "true");
        return PREFIX + "lastYear(" + month + ", " + day + ", " + hr + ", " + min + ")"; // Unresolved
    }

    public static String ph2_now_inst(int hr, int min) throws Exception {
        return mapToCurrentInstance(TruncateBoundary.NONE, 0, 0, 0, hr, min);
    }

    public static String ph2_today_inst(int hr, int min) throws Exception {
        return mapToCurrentInstance(TruncateBoundary.DAY, 0, 0, 0, hr, min);
    }

    public static String ph2_yesterday_inst(int hr, int min) throws Exception {
        return mapToCurrentInstance(TruncateBoundary.DAY, 0, 0, -1, hr, min);
    }

    public static String ph2_currentMonth_inst(int day, int hr, int min) throws Exception {
        return mapToCurrentInstance(TruncateBoundary.MONTH, 0, 0, day, hr, min);
    }

    public static String ph2_lastMonth_inst(int day, int hr, int min) throws Exception {
        return mapToCurrentInstance(TruncateBoundary.MONTH, 0, -1, day, hr, min);
    }

    public static String ph2_currentYear_inst(int month, int day, int hr, int min) throws Exception {
        return mapToCurrentInstance(TruncateBoundary.YEAR, 0, month, day, hr, min);
    }

    public static String ph2_lastYear_inst(int month, int day, int hr, int min) throws Exception {
        return mapToCurrentInstance(TruncateBoundary.YEAR, -1, month, day, hr, min);
    }

    private static String evaluateCurrent(String curExpr) throws Exception {
        if(curExpr.equals("")) {
            return curExpr;
        }

        int inst = parseOneArg(curExpr);
        return CoordELFunctions.ph2_coord_current(inst);
    }

    public static String ph2_now(int hr, int min) throws Exception {
        String inst = ph2_now_inst(hr, min);
        return evaluateCurrent(inst);
    }

    public static String ph2_today(int hr, int min) throws Exception {
        String inst = ph2_today_inst(hr, min);
        return evaluateCurrent(inst);
    }

    public static String ph2_yesterday(int hr, int min) throws Exception {
        String inst = ph2_yesterday_inst(hr, min);
        return evaluateCurrent(inst);
    }

    public static String ph2_currentMonth(int day, int hr, int min) throws Exception {
        String inst = ph2_currentMonth_inst(day, hr, min);
        return evaluateCurrent(inst);
    }

    public static String ph2_lastMonth(int day, int hr, int min) throws Exception {
        String inst = ph2_lastMonth_inst(day, hr, min);
        return evaluateCurrent(inst);
    }

    public static String ph2_currentYear(int month, int day, int hr, int min) throws Exception {
        String inst = ph2_currentYear_inst(month, day, hr, min);
        return evaluateCurrent(inst);
    }

    public static String ph2_lastYear(int month, int day, int hr, int min) throws Exception {
        String inst = ph2_lastYear_inst(month, day, hr, min);
        return evaluateCurrent(inst);
    }

    /**
     * Maps the dataset time to coord:current(n) with respect to action's nominal time
     * dataset time = truncate(nominal time) + yr + day + month + hr + min
     * @param truncField
     * @param yr
     * @param month
     * @param day
     * @param hr
     * @param min
     * @return coord:current(n)
     * @throws Exception
     */
    //TODO handle the case where action_Creation_time or the n-th instance is earlier than the Initial_Instance of dataset.
    private static String mapToCurrentInstance(TruncateBoundary trunc, int yr, int month, int day, int hr, int min) throws Exception {
        Calendar nominalInstanceCal = getEffectiveNominalTime();
        if (nominalInstanceCal == null) {
            XLog.getLog(OozieELExtensions.class).warn(
                    "If the initial instance of the dataset is later than the nominal time, an empty string is returned. " +
                            "This means that no data is available at the current-instance specified by the user " +
                    "and the user could try modifying his initial-instance to an earlier time.");
            return "";
        }

        Calendar dsInstanceCal = Calendar.getInstance(getDatasetTZ());
        dsInstanceCal.setTime(nominalInstanceCal.getTime());

        //truncate
        switch (trunc) {
            case YEAR:
                dsInstanceCal.set(Calendar.MONTH, 0);
            case MONTH:
                dsInstanceCal.set(Calendar.DAY_OF_MONTH, 1);
            case DAY:
                dsInstanceCal.set(Calendar.HOUR_OF_DAY, 0);
                dsInstanceCal.set(Calendar.MINUTE, 0);
                dsInstanceCal.set(Calendar.SECOND, 0);
                dsInstanceCal.set(Calendar.MILLISECOND, 0);
                break;
            case NONE:    //don't truncate
                break;
            default:
                throw new IllegalArgumentException("Truncation boundary " + trunc + " is not supported");
        }

        //add
        dsInstanceCal.add(Calendar.YEAR, yr);
        dsInstanceCal.add(Calendar.MONTH, month);
        dsInstanceCal.add(Calendar.DAY_OF_MONTH, day);
        dsInstanceCal.add(Calendar.HOUR_OF_DAY, hr);
        dsInstanceCal.add(Calendar.MINUTE, min);

        int[] instCnt = new int[1];
        Calendar compInstCal = getCurrentInstance(dsInstanceCal.getTime(), instCnt);
        if(compInstCal == null) {
            return "";
        }
        int dsInstanceCnt = instCnt[0];

        compInstCal = getCurrentInstance(nominalInstanceCal.getTime(), instCnt);
        if(compInstCal == null) {
            return "";
        }
        int nominalInstanceCnt = instCnt[0];

        return COORD_CURRENT + "(" + (dsInstanceCnt - nominalInstanceCnt) + ")";
    }

    /*============================================================================
     * NOTE:
     * Copied from CoordELFunctions in oozie source as these are private functions
     *============================================================================
     */
    final private static String DATASET = "oozie.coord.el.dataset.bean";
    final private static String COORD_ACTION = "oozie.coord.el.app.bean";

    private static int parseOneArg(String funcName) throws Exception {
        int firstPos = funcName.indexOf("(");
        int lastPos = funcName.lastIndexOf(")");
        if ((firstPos >= 0) && (lastPos > firstPos)) {
            String tmp = funcName.substring(firstPos + 1, lastPos).trim();
            if (tmp.length() > 0) {
                return (int) Double.parseDouble(tmp);
            }
        }
        throw new RuntimeException("Unformatted function :" + funcName);
    }

    private static Calendar getEffectiveNominalTime() {
        Date datasetInitialInstance = getInitialInstance();
        TimeZone dsTZ = getDatasetTZ();
        // Convert Date to Calendar for corresponding TZ
        Calendar current = Calendar.getInstance();
        current.setTime(datasetInitialInstance);
        current.setTimeZone(dsTZ);

        Calendar calEffectiveTime = Calendar.getInstance();
        calEffectiveTime.setTime(getActionCreationtime());
        calEffectiveTime.setTimeZone(dsTZ);
        if (current.compareTo(calEffectiveTime) > 0) {
            // Nominal Time < initial Instance
            // TODO: getClass() call doesn't work from static method.
            // XLog.getLog("CoordELFunction.class").warn("ACTION CREATED BEFORE INITIAL INSTACE "+
            // current.getTime());
            return null;
        }
        return calEffectiveTime;
    }

    /**
     * @return Nominal or action creation Time when all the dependencies of an application instance are met.
     */
    private static Date getActionCreationtime() {
        ELEvaluator eval = ELEvaluator.getCurrent();
        SyncCoordAction coordAction = (SyncCoordAction) eval.getVariable(COORD_ACTION);
        if (coordAction == null) {
            throw new RuntimeException("Associated Application instance should be defined with key " + COORD_ACTION);
        }
        return coordAction.getNominalTime();
    }

    /**
     * Find the current instance based on effectiveTime (i.e Action_Creation_Time or Action_Start_Time)
     *
     * @return current instance i.e. current(0) returns null if effectiveTime is earlier than Initial Instance time of
     *         the dataset.
     */
    private static Calendar getCurrentInstance(Date effectiveTime, int instanceCount[]) {
        Date datasetInitialInstance = getInitialInstance();
        TimeUnit dsTimeUnit = getDSTimeUnit();
        TimeZone dsTZ = getDatasetTZ();
        // Convert Date to Calendar for corresponding TZ
        Calendar current = Calendar.getInstance();
        current.setTime(datasetInitialInstance);
        current.setTimeZone(dsTZ);

        Calendar calEffectiveTime = Calendar.getInstance();
        calEffectiveTime.setTime(effectiveTime);
        calEffectiveTime.setTimeZone(dsTZ);
        instanceCount[0] = 0;
        if (current.compareTo(calEffectiveTime) > 0) {
            // Nominal Time < initial Instance
            // TODO: getClass() call doesn't work from static method.
            // XLog.getLog("CoordELFunction.class").warn("ACTION CREATED BEFORE INITIAL INSTACE "+
            // current.getTime());
            return null;
        }
        Calendar origCurrent = (Calendar) current.clone();
        while (current.compareTo(calEffectiveTime) <= 0) {
            current = (Calendar) origCurrent.clone();
            instanceCount[0]++;
            current.add(dsTimeUnit.getCalendarUnit(), instanceCount[0] * getDSFrequency());
        }
        instanceCount[0]--;

        current = (Calendar) origCurrent.clone();
        current.add(dsTimeUnit.getCalendarUnit(), instanceCount[0] * getDSFrequency());
        return current;
    }

    /**
     * @return dataset TimeUnit
     */
    private static TimeUnit getDSTimeUnit() {
        ELEvaluator eval = ELEvaluator.getCurrent();
        SyncCoordDataset ds = (SyncCoordDataset) eval.getVariable(DATASET);
        if (ds == null) {
            throw new RuntimeException("Associated Dataset should be defined with key " + DATASET);
        }
        return ds.getTimeUnit();
    }

    /**
     * @return the initial instance of a DataSet in DATE
     */
    private static Date getInitialInstance() {
        return getInitialInstanceCal().getTime();
        // return ds.getInitInstance();
    }

    /**
     * @return the initial instance of a DataSet in Calendar
     */
    private static Calendar getInitialInstanceCal() {
        ELEvaluator eval = ELEvaluator.getCurrent();
        SyncCoordDataset ds = (SyncCoordDataset) eval.getVariable(DATASET);
        if (ds == null) {
            throw new RuntimeException("Associated Dataset should be defined with key " + DATASET);
        }
        Calendar effInitTS = Calendar.getInstance();
        effInitTS.setTime(ds.getInitInstance());
        effInitTS.setTimeZone(ds.getTimeZone());
        // To adjust EOD/EOM
        DateUtils.moveToEnd(effInitTS, getDSEndOfFlag());
        return effInitTS;
        // return ds.getInitInstance();
    }

    /**
     * @return dataset frequency in minutes
     */
    private static int getDSFrequency() {
        ELEvaluator eval = ELEvaluator.getCurrent();
        SyncCoordDataset ds = (SyncCoordDataset) eval.getVariable(DATASET);
        if (ds == null) {
            throw new RuntimeException("Associated Dataset should be defined with key " + DATASET);
        }
        return ds.getFrequency();
    }

    /**
     * @return dataset TimeUnit
     */
    private static TimeUnit getDSEndOfFlag() {
        ELEvaluator eval = ELEvaluator.getCurrent();
        SyncCoordDataset ds = (SyncCoordDataset) eval.getVariable(DATASET);
        if (ds == null) {
            throw new RuntimeException("Associated Dataset should be defined with key " + DATASET);
        }
        return ds.getEndOfDuration();// == null ? "": ds.getEndOfDuration();
    }

    /**
     * @return dataset TimeZone
     */
    private static TimeZone getDatasetTZ() {
        ELEvaluator eval = ELEvaluator.getCurrent();
        SyncCoordDataset ds = (SyncCoordDataset) eval.getVariable(DATASET);
        if (ds == null) {
            throw new RuntimeException("Associated Dataset should be defined with key " + DATASET);
        }
        return ds.getTimeZone();
    }
}