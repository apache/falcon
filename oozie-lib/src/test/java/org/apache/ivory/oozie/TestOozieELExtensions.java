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

import static org.testng.AssertJUnit.assertEquals;

import java.io.File;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.coord.SyncCoordAction;
import org.apache.oozie.coord.SyncCoordDataset;
import org.apache.oozie.coord.TimeUnit;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.ELService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ELEvaluator;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestOozieELExtensions {

    private Services services;
    private ELEvaluator instEval;
    private ELEvaluator createEval;
    private SyncCoordAction appInst;
    private SyncCoordDataset ds;

    @BeforeClass
    public void setUp() throws Exception {
        String curPath = new File(".").getAbsolutePath();
        System.setProperty(Services.OOZIE_HOME_DIR, curPath);
        String confPath = new File(getClass().getResource("/oozie-site.xml").getFile()).getParent();
        System.setProperty(ConfigurationService.OOZIE_CONFIG_DIR, confPath);
        Services.setOozieHome();

        services = new Services();
        services.init();
    }

    @Test
    public void testCurrentMonth() throws Exception {
        init();

        String expr = "${ivory:currentMonth(0,0,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        assertEquals("2009-09-01T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${ivory:currentMonth(2,-1,0)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        assertEquals("2009-09-02T23:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    private String getELExpression(String expr) {
        if(expr != null) {
            return "${" + expr + "}";
        }
        return null;
    }

    @Test
    public void testToday() throws Exception {
        init();

        String expr = "${ivory:today(0,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        assertEquals("2009-09-02T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${ivory:today(1,-20)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        assertEquals("2009-09-02T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    @Test
    public void testNow() throws Exception {
        init();

        String expr = "${ivory:now(0,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        assertEquals("2009-09-02T10:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${ivory:now(2,-10)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        assertEquals("2009-09-02T12:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    @Test
    public void testYesterday() throws Exception {
        init();

        String expr = "${ivory:yesterday(0,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        assertEquals("2009-09-01T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${ivory:yesterday(1,10)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        assertEquals("2009-09-01T01:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    @Test
    public void testLastMonth() throws Exception {
        init();

        String expr = "${ivory:lastMonth(0,0,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        assertEquals("2009-08-01T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${ivory:lastMonth(1,1,10)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        assertEquals("2009-08-02T01:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    @Test
    public void testCurrentYear() throws Exception {
        init();

        String expr = "${ivory:currentYear(0,0,0,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        assertEquals("2009-01-01T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${ivory:currentYear(1,0,1,0)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        assertEquals("2009-02-01T01:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    @Test
    public void testLastYear() throws Exception {
        init();

        String expr = "${ivory:lastYear(0,0,0,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        assertEquals("2008-01-01T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${ivory:lastYear(1,0,1,0)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        assertEquals("2008-02-01T01:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    @Test
    public void testNominalGreaterThanInitial() throws Exception {
        init();
        ds.setInitInstance(DateUtils.parseDateUTC("2009-08-30T010:00Z"));

        String expr = "${ivory:currentYear(0,0,0,0)}";
        assertEquals("", CoordELFunctions.evalAndWrap(instEval, expr));
    }

    private void init() throws Exception {
        ds = new SyncCoordDataset();
        ds.setFrequency(1);
        ds.setInitInstance(DateUtils.parseDateUTC("2007-09-30T010:00Z"));
        ds.setTimeUnit(TimeUnit.HOUR);
        ds.setTimeZone(DateUtils.getTimeZone("UTC"));
        ds.setName("test");
        ds.setUriTemplate("hdfs://localhost:9000/user/test_user/US/${YEAR}/${MONTH}/${DAY}");
        ds.setType("SYNC");
        ds.setDoneFlag("");

        appInst = new SyncCoordAction();
        appInst.setActualTime(DateUtils.parseDateUTC("2009-09-02T11:30Z"));
        appInst.setNominalTime(DateUtils.parseDateUTC("2009-09-02T10:30Z"));
        appInst.setTimeZone(DateUtils.getTimeZone("UTC"));
        appInst.setActionId("00000-oozie-C@1");
        appInst.setName("mycoordinator-app");

        instEval = Services.get().get(ELService.class).createEvaluator("coord-action-create-inst");
        instEval.setVariable(OozieClient.USER_NAME, "test_user");
        instEval.setVariable(OozieClient.GROUP_NAME, "test_group");
        CoordELFunctions.configureEvaluator(instEval, ds, appInst);

        createEval = Services.get().get(ELService.class).createEvaluator("coord-action-create");
        createEval.setVariable(OozieClient.USER_NAME, "test_user");
        createEval.setVariable(OozieClient.GROUP_NAME, "test_group");
        CoordELFunctions.configureEvaluator(createEval, ds, appInst);
    }
}
