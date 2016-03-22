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

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.CoordinatorJob.Timeunit;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.coord.SyncCoordAction;
import org.apache.oozie.coord.SyncCoordDataset;
import org.apache.oozie.coord.TimeUnit;
import org.apache.oozie.dependency.FSURIHandler;
import org.apache.oozie.dependency.URIHandler;
import org.apache.oozie.dependency.URIHandlerException;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.ELService;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.URIHandlerService;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ELEvaluator;

import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test class for Falcon - Oozie el extensions.
 */
public class TestOozieELExtensions {

    private ELEvaluator instEval;
    private ELEvaluator createEval;

    @BeforeClass
    public void setUp() throws Exception {
        String curPath = new File(".").getAbsolutePath();
        System.setProperty(Services.OOZIE_HOME_DIR, curPath);
        String confPath = new File(getClass().getResource("/oozie-site.xml").getFile()).getParent();
        System.setProperty(ConfigurationService.OOZIE_CONFIG_DIR, confPath);
        Services.setOozieHome();

        Services services = new Services();
        Configuration conf = services.getConf();
        try {
            Class.forName("org.apache.oozie.service.URIHandlerService");
            conf.set("oozie.services", "org.apache.oozie.service.HadoopAccessorService,"
                    + "org.apache.oozie.service.ELService,org.apache.oozie.service.URIHandlerService");
        } catch (ClassNotFoundException e) {
            conf.set("oozie.services", "org.apache.oozie.service.HadoopAccessorService,"
                    + "org.apache.oozie.service.ELService");
        }
        conf.set("oozie.service.HadoopAccessorService.hadoop.configurations", "*=..");
        conf.set("oozie.service.HadoopAccessorService.action.configurations", "*=..");
        services.init();

        instEval = Services.get().get(ELService.class).createEvaluator("coord-action-create-inst");
        instEval.setVariable(OozieClient.USER_NAME, "test_user");
        instEval.setVariable(OozieClient.GROUP_NAME, "test_group");
        createEval = Services.get().get(ELService.class).createEvaluator("coord-action-create");
        createEval.setVariable(OozieClient.USER_NAME, "test_user");
        createEval.setVariable(OozieClient.GROUP_NAME, "test_group");
    }

    @Test
    public void testActionExpressions() throws Exception {
        ELEvaluator eval = createActionStartEvaluator();
        Assert.assertEquals(CoordELFunctions.evalAndWrap(eval, "${now(0, 0)}"), "2009-09-02T10:00Z");
        Assert.assertEquals(CoordELFunctions.evalAndWrap(eval, "${now(1, 0)}"), "2009-09-02T11:00Z");

        Assert.assertEquals(CoordELFunctions.evalAndWrap(eval, "${today(0, 0)}"), "2009-09-02T00:00Z");
        Assert.assertEquals(CoordELFunctions.evalAndWrap(eval, "${today(-1, 0)}"), "2009-09-01T23:00Z");

        Assert.assertEquals(CoordELFunctions.evalAndWrap(eval, "${yesterday(0, 0)}"), "2009-09-01T00:00Z");
        Assert.assertEquals(CoordELFunctions.evalAndWrap(eval, "${yesterday(0, 60)}"), "2009-09-01T01:00Z");

        Assert.assertEquals(CoordELFunctions.evalAndWrap(eval, "${currentMonth(0, 0, 0)}"), "2009-09-01T00:00Z");
        Assert.assertEquals(CoordELFunctions.evalAndWrap(eval, "${currentMonth(-1, 0, 0)}"), "2009-08-31T00:00Z");

        Assert.assertEquals(CoordELFunctions.evalAndWrap(eval, "${lastMonth(0, 0, 0)}"), "2009-08-01T00:00Z");
        Assert.assertEquals(CoordELFunctions.evalAndWrap(eval, "${lastMonth(0, 1, 0)}"), "2009-08-01T01:00Z");

        Assert.assertEquals(CoordELFunctions.evalAndWrap(eval, "${currentYear(0, 0, 0, 0)}"), "2009-01-01T00:00Z");
        Assert.assertEquals(CoordELFunctions.evalAndWrap(eval, "${currentYear(0, -1, 0, 0)}"), "2008-12-31T00:00Z");

        Assert.assertEquals(CoordELFunctions.evalAndWrap(eval, "${lastYear(0, 0, 0, 0)}"), "2008-01-01T00:00Z");
        Assert.assertEquals(CoordELFunctions.evalAndWrap(eval, "${lastYear(1, 0, 0, 0)}"), "2008-02-01T00:00Z");

        Assert.assertEquals(CoordELFunctions.evalAndWrap(eval, "${instanceTime()}"), "2009-09-02T10:00Z");

        Assert.assertEquals(CoordELFunctions.evalAndWrap(eval, "${dateOffset(instanceTime(), 1, 'DAY')}"),
                "2009-09-03T10:00Z");

        Assert.assertEquals(CoordELFunctions.evalAndWrap(eval, "${formatTime(instanceTime(), 'yyyy-MMM-dd')}"),
                "2009-Sep-02");
    }

    @Test
    public void testUser() throws Exception {
        ELEvaluator eval = Services.get().get(ELService.class).createEvaluator("coord-action-start");
        eval.setVariable(OozieClient.USER_NAME, "test");
        Assert.assertEquals(CoordELFunctions.evalAndWrap(eval, "${user()}"), "test");
    }

    @Test
    public void testDataIn() throws Exception {
        ELEvaluator eval = createActionStartEvaluator();
        String uris = "hdfs://localhost:8020/clicks/2009/09/02/10,hdfs://localhost:8020/clicks/2009/09/02/09";
        eval.setVariable(".datain.clicks", uris);
        String expuris =
                "hdfs://localhost:8020/clicks/2009/09/02/10/*/US,hdfs://localhost:8020/clicks/2009/09/02/09/*/US";
        Assert.assertEquals(expuris, CoordELFunctions.evalAndWrap(eval, "${dataIn('clicks', '*/US')}"));
    }

    @Test(dataProvider = "optionalDatasets")
    public void testDataInOptional(String expuris, String partition, String doneFlag) throws Exception {
        ELEvaluator eval = createActionStartEvaluator();
        String inName = "clicks";
        eval.setVariable(".datain.clicks", null);
        Services.get().setService(DummyURIHandlerService.class);

        SyncCoordDataset ds = createDataSet("2007-09-30T010:00Z");
        eval.setVariable(inName + ".frequency", String.valueOf(ds.getFrequency()));
        eval.setVariable(inName + ".freq_timeunit", ds.getTimeUnit().name());
        eval.setVariable(inName + ".timezone", ds.getTimeZone().getID());
        eval.setVariable(inName + ".end_of_duration", Timeunit.NONE.name());
        eval.setVariable(inName + ".initial-instance", OozieELExtensions.formatDateUTC(ds.getInitInstance()));
        eval.setVariable(inName + ".done-flag", doneFlag);
        eval.setVariable(inName + ".uri-template", ds.getUriTemplate());
        eval.setVariable(inName + ".start-instance", "now(-1,0)");
        eval.setVariable(inName + ".end-instance", "now(0,0)");
        eval.setVariable(OozieClient.USER_NAME, "test");
        eval.setVariable(inName + ".empty-dir", "hdfs://localhost:8020/projects/falcon/staging/EMPTY_DIR_DONT_DELETE");
        Assert.assertEquals(CoordELFunctions.evalAndWrap(eval, "${dataIn('clicks', '" + partition + "')}"), expuris);
    }

    @DataProvider(name = "optionalDatasets")
    public Object[][] getOptionalDatasets() {
        return new Object[][] {
            // With partitions and availability flag. All instances available.
            {"hdfs://localhost:8020/clicks/2009/09/02/10/*/US,hdfs://localhost:8020/clicks/2009/09/02/09/*/US",
                "*/US", "_DONE", },
            // With availability flag. All instances missing
            {"hdfs://localhost:8020/projects/falcon/staging/EMPTY_DIR_DONT_DELETE", "null", "_FINISH"},
            // No availability flag. One instance missing
            {"hdfs://localhost:8020/clicks/2009/09/02/09", "null", ""},
            // With availability flag. One instance missing.
            {"hdfs://localhost:8020/clicks/2009/09/02/10", "null", "_SUCCESS"},
            // No availability flag and partition. One instance missing
            {"hdfs://localhost:8020/clicks/2009/09/02/09/US", "US", ""},
            // With availability flag and partition. One instance missing.
            {"hdfs://localhost:8020/clicks/2009/09/02/10/US", "US", "_SUCCESS"},
        };
    }

    @Test
    public void testCurrentMonth() throws Exception {
        initForCurrentThread();

        String expr = "${currentMonth(0,0,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-01T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${currentMonth(2,-1,0)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-02T23:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    @Test
    public void testCurrentWeek() throws Exception {
        initForCurrentThread();

        String expr = "${currentWeek('SUN',0,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-08-30T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${currentWeek('FRI',1,0)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-08-28T01:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${lastWeek('SUN',0,0)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-08-23T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${lastWeek('MON',0,0)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-08-24T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    private String getELExpression(String expr) {
        if (expr != null) {
            return "${" + expr + "}";
        }
        return null;
    }

    @Test
    public void testToday() throws Exception {
        initForCurrentThread();

        String expr = "${today(0,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-02T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${today(1,-20)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-02T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    @Test
    public void testNow() throws Exception {
        initForCurrentThread();

        String expr = "${now(0,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-02T10:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${now(2,-10)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-02T12:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    @Test
    public void testYesterday() throws Exception {
        initForCurrentThread();

        String expr = "${yesterday(0,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-01T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${yesterday(1,10)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-01T01:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    @Test
    public void testLastMonth() throws Exception {
        initForCurrentThread();

        String expr = "${lastMonth(0,0,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-08-01T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${lastMonth(1,1,10)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-08-02T01:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    @Test
    public void testCurrentYear() throws Exception {
        initForCurrentThread();

        String expr = "${currentYear(0,0,0,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-01-01T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${currentYear(1,0,1,0)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-02-01T01:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    @Test
    public void testLastYear() throws Exception {
        initForCurrentThread();

        String expr = "${lastYear(0,0,0,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2008-01-01T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${lastYear(1,0,1,0)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2008-02-01T01:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    @Test
    public void testNominalGreaterThanInitial() throws Exception {
        initForCurrentThread("2009-08-30T010:00Z", "2009-09-02T11:30Z", "2009-09-02T10:30Z");
        String expr = "${currentYear(0,0,0,0)}";
        Assert.assertEquals("", CoordELFunctions.evalAndWrap(instEval, expr));
    }

    private void initForCurrentThread() throws Exception {
        initForCurrentThread("2007-09-30T010:00Z", "2009-09-02T11:30Z", "2009-09-02T10:30Z");
    }

    private SyncCoordDataset createDataSet(String initialInstance) throws Exception {
        SyncCoordDataset ds;
        ds = new SyncCoordDataset();
        ds.setFrequency(1);
        ds.setInitInstance(DateUtils.parseDateUTC(initialInstance));
        ds.setTimeUnit(TimeUnit.HOUR);
        ds.setTimeZone(OozieELExtensions.UTC);
        ds.setName("test");
        ds.setUriTemplate("hdfs://localhost:8020/clicks/${YEAR}/${MONTH}/${DAY}/${HOUR}");
        ds.setType("SYNC");
        ds.setDoneFlag("");
        return ds;
    }

    private SyncCoordAction createCoordAction(String actualTime, String nominalTime) throws Exception {
        SyncCoordAction appInst;
        appInst = new SyncCoordAction();
        appInst.setActualTime(DateUtils.parseDateUTC(actualTime));
        appInst.setNominalTime(DateUtils.parseDateUTC(nominalTime));
        appInst.setTimeZone(OozieELExtensions.UTC);
        appInst.setActionId("00000-oozie-C@1");
        appInst.setName("mycoordinator-app");
        return appInst;
    }

    private void initForCurrentThread(String initialInstance, String actualTime, String nominalTime) throws Exception {
        SyncCoordDataset ds = createDataSet(initialInstance);
        SyncCoordAction appInst = createCoordAction(actualTime, nominalTime);
        CoordELFunctions.configureEvaluator(instEval, ds, appInst);
        CoordELFunctions.configureEvaluator(createEval, ds, appInst);
    }

    private ELEvaluator createActionStartEvaluator() throws Exception {
        SyncCoordAction appInst = createCoordAction("2009-09-02T11:30Z", "2009-09-02T10:00Z");
        ELEvaluator eval = Services.get().get(ELService.class).createEvaluator("coord-action-start");
        CoordELFunctions.configureEvaluator(eval, null, appInst);
        return eval;
    }

    // A mock URIHandlerService that simulates availability of data as per testcase requirement
    private static class DummyURIHandlerService extends URIHandlerService {
        private URIHandler mockHandler = Mockito.mock(FSURIHandler.class);

        @Override
        public void init(Services services) throws ServiceException {
            try {
                Mockito.when(mockHandler.exists((URI)Mockito.argThat(new URIMatcher()),
                        Mockito.any(Configuration.class), Mockito.matches("test"))).thenReturn(true);
                Mockito.when(mockHandler.getURIWithDoneFlag(Mockito.anyString(),
                        Mockito.anyString())).thenCallRealMethod();
            } catch (URIHandlerException e) {
                throw new ServiceException(e);
            }
        }

        @Override
        public void destroy() {

        }

        public URIHandler getURIHandler(URI uri) {
            return mockHandler;
        }

        @Override
        public Class<? extends Service> getInterface() {
            return URIHandlerService.class;
        }
    }

    private static class URIMatcher extends ArgumentMatcher {
        private List<URI> availableURIs = new ArrayList<>();

        public URIMatcher() {
            try {
                availableURIs.add(new URI("hdfs://localhost:8020/clicks/2009/09/02/10/_DONE"));
                availableURIs.add(new URI("hdfs://localhost:8020/clicks/2009/09/02/09/_DONE"));
                availableURIs.add(new URI("hdfs://localhost:8020/clicks/2009/09/02/10/_SUCCESS"));
                availableURIs.add(new URI("hdfs://localhost:8020/clicks/2009/09/02/09"));
            } catch (URISyntaxException e) {
                //Shouldn't happen
            }
        }

        @Override
        public boolean matches(Object o) {
            return availableURIs.contains(o);
        }
    }
}
