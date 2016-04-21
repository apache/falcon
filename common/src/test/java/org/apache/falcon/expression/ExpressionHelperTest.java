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

import org.apache.falcon.FalconException;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.text.ParseException;
import java.util.Date;

/**
 * Unit test cases for EL Expressions.
 */
public class ExpressionHelperTest {

    private ExpressionHelper expressionHelper = ExpressionHelper.get();

    @BeforeTest
    public void init() throws ParseException {
        Date referenceDate = ExpressionHelper.FORMATTER.get().parse("2015-02-01T00:00Z");
        expressionHelper.setReferenceDate(referenceDate);
    }

    @Test(dataProvider = "ElExpressions")
    public void testStartOffset(String expression, String expectedDateStr) throws FalconException {
        Date evalDate = expressionHelper.evaluate(expression, Date.class);
        String evalDateStr = ExpressionHelper.FORMATTER.get().format(evalDate);
        Assert.assertEquals(evalDateStr, expectedDateStr);
    }


    @DataProvider(name = "ElExpressions")
    public Object[][] createOffsets() {
        return new Object[][] {
            {"now(-10,-30)", "2015-01-31T13:30Z"},
            {"now(10,-30)", "2015-02-01T09:30Z"},

            {"today(0,0)", "2015-02-01T00:00Z"},
            {"today(-1,0)", "2015-01-31T23:00Z"},
            {"yesterday(0,0)", "2015-01-31T00:00Z"},
            {"yesterday(-1,0)", "2015-01-30T23:00Z"},
            {"yesterday(1,30)", "2015-01-31T01:30Z"},

            {"currentMonth(2,0,0)", "2015-02-03T00:00Z"},
            {"currentMonth(-2,1,30)", "2015-01-30T01:30Z"},
            {"lastMonth(3,0,0)", "2015-01-04T00:00Z"},
            {"lastMonth(-3,0,0)", "2014-12-29T00:00Z"},

            {"currentWeek('THU',0,0)", "2015-01-29T00:00Z"},
            {"currentWeek('SUN',0,0)", "2015-02-01T00:00Z"},
            {"lastWeek('THU',0,0)", "2015-01-22T00:00Z"},
            {"lastWeek('SUN',0,0)", "2015-01-25T00:00Z"},

            {"currentYear(1,1,0,0)", "2015-02-02T00:00Z"},
            {"currentYear(-1,1,0,0)", "2014-12-02T00:00Z"},
            {"lastYear(1,1,0,0)", "2014-02-02T00:00Z"},
            {"lastYear(-1,1,0,0)", "2013-12-02T00:00Z"},

            // latest and future will return the reference time
            {"latest(0)", "2015-02-01T00:00Z"},
            {"latest(-1)", "2015-02-01T00:00Z"},
            {"future(0,0)", "2015-02-01T00:00Z"},
            {"future(1,0)", "2015-02-01T00:00Z"},
        };
    }

    @Test
    public void testFormatTime() throws FalconException {
        String output = expressionHelper.evaluate("formatTime(\"2016-02-01T10:59Z\", \"yyyy\")",
                String.class);
        Assert.assertEquals(output, "2016");
        output = expressionHelper.evaluate("formatTime(\"2016-02-01T10:59Z\", \"yyyy-MM\")",
                String.class);
        Assert.assertEquals(output, "2016-02");
        output = expressionHelper.evaluate("formatTime(\"2016-02-01T10:59Z\", \"yyyy-MM-dd\")",
                String.class);
        Assert.assertEquals(output, "2016-02-01");
    }


    @Test
    public void testOffsetAndInstanceTime() throws FalconException {
        String date = expressionHelper.evaluate("dateOffset(instanceTime(), 1, 'DAY')", String.class);
        Assert.assertEquals(date, "2015-02-02T00:00Z");
        date = expressionHelper.evaluate("dateOffset(instanceTime(), 3, 'HOUR')", String.class);
        Assert.assertEquals(date, "2015-02-01T03:00Z");
        date = expressionHelper.evaluate("dateOffset(instanceTime(), -25, 'MINUTE')", String.class);
        Assert.assertEquals(date, "2015-01-31T23:35Z");
    }
}
