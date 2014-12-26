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

package org.apache.falcon.entity;

import org.apache.falcon.entity.common.FeedDataPath;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 */
public class FeedDataPathTest {

    @Test
    public void testMinutesRegularExpression() {
        String monthPattern = FeedDataPath.VARS.MINUTE.getPatternRegularExpression();
        Assert.assertFalse("0".matches(monthPattern));
        Assert.assertFalse("1".matches(monthPattern));
        Assert.assertFalse("61".matches(monthPattern));
        Assert.assertFalse("010".matches(monthPattern));
        Assert.assertFalse("10 ".matches(monthPattern));
        Assert.assertFalse(" 10".matches(monthPattern));


        Assert.assertTrue("00".matches(monthPattern));
        Assert.assertTrue("01".matches(monthPattern));
        Assert.assertTrue("60".matches(monthPattern));
    }

    @Test
    public void testHourRegularExpression() {
        String hourPattern = FeedDataPath.VARS.HOUR.getPatternRegularExpression();
        Assert.assertFalse("0".matches(hourPattern));
        Assert.assertFalse("1".matches(hourPattern));
        Assert.assertFalse("2".matches(hourPattern));
        Assert.assertFalse("25".matches(hourPattern));
        Assert.assertFalse("29".matches(hourPattern));
        Assert.assertFalse("010".matches(hourPattern));
        Assert.assertFalse("10 ".matches(hourPattern));
        Assert.assertFalse(" 10".matches(hourPattern));


        Assert.assertTrue("00".matches(hourPattern));
        Assert.assertTrue("01".matches(hourPattern));
        Assert.assertTrue("24".matches(hourPattern));
        Assert.assertTrue("10".matches(hourPattern));
        Assert.assertTrue("19".matches(hourPattern));
        Assert.assertTrue("12".matches(hourPattern));
    }


    @Test
    public void testDayRegularExpression() {
        String dayPattern = FeedDataPath.VARS.DAY.getPatternRegularExpression();
        Assert.assertFalse("0".matches(dayPattern));
        Assert.assertFalse("1".matches(dayPattern));
        Assert.assertFalse("32".matches(dayPattern));
        Assert.assertFalse("00".matches(dayPattern));
        Assert.assertFalse("010".matches(dayPattern));
        Assert.assertFalse("10 ".matches(dayPattern));
        Assert.assertFalse(" 10".matches(dayPattern));


        Assert.assertTrue("01".matches(dayPattern));
        Assert.assertTrue("10".matches(dayPattern));
        Assert.assertTrue("29".matches(dayPattern));
        Assert.assertTrue("30".matches(dayPattern));
        Assert.assertTrue("31".matches(dayPattern));
    }

    @Test
    public void testMonthRegularExpression() {
        String monthPattern = FeedDataPath.VARS.MONTH.getPatternRegularExpression();
        Assert.assertFalse("0".matches(monthPattern));
        Assert.assertFalse("1".matches(monthPattern));
        Assert.assertFalse("13".matches(monthPattern));
        Assert.assertFalse("19".matches(monthPattern));
        Assert.assertFalse("00".matches(monthPattern));
        Assert.assertFalse("010".matches(monthPattern));
        Assert.assertFalse("10 ".matches(monthPattern));
        Assert.assertFalse(" 10".matches(monthPattern));


        Assert.assertTrue("01".matches(monthPattern));
        Assert.assertTrue("02".matches(monthPattern));
        Assert.assertTrue("10".matches(monthPattern));
        Assert.assertTrue("12".matches(monthPattern));
    }

    @Test
    public void testYearRegularExpression() {
        String monthPattern = FeedDataPath.VARS.YEAR.getPatternRegularExpression();
        Assert.assertFalse("0".matches(monthPattern));
        Assert.assertFalse("1".matches(monthPattern));
        Assert.assertFalse("13".matches(monthPattern));
        Assert.assertFalse("19".matches(monthPattern));
        Assert.assertFalse("00".matches(monthPattern));
        Assert.assertFalse("010".matches(monthPattern));
        Assert.assertFalse("10 ".matches(monthPattern));
        Assert.assertFalse(" 10".matches(monthPattern));


        Assert.assertTrue("0001".matches(monthPattern));
        Assert.assertTrue("2014".matches(monthPattern));
    }


}
