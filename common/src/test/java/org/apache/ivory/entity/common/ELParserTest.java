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

import static org.testng.AssertJUnit.assertEquals;

import org.apache.ivory.IvoryException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ELParserTest {

	@Test
	public void testNowEL() throws IvoryException {
		test2paramsEL("now");
	}

	@Test
	public void testTodayEL() throws IvoryException {
		test2paramsEL("today");
	}

	@Test
	public void testYesterdayEL() throws IvoryException {
		test2paramsEL("yesterday");
	}

	@Test
	public void testCurrentMonthEL() throws IvoryException {
		test3paramsEL("currentMonth");
	}

	@Test
	public void testLastMonthEL() throws IvoryException {
		test3paramsEL("lastMonth");
	}

	@Test
	public void testCurrentYearEL() throws IvoryException {
		test4paramsEL("currentYear");
	}

	@Test
	public void testLastYearEL() throws IvoryException {
		test4paramsEL("lastYear");
	}

	@Test(expectedExceptions=IvoryException.class)
	public void testMinutes() throws IvoryException {
		ELParser elParser = new ELParser();
		elParser.parseOozieELExpression("minutes(0)");
		assertEquals("0", elParser.getMinute());
		assertEquals(0, elParser.getFeedDuration());
		elParser.parseOozieELExpression("minutes(12)");
		assertEquals("12", elParser.getMinute());
		assertEquals(12, elParser.getFeedDuration());
		elParser.parseOozieELExpression("minutes(-12)");
		assertEquals("-12", elParser.getMinute());
		assertEquals(-12, elParser.getFeedDuration());
	}

	@Test(expectedExceptions=IvoryException.class)
	public void testHours() throws IvoryException {
		ELParser elParser = new ELParser();
		elParser.parseOozieELExpression("hours(0)");
		assertEquals("0", elParser.getHour());
		assertEquals(0, elParser.getFeedDuration());
		elParser.parseOozieELExpression("hours(12)");
		assertEquals("12", elParser.getHour());
		assertEquals(720, elParser.getFeedDuration());
		elParser.parseOozieELExpression("hours(-12)");
		assertEquals("-12", elParser.getHour());
		assertEquals(-720, elParser.getFeedDuration());
	}

	@Test(expectedExceptions=IvoryException.class)
	public void testDays() throws IvoryException {
		ELParser elParser = new ELParser();
		elParser.parseOozieELExpression("days(0)");
		assertEquals("0", elParser.getDay());
		assertEquals(0, elParser.getFeedDuration());
		elParser.parseOozieELExpression("days(12)");
		assertEquals("12", elParser.getDay());
		assertEquals(17280, elParser.getFeedDuration());
		elParser.parseOozieELExpression("days(-12)");
		assertEquals("-12", elParser.getDay());
		assertEquals(-17280, elParser.getFeedDuration());
	}

	@Test(expectedExceptions=IvoryException.class)
	public void testMonths() throws IvoryException {
		ELParser elParser = new ELParser();
		elParser.parseOozieELExpression("months(0)");
		assertEquals("0", elParser.getMonth());
		assertEquals(0, elParser.getFeedDuration());
		elParser.parseOozieELExpression("months(12)");
		assertEquals("12", elParser.getMonth());
		assertEquals(535680, elParser.getFeedDuration());
		elParser.parseOozieELExpression("months(-12)");
		assertEquals("-12", elParser.getMonth());
		assertEquals(-535680, elParser.getFeedDuration());
	}

	private void test2paramsEL(String elName) throws IvoryException {
		ELParser elParser = new ELParser();
		elParser.parseElExpression(elName + "(0,0)");
		assertEquals(elParser.getHour(), "0");
		assertEquals(elParser.getMinute(), "0");
		//System.out.println(elName+"  "+elParser.getRequiredInputDuration()/(60));

		elParser.parseElExpression(elName + "  ( 0 ,  12 )");
		assertEquals(elParser.getHour(), "0");
		assertEquals(elParser.getMinute(), "12");
		//System.out.println(elName+"  "+elParser.getRequiredInputDuration()/(60));

		elParser.parseElExpression(elName + "(  13,0)");
		assertEquals(elParser.getHour(), "13");
		assertEquals(elParser.getMinute(), "0");
		//System.out.println(elName+"  "+elParser.getRequiredInputDuration()/(60));

		elParser.parseElExpression(elName + "(15,16  )");
		assertEquals(elParser.getHour(), "15");
		assertEquals(elParser.getMinute(), "16");
		//System.out.println(elName+"  "+elParser.getRequiredInputDuration()/(60));

		elParser.parseElExpression(elName + "(-1, -1)");
		assertEquals(elParser.getHour(), "-1");
		assertEquals(elParser.getMinute(), "-1");
		//System.out.println(elName+"  "+elParser.getRequiredInputDuration()/(60));

		elParser.parseElExpression(elName + "(-13,-14)");
		assertEquals(elParser.getHour(), "-13");
		assertEquals(elParser.getMinute(), "-14");
		//System.out.println(elName+"  "+elParser.getRequiredInputDuration()/(60));

		elParser.parseElExpression(elName + "(15,-14)");
		assertEquals(elParser.getHour(), "15");
		assertEquals(elParser.getMinute(), "-14");
		//System.out.println(elName+"  "+elParser.getRequiredInputDuration()/(60));

		elParser.parseElExpression(elName + "(-13,12)");
		assertEquals(elParser.getHour(), "-13");
		assertEquals(elParser.getMinute(), "12");
		//System.out.println(elName+"  "+elParser.getRequiredInputDuration()/(60));
	}

	private void test3paramsEL(String elName) throws IvoryException {
		ELParser elParser = new ELParser();
		elParser.parseElExpression(elName + "  (  0  ,  0  ,  0  )");
		assertEquals(elParser.getDay(), "0");
		assertEquals(elParser.getHour(), "0");
		assertEquals(elParser.getMinute(), "0");

		elParser.parseElExpression(elName + "(1  ,0,12)");
		assertEquals(elParser.getDay(), "1");
		assertEquals(elParser.getHour(), "0");
		assertEquals(elParser.getMinute(), "12");

		elParser.parseElExpression(elName + "(12  ,   13,0)");
		assertEquals(elParser.getDay(), "12");
		assertEquals(elParser.getHour(), "13");
		assertEquals(elParser.getMinute(), "0");

		elParser.parseElExpression(elName + "(12  ,   15,16)");
		assertEquals(elParser.getDay(), "12");
		assertEquals(elParser.getHour(), "15");
		assertEquals(elParser.getMinute(), "16");

		elParser.parseElExpression(elName + "(-1,-1,-1  )");
		assertEquals(elParser.getDay(), "-1");
		assertEquals(elParser.getHour(), "-1");
		assertEquals(elParser.getMinute(), "-1");

		elParser.parseElExpression(elName + "(-12 ,-13  ,-14)");
		assertEquals(elParser.getDay(), "-12");
		assertEquals(elParser.getHour(), "-13");
		assertEquals(elParser.getMinute(), "-14");

		elParser.parseElExpression(elName + "  (  14  ,15,-14)");
		assertEquals(elParser.getDay(), "14");
		assertEquals(elParser.getHour(), "15");
		assertEquals(elParser.getMinute(), "-14");

		elParser.parseElExpression(elName + "(-13,12,30)");
		assertEquals(elParser.getDay(), "-13");
		assertEquals(elParser.getHour(), "12");
		assertEquals(elParser.getMinute(), "30");
	}

	private void test4paramsEL(String elName) throws IvoryException {
		ELParser elParser = new ELParser();
		elParser.parseElExpression(elName + "  (0  ,  0  , 0  ,  0 )");
		assertEquals(elParser.getMonth(), "0");
		assertEquals(elParser.getDay(), "0");
		assertEquals(elParser.getHour(), "0");
		assertEquals(elParser.getMinute(), "0");
		assertEquals(elParser.getMinute(), "0");
		assertEquals(elParser.getInputDuration(), 0);

		elParser.parseElExpression(elName + "  (1,1,0,12)");
		assertEquals(elParser.getMonth(), "1");
		assertEquals(elParser.getDay(), "1");
		assertEquals(elParser.getHour(), "0");
		assertEquals(elParser.getMinute(), "12");
		assertEquals(elParser.getInputDuration(), 46092);

		elParser.parseElExpression(elName + "(11  ,12,13,14)");
		assertEquals(elParser.getMonth(), "11");
		assertEquals(elParser.getDay(), "12");
		assertEquals(elParser.getHour(), "13");
		assertEquals(elParser.getMinute(), "14");
		assertEquals(elParser.getInputDuration(), 509114);

		elParser.parseElExpression(elName + "(0  ,  12,15,16)");
		assertEquals(elParser.getMonth(), "0");
		assertEquals(elParser.getDay(), "12");
		assertEquals(elParser.getHour(), "15");
		assertEquals(elParser.getMinute(), "16");

		elParser.parseElExpression(elName + "(-1,-1,  -1,  -1  )");
		assertEquals(elParser.getMonth(), "-1");
		assertEquals(elParser.getDay(), "-1");
		assertEquals(elParser.getHour(), "-1");
		assertEquals(elParser.getMinute(), "-1");

		elParser.parseElExpression(elName + " (  -11,-12,-13,-14)");
		assertEquals(elParser.getMonth(), "-11");
		assertEquals(elParser.getDay(), "-12");
		assertEquals(elParser.getHour(), "-13");
		assertEquals(elParser.getMinute(), "-14");

		elParser.parseElExpression(elName + "(15,   14,  15,-14)");
		assertEquals(elParser.getMonth(), "15");
		assertEquals(elParser.getDay(), "14");
		assertEquals(elParser.getHour(), "15");
		assertEquals(elParser.getMinute(), "-14");

		elParser.parseElExpression(elName + "(   -10,-13,  12,30)");
		assertEquals(elParser.getMonth(), "-10");
		assertEquals(elParser.getDay(), "-13");
		assertEquals(elParser.getHour(), "12");
		assertEquals(elParser.getMinute(), "30");
		assertEquals(elParser.getInputDuration(), -464370);
	}

	//TODO add other el exp
	@DataProvider
	public Object[][] testValidFeedRentention() {
		return new Object[][]{
				new Object[] {"now(0,-30)","minutes(30)"},new Object[] {"now(-1,0)","hours(1)"},
				new Object[] {"now(-1,-30)","minutes(90)"},new Object[] {"now(-1,30)","minutes(30)"},
				new Object[] {"now(1,0)","minutes(10)"},new Object[] {"now(1,-30)","minutes(10)"},
				new Object[] {"now(1,-70)","minutes(10)"},new Object[] {"now(1,20)","minutes(10)"},
				new Object[] {"today(4,0)","hours(20)"},new Object[] {"today(4,0)","hours(22)"},
				new Object[] {"today(20,0)","hours(4)"},new Object[] {"today(4,15)","hours(20)"},
				new Object[] {"today(-2,0)","hours(26)"},new Object[] {"today(-2,20)","hours(26)"},
				new Object[] {"today(-2,-30)","hours(27)"},new Object[] {"today(-2,-60)","hours(27)"},
				new Object[] {"yesterday(22,0)","hours(26)"},new Object[] {"yesterday(22,0)","hours(28)"},
				new Object[] {"yesterday(2,0)","hours(46)"},new Object[] {"yesterday(22,15)","hours(26)"},
				new Object[] {"yesterday(-2,0)","days(3)"},new Object[] {"yesterday(-2,20)","hours(50)"},
				new Object[] {"yesterday(-2,-30)","hours(51)"},new Object[] {"yesterday(-2,-60)","hours(51)"},
				
		};
	}
	
	//TODO add negative cases
	@DataProvider
	public Object[][] testInvalidFeedRentention() {
		return new Object[][]{
				new Object[] {"now(0,-30)","minutes(29)"},new Object[] {"now(-1,0)","hours(0)"},
				new Object[] {"now(-1,-30)","minutes(89)"},new Object[] {"now(-1,30)","minutes(29)"},
				new Object[] {"now(1,0)","minutes(9)"},new Object[] {"now(1,-30)","minutes(9)"},
				new Object[] {"now(1,-70)","minutes(10)"},new Object[] {"now(1,20)","minutes(10)"},
				new Object[] {"today(4,0)","hours(20)"},new Object[] {"today(4,0)","hours(22)"},
				new Object[] {"today(20,0)","hours(4)"},new Object[] {"today(4,15)","hours(20)"},
				new Object[] {"today(-2,0)","hours(26)"},new Object[] {"today(-2,20)","hours(26)"},
				new Object[] {"today(-2,-30)","hours(27)"},new Object[] {"today(-2,-60)","hours(27)"},
				new Object[] {"yesterday(22,0)","hours(26)"},new Object[] {"yesterday(22,0)","hours(28)"},
				new Object[] {"yesterday(2,0)","hours(46)"},new Object[] {"yesterday(22,15)","hours(26)"},
				new Object[] {"yesterday(-2,0)","days(3)"},new Object[] {"yesterday(-2,20)","hours(50)"},
				new Object[] {"yesterday(-2,-30)","hours(51)"},new Object[] {"yesterday(-2,-60)","hours(51)"},
				
		};
	}

	@Test(dataProvider = "testValidFeedRentention")
	public void ValidDateTest(String inputStartInstance, String feedRetention) throws IvoryException {
		ELParser parser = new ELParser();
		parser.parseElExpression(inputStartInstance);
		long requiredInputDuration =parser.getRequiredInputDuration();
		parser.parseOozieELExpression(feedRetention);
		long feedDuration=parser.getFeedDuration();
		Assert.assertFalse(feedDuration-requiredInputDuration<0);
	}

}
