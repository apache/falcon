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

package org.apache.falcon.entity.v0;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Date format yyyy/mm/dd validator Testing.
 */
public class DateValidatorTest {

    @DataProvider
    public Object[][] validDateProvider() {
        return new Object[][]{
            new Object[]{"2011-11-01T00:00Z", }, new Object[]{"2020-01-01T00:00Z", },
            new Object[]{"2010-01-31T00:59Z", }, new Object[]{"2020-01-31T00:00Z", },
            new Object[]{"2008-02-29T01:00Z", }, new Object[]{"2008-02-29T00:00Z", },
            new Object[]{"2009-02-28T01:01Z", }, new Object[]{"2009-02-28T00:00Z", },
            new Object[]{"2010-03-31T23:00Z", }, new Object[]{"2010-03-31T00:00Z", },
            new Object[]{"2010-04-30T23:59Z", }, new Object[]{"2010-04-30T00:00Z", },
            new Object[]{"2010-05-31T23:23Z", }, new Object[]{"2010-05-31T00:00Z", },
            new Object[]{"2010-06-30T00:00Z", }, new Object[]{"2010-06-30T00:00Z", },
            new Object[]{"2010-07-31T00:00Z", }, new Object[]{"2010-07-31T00:00Z", },
            new Object[]{"2010-08-31T00:00Z", }, new Object[]{"2010-08-31T00:00Z", },
            new Object[]{"2010-09-30T00:00Z", }, new Object[]{"2010-09-30T00:00Z", },
            new Object[]{"2010-10-31T00:00Z", }, new Object[]{"2010-10-31T00:00Z", },
            new Object[]{"2010-11-30T00:00Z", }, new Object[]{"2010-11-30T00:00Z", },
            new Object[]{"2010-12-31T00:00Z", }, new Object[]{"2010-12-31T00:00Z", },
            new Object[]{"1999-01-30T01:00Z", }, new Object[]{"2999-12-31T00:00Z", },
        };
    }

    @DataProvider
    public Object[][] invalidDateProvider() {
        return new Object[][]{
            new Object[]{"2010-12-31T00:60Z", }, new Object[]{"2010-12-31T24:00Z", },
            new Object[]{"2010-01-32T00:00Z", }, new Object[]{"2020-01-32T00:00Z", },
            new Object[]{"2010-13-1T00:00Z", }, new Object[]{"1820-01-01T00:00Z", },
            new Object[]{"2007-2-29T00:00Z", }, new Object[]{"2007-02-29T00:00Z", },
            new Object[]{"2008-2-30T00:00Z", }, new Object[]{"2008-02-31T00:00Z", },
            new Object[]{"2008-a-29T00:00Z", }, new Object[]{"2008-02aT00:00Z", },
            new Object[]{"2008-2-333T00:00Z", }, new Object[]{"200a-02-29T00:00Z", },
            new Object[]{"2010-4-31T00:00Z", }, new Object[]{"2010-04-31T00:00Z", },
            new Object[]{"2010-6-31T00:00Z", }, new Object[]{"2010-06-31T00:00Z", },
            new Object[]{"2010-9-31T00:00Z", }, new Object[]{"2010-09-31T00:00Z", },
            new Object[]{"2010-11-31T00:00Z", }, new Object[]{"1999-04-31T01:00Z", },
        };
    }

    @Test(dataProvider = "validDateProvider")
    public void validDateTest(String date) {
        boolean valid = DateValidator.validate(date);
        System.out.println("Date is valid : " + date + " , " + valid);
        Assert.assertEquals(valid, true);
    }

    @Test(dataProvider = "invalidDateProvider",
            dependsOnMethods = "validDateTest")
    public void invalidDateTest(String date) {
        boolean valid = DateValidator.validate(date);
        System.out.println("Date is valid : " + date + " , " + valid);
        Assert.assertEquals(valid, false);
    }
}
