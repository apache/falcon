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

package org.apache.falcon.regression.searchUI;

import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.testHelper.BaseUITestClass;
import org.apache.falcon.regression.ui.search.LoginPage;
import org.apache.falcon.regression.ui.search.SearchPage;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;

/** UI tests for login/logout. */
@Test(groups = "search-ui")
public class LoginTest extends BaseUITestClass {
    private static final Logger LOGGER = Logger.getLogger(LoginTest.class);
    private LoginPage loginPage = null;

    @BeforeMethod(alwaysRun = true)
    public void setup() {
        openBrowser();
        loginPage = LoginPage.open(getDriver());
        loginPage.getPageHeader().checkLoggedOut();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        closeBrowser();
    }

    @Test
    public void testLoginWithoutUser() throws Exception {
        AssertUtil.assertEmpty(loginPage.getUserVisibleWarning(), "Unexpected user warning.");

        loginPage.tryLogin();

        Assert.assertEquals(loginPage.getUserVisibleWarning(), "Please enter your user name.",
            "Unexpected warning displayed when logging in without user.");
    }

    @Test
    public void testValidUserNames() throws Exception {
        AssertUtil.assertEmpty(loginPage.getUserVisibleWarning(), "Unexpected user warning.");
        for (String str : "some_user".split("")) {
            loginPage.appendToUserName(str);
        }
        AssertUtil.assertEmpty(loginPage.getUserVisibleWarning(), "Unexpected user warning.");
    }

    @DataProvider
    public Object[][] getInvalidNames() {
        return new Object[][]{
            {"some@user"},
            {"some" + "\u20AC" + "user"},
            {"some user"},
        };
    }

    @Test(dataProvider = "getInvalidNames")
    public void testInvalidUserNames(final String userName) throws Exception {
        AssertUtil.assertEmpty(loginPage.getUserVisibleWarning(), "Unexpected user warning.");
        for (String str : userName.split("")) {
            loginPage.appendToUserName(str);
        }
        Assert.assertEquals(loginPage.getUserVisibleWarning(), "The User has an invalid format.",
            "Unexpected user warning.");
    }

    @Test
    public void testLoginWithoutPassword() throws Exception {
        loginPage.appendToUserName("some-user");

        loginPage.tryLogin();

    }

    @Test
    public void testLoginSuccessfully() throws Exception {
        SearchPage searchPage = loginPage.doDefaultLogin();
        searchPage.checkPage();

        final LoginPage newLoginPage = searchPage.getPageHeader().doLogout();
        newLoginPage.getPageHeader().checkLoggedOut();
    }

    @Test
    public void testHeader() throws Exception {
        loginPage.getPageHeader().checkHeader();
    }
}
