/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.regression.testHelper;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.firefox.FirefoxProfile;

public class BaseUITestClass extends BaseTestClass{

    protected static WebDriver DRIVER;

    public static WebDriver getDRIVER() {
        return DRIVER;
    }

    protected void openBrowser() {

        FirefoxProfile profile = new FirefoxProfile();
        profile.setPreference("network.negotiate-auth.trusted-uris", "http://, https://");

        DRIVER = new FirefoxDriver(profile);
        DRIVER.manage().window().maximize();

    }


    public void closeBrowser() {
        if (DRIVER != null) {
            DRIVER.close();
            DRIVER.quit();
            DRIVER = null;
        }
    }
}
