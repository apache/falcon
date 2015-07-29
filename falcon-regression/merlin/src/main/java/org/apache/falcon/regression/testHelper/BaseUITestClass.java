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

package org.apache.falcon.regression.testHelper;

import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.core.util.Config;
import org.openqa.selenium.Dimension;
import org.openqa.selenium.Point;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.firefox.FirefoxProfile;

import java.util.concurrent.TimeUnit;

/**
 * Base class for UI test classes.
 */
public class BaseUITestClass extends BaseTestClass{

    private static WebDriver driver;

    public static WebDriver getDriver() {
        return driver;
    }

    protected static void openBrowser() {

        FirefoxProfile profile = new FirefoxProfile();
        profile.setPreference("network.negotiate-auth.trusted-uris", MerlinConstants.PRISM_URL);

        driver = new FirefoxDriver(profile);
        driver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS);

        int width = Config.getInt("browser.window.width", 0);
        int height = Config.getInt("browser.window.height", 0);

        if (width * height == 0) {
            driver.manage().window().maximize();
        } else {
            driver.manage().window().setPosition(new Point(0, 0));
            driver.manage().window().setSize(new Dimension(width, height));
        }

    }


    public static void closeBrowser() {
        if (driver != null) {
            driver.close();
            driver.quit();
            driver = null;
        }
    }
}
