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

package org.apache.falcon.regression;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.Config;
import org.apache.falcon.regression.core.util.LogUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.testHelper.BaseUITestClass;
import org.apache.log4j.Logger;
import org.apache.log4j.NDC;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.TakesScreenshot;
import org.testng.IExecutionListener;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class TestngListener implements ITestListener, IExecutionListener {
    private static final Logger logger = Logger.getLogger(TestngListener.class);
    private final String hr = StringUtils.repeat("-", 100);

    @Override
    public void onTestStart(ITestResult result) {
        logger.info(hr);
        logger.info(
            String.format("Testing going to start for: %s.%s(%s)", result.getTestClass().getName(),
                result.getName(), Arrays.toString(result.getParameters())));
        NDC.push(result.getName());
    }

    private void logEndOfTest(ITestResult result, String outcome) {
        logger.info(
            String.format("Testing going to end for: %s.%s(%s) %s", result.getTestClass().getName(),
                result.getName(), Arrays.toString(result.getParameters()), outcome));
        NDC.pop();
        logger.info(hr);
    }

    @Override
    public void onTestSuccess(ITestResult result) {
        logEndOfTest(result, "SUCCESS");
    }

    @Override
    public void onTestFailure(ITestResult result) {
        logEndOfTest(result, "FAILED");
        if (BaseUITestClass.getDRIVER() != null) {
            byte[] scrFile = ((TakesScreenshot)BaseUITestClass.getDRIVER()).getScreenshotAs
                    (OutputType.BYTES);
            try {
                String filename = OSUtil.getPath("target", "surefire-reports", "screenshots", String.format("%s.%s.png",
                        result.getTestClass().getRealClass().getSimpleName(), result.getName()));
                FileUtils.writeByteArrayToFile(new File(filename), scrFile);
            } catch (IOException e) {
                logger.info("Saving screenshot FAILED: " + e.getCause());
            }
        }

        logger.info(ExceptionUtils.getStackTrace(result.getThrowable()));
        logger.info(hr);
    }

    @Override
    public void onTestSkipped(ITestResult result) {
        logEndOfTest(result, "SKIPPED");
    }

    @Override
    public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
        logEndOfTest(result, "TestFailedButWithinSuccessPercentage");
    }

    @Override
    public void onStart(ITestContext context) {
    }

    @Override
    public void onFinish(ITestContext context) {
    }

    @Override
    public void onExecutionStart() {
    }

    @Override
    public void onExecutionFinish() {
        if (!Config.getBoolean("log.capture.oozie", false)) {
            logger.info("oozie log capturing is disabled");
            return;
        }
        final String logLocation = Config.getProperty("log.capture.location", "./");
        final String[] serverNames = Config.getStringArray("servers");
        for (final String serverName : serverNames) {
            final ColoHelper coloHelper = new ColoHelper(serverName.trim());
            LogUtil.writeOozieLogs(coloHelper, logLocation);
        }
    }


}
