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

package org.apache.falcon.regression.core.util;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.testng.Assert;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * util methods related to exec.
 */
public final class ExecUtil {
    private ExecUtil() {
        throw new AssertionError("Instantiating utility class...");
    }
    private static final Logger LOGGER = Logger.getLogger(ExecUtil.class);

    static List<String> runRemoteScriptAsSudo(final String hostName, final String userName,
                                              final String password, final String command,
                                              final String runAs, final String identityFile) throws
        JSchException, IOException {
        JSch jsch = new JSch();
        Session session = jsch.getSession(userName, hostName, 22);
        // only set the password if its not empty
        if (null != password && !password.isEmpty()) {
            session.setUserInfo(new HardcodedUserInfo(password));
        }
        Properties config = new Properties();
        config.setProperty("StrictHostKeyChecking", "no");
        config.setProperty("UserKnownHostsFile", "/dev/null");
        // only set the password if its not empty
        if (null == password || password.isEmpty()) {
            jsch.addIdentity(identityFile);
        }
        session.setConfig(config);
        session.connect();
        Assert.assertTrue(session.isConnected(), "The session was not connected correctly!");

        List<String> data = new ArrayList<String>();

        ChannelExec channel = (ChannelExec) session.openChannel("exec");
        channel.setPty(true);
        String runCmd;
        if (null == runAs || runAs.isEmpty()) {
            runCmd = "sudo -S -p '' " + command;
        } else {
            runCmd = String.format("sudo su - %s -c '%s'", runAs, command);
        }
        if (userName.equals(runAs)) {
            runCmd = command;
        }
        LOGGER.info(
            "host_name: " + hostName + " user_name: " + userName + " password: " + password
                    +
                " command: " +runCmd);
        channel.setCommand(runCmd);
        InputStream in = channel.getInputStream();
        OutputStream out = channel.getOutputStream();
        channel.setErrStream(System.err);
        channel.connect();
        TimeUtil.sleepSeconds(20);
        // only print the password if its not empty
        if (null != password && !password.isEmpty()) {
            out.write((password + "\n").getBytes());
            out.flush();
        }

        //save console output to data
        BufferedReader r = new BufferedReader(new InputStreamReader(in));
        String line;
        while (true) {
            while ((line=r.readLine())!=null) {
                LOGGER.debug(line);
                data.add(line);
            }
            if (channel.isClosed()) {
                break;
            }
        }

        byte[] tmp = new byte[1024];
        while (true) {
            while (in.available() > 0) {
                int i = in.read(tmp, 0, 1024);
                if (i < 0) {
                    break;
                }
                LOGGER.info(new String(tmp, 0, i));
            }
            if (channel.isClosed()) {
                LOGGER.info("exit-status: " + channel.getExitStatus());
                break;
            }
            TimeUtil.sleepSeconds(1);
        }

        in.close();
        channel.disconnect();
        session.disconnect();
        out.close();
        return data;
    }

    public static ExecResult executeCommand(String command) {
        LOGGER.info("Command to be executed: " + command);
        StringBuilder errors = new StringBuilder();
        StringBuilder output = new StringBuilder();

        try {
            Process process = Runtime.getRuntime().exec(command);

            BufferedReader errorReader =
                new BufferedReader(new InputStreamReader(process.getErrorStream()));
            BufferedReader consoleReader =
                new BufferedReader(new InputStreamReader(process.getInputStream()));

            String line;
            while ((line = errorReader.readLine()) != null) {
                errors.append(line).append("\n");
            }

            while ((line = consoleReader.readLine()) != null) {
                output.append(line).append("\n");
            }
            final int exitVal = process.waitFor();
            LOGGER.info("exitVal: " + exitVal);
            LOGGER.info("output: " + output);
            LOGGER.info("errors: " + errors);
            return new ExecResult(exitVal, output.toString().trim(), errors.toString().trim());
        } catch (InterruptedException e) {
            Assert.fail("Process execution failed:" + ExceptionUtils.getStackTrace(e));
        } catch (IOException e) {
            Assert.fail("Process execution failed:" + ExceptionUtils.getStackTrace(e));
        }
        return null;
    }

    public static int executeCommandGetExitCode(String command) {
        return executeCommand(command).getExitVal();
    }

    public static String executeCommandGetOutput(String command) {
        return executeCommand(command).getOutput();
    }

    private  static final class HardcodedUserInfo implements UserInfo {

        private final String password;

        private HardcodedUserInfo(String password) {
            this.password = password;
        }

        public String getPassphrase() {
            return null;
        }

        public String getPassword() {
            return password;
        }

        public boolean promptPassword(String s) {
            return true;
        }

        public boolean promptPassphrase(String s) {
            return true;
        }

        public boolean promptYesNo(String s) {
            return true;
        }

        public void showMessage(String s) {
            LOGGER.info("message = " + s);
        }
    }

    private static final class ExecResult {

        private final int exitVal;
        private final String output;
        private final String error;

        private ExecResult(final int exitVal, final String output, final String error) {
            this.exitVal = exitVal;
            this.output = output;
            this.error = error;
        }

        public int getExitVal() {
            return exitVal;
        }

        public String getOutput() {
            return output;
        }

        public String getError() {
            return error;
        }
    }
}
