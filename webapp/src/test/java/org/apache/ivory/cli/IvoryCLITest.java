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

package org.apache.ivory.cli;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.Assert;

import org.apache.activemq.broker.BrokerService;
import org.apache.ivory.cluster.util.EmbeddedCluster;
import org.apache.ivory.util.EmbeddedServer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

//TODO most of the code is from EntityManagerJerseyTests 's base class.
//Refactor both the classes to move this methods to helper;
public class IvoryCLITest {

	private EmbeddedServer ivoryServer;
	private EmbeddedCluster cluster;
	private static final String BASE_URL = "http://localhost:15001/";
	private static final Pattern varPattern = Pattern
			.compile("##[A-Za-z0-9_]*##");
	private static final String CLUSTER = "/org/apache/ivory/cli/cluster.xml";
	private static final String FEED_INPUT = "/org/apache/ivory/cli/feed-input.xml";
	private static final String FEED_OUTPUT = "/org/apache/ivory/cli/feed-output.xml";
	private static final String PROCESS = "/org/apache/ivory/cli/process.xml";

	private static final String BROKER_URL = "vm://localhost?broker.useJmx=false&broker.persistent=true";
	// private static final String BROKER_URL =
	// "tcp://localhost:61616?daemon=true";
	private static final String BROKER_IMPL_CLASS = "org.apache.activemq.ActiveMQConnectionFactory";
	private BrokerService broker;

	private static final boolean enableTest = false;

	@BeforeClass
	public void setup() throws Exception {

		if (new File("webapp/src/main/webapp").exists()) {
			this.ivoryServer = new EmbeddedServer(15001,
					"webapp/src/main/webapp");
		} else if (new File("src/main/webapp").exists()) {
			this.ivoryServer = new EmbeddedServer(15001, "src/main/webapp");
		} else {
			throw new RuntimeException("Cannot run jersey tests");
		}
		this.ivoryServer.start();
		this.cluster = EmbeddedCluster.newCluster("test-cluster", false);

		broker = new BrokerService();
		broker.setUseJmx(true);
		broker.addConnector(BROKER_URL);
		broker.start();

	}

	@Test(enabled = enableTest)
	public void testSubmitEntityValidCommands() throws Exception {

		String filePath;
		Map<String, String> overlay = getUniqueOverlay();

		filePath = overlayParametersOverTemplate(CLUSTER, overlay);
		Assert.assertEquals(
				0,
				executeWithURL("entity -submit -type cluster -file " + filePath));

		filePath = overlayParametersOverTemplate(FEED_INPUT, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -submit -type feed -file " + filePath));

		filePath = overlayParametersOverTemplate(FEED_OUTPUT, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -submit -type feed -file " + filePath));

		filePath = overlayParametersOverTemplate(PROCESS, overlay);
		Assert.assertEquals(
				0,
				executeWithURL("entity -submit -type process -file " + filePath));

	}

	@Test(enabled = enableTest)
	public void testSubmitAndScheduleEntityValidCommands() throws Exception {

		String filePath;
		Map<String, String> overlay = getUniqueOverlay();

		filePath = overlayParametersOverTemplate(CLUSTER, overlay);
		Assert.assertEquals(-1,
				executeWithURL("entity -submitAndSchedule -type cluster -file "
						+ filePath));

		Assert.assertEquals(
				0,
				executeWithURL("entity -submit -type cluster -file " + filePath));

		filePath = overlayParametersOverTemplate(FEED_INPUT, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -submitAndSchedule -type feed -file "
						+ filePath));
		filePath = overlayParametersOverTemplate(FEED_OUTPUT, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -submitAndSchedule -type feed -file "
						+ filePath));
		filePath = overlayParametersOverTemplate(FEED_INPUT, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -submit -type feed -file " + filePath));

		filePath = overlayParametersOverTemplate(FEED_OUTPUT, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -submit -type feed -file " + filePath));

		filePath = overlayParametersOverTemplate(PROCESS, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -submitAndSchedule -type process -file "
						+ filePath));

	}

	@Test(enabled = enableTest)
	public void testValidateValidCommands() throws Exception {

		String filePath;
		Map<String, String> overlay = getUniqueOverlay();

		filePath = overlayParametersOverTemplate(CLUSTER, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -validate -type cluster -file "
						+ filePath));
		Assert.assertEquals(
				0,
				executeWithURL("entity -submit -type cluster -file " + filePath));

		filePath = overlayParametersOverTemplate(FEED_INPUT, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -validate -type feed -file " + filePath));
		Assert.assertEquals(0,
				executeWithURL("entity -submit -type feed -file " + filePath));

		filePath = overlayParametersOverTemplate(FEED_OUTPUT, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -validate -type feed -file " + filePath));
		Assert.assertEquals(0,
				executeWithURL("entity -submit -type feed -file " + filePath));

		filePath = overlayParametersOverTemplate(PROCESS, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -validate -type process -file "
						+ filePath));
		Assert.assertEquals(
				0,
				executeWithURL("entity -submit -type process -file " + filePath));

	}

	@Test(enabled = enableTest)
	public void testDefinitionEntityValidCommands() throws Exception {
		Map<String, String> overlay = getUniqueOverlay();
		submitTestFiles(overlay);

		Assert.assertEquals(0,
				executeWithURL("entity -definition -type cluster -name "
						+ overlay.get("clusterName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -definition -type feed -name "
						+ overlay.get("inputFeedName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -definition -type feed -name "
						+ overlay.get("outputFeedName")));

		Assert.assertEquals(0,
				executeWithURL("entity -definition -type process -name "
						+ overlay.get("processName")));

	}

	@Test(enabled = enableTest)
	public void testScheduleEntityValidCommands() throws Exception {

		Map<String, String> overlay = getUniqueOverlay();
		submitTestFiles(overlay);

		Assert.assertEquals(-1,
				executeWithURL("entity -schedule -type cluster -name "
						+ overlay.get("clusterName")));

		// TODO with feed lib fix
		// Assert.assertEquals(0,
		// executeWithURL("entity -schedule -type feed -name "
		// + overlay.get("outputFeedName")));

		Assert.assertEquals(0,
				executeWithURL("entity -schedule -type process -name "
						+ overlay.get("processName")));

	}

	@Test(enabled = enableTest)
	public void testSuspendResumeStatusEntityValidCommands() throws Exception {

		Map<String, String> overlay = getUniqueOverlay();

		Assert.assertEquals(
				0,
				executeWithURL("entity -status -type feed -name "
						+ overlay.get("outputFeedName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -status -type process -name "
						+ overlay.get("processName")));

		overlayParametersOverTemplate(CLUSTER, overlay);
		submitTestFiles(overlay);

		Assert.assertEquals(
				0,
				executeWithURL("entity -schedule -type feed -name "
						+ overlay.get("outputFeedName")));

		Assert.assertEquals(0,
				executeWithURL("entity -schedule -type process -name "
						+ overlay.get("processName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -suspend -type feed -name "
						+ overlay.get("outputFeedName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -suspend -type process -name "
						+ overlay.get("processName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -status -type feed -name "
						+ overlay.get("outputFeedName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -status -type process -name "
						+ overlay.get("processName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -resume -type feed -name "
						+ overlay.get("outputFeedName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -resume -type process -name "
						+ overlay.get("processName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -status -type feed -name "
						+ overlay.get("outputFeedName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -status -type process -name "
						+ overlay.get("processName")));

	}

	@Test(enabled = enableTest)
	public void testDeleteEntityValidCommands() throws Exception {

		Map<String, String> overlay = getUniqueOverlay();
		submitTestFiles(overlay);

		Assert.assertEquals(
				-1,
				executeWithURL("entity -delete -type cluster -name "
						+ overlay.get("clusterName")));

		Assert.assertEquals(
				-1,
				executeWithURL("entity -delete -type feed -name "
						+ overlay.get("inputFeedName")));

		Assert.assertEquals(
				-1,
				executeWithURL("entity -delete -type feed -name "
						+ overlay.get("outputFeedName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -delete -type process -name "
						+ overlay.get("processName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -delete -type feed -name "
						+ overlay.get("inputFeedName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -delete -type feed -name "
						+ overlay.get("outputFeedName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -delete -type cluster -name "
						+ overlay.get("clusterName")));
	}

	@Test(enabled = enableTest)
	public void testInvalidCLIEntitycommands() throws Exception {

		Map<String, String> overlay = getUniqueOverlay();
		overlayParametersOverTemplate(FEED_INPUT, overlay);
		Assert.assertEquals(-1,
				executeWithURL("entity -submit -type feed -name " + "name"));

		Assert.assertEquals(-1,
				executeWithURL("entity -schedule -type feed -file " + "name"));
	}

	@Test(enabled = enableTest)
	public void testInstanceRunningAndStatusCommands() throws Exception {
		Map<String, String> overlay = getUniqueOverlay();
		submitTestFiles(overlay);

		Assert.assertEquals(0,
				executeWithURL("entity -schedule -type process -name "
						+ overlay.get("processName")));

		Thread.sleep(5000);

		Assert.assertEquals(0, executeWithURL("instance -running -processName "
				+ overlay.get("processName")));

		Assert.assertEquals(0, executeWithURL("instance -status -processName "
				+ overlay.get("processName") + " -start 2010-01-01T01:00Z"));
		
		Assert.assertEquals(0, executeWithURL("instance -status -processName "
				+ overlay.get("processName") + " -start 2010-01-01T01:00Z"
				+ " -type DEFAULT -runid 0"));
	}

	@Test(enabled = enableTest)
	public void testInstanceSuspendAndResume() throws Exception {
		Map<String, String> overlay = getUniqueOverlay();
		submitTestFiles(overlay);

		Assert.assertEquals(0,
				executeWithURL("entity -schedule -type process -name "
						+ overlay.get("processName")));

		Thread.sleep(5000);

		Assert.assertEquals(0, executeWithURL("instance -suspend -processName "
				+ overlay.get("processName")
				+ " -start 2010-01-01T01:00Z  -end 2011-01-01T01:00Z"));
		Thread.sleep(2000);
		Assert.assertEquals(0, executeWithURL("instance -resume -processName "
				+ overlay.get("processName") + " -start 2010-01-01T01:00Z"));
	}

	@Test(enabled = enableTest)
	public void testInstanceKillAndRerun() throws Exception {
		Map<String, String> overlay = getUniqueOverlay();
		submitTestFiles(overlay);

		Assert.assertEquals(0,
				executeWithURL("entity -schedule -type process -name "
						+ overlay.get("processName")));

		Thread.sleep(5000);

		Assert.assertEquals(0, executeWithURL("instance -kill -processName "
				+ overlay.get("processName")
				+ " -start 2010-01-01T01:00Z  -end 2011-01-01T01:00Z"));
		Thread.sleep(2000);
		Assert.assertEquals(0, executeWithURL("instance -rerun -processName "
				+ overlay.get("processName")
				+ " -start 2010-01-01T01:00Z -file "
				+ createTempJobPropertiesFile()));
	}

	@Test(enabled = enableTest)
	public void testInvalidCLIInstanceCommands() throws Exception {
		// no command
		Assert.assertEquals(-1, executeWithURL(" -kill -processName " + "name"
				+ " -start 2010-01-01T01:00Z  -end 2011-01-01T01:00Z"));

		Assert.assertEquals(-1, executeWithURL("instance -kill  " + "name"
				+ " -start 2010-01-01T01:00Z  -end 2011-01-01T01:00Z"));

		Assert.assertEquals(-1, executeWithURL("instance -kill -processName "
				+ "name" + " -end 2011-01-01T01:00Z"));

		Assert.assertEquals(-1, executeWithURL("instance -kill -processName "
				+ " -start 2010-01-01T01:00Z  -end 2011-01-01T01:00Z"));

	}

	@Test(enabled = enableTest)
	public void testIvoryURL() throws Exception {
		Assert.assertEquals(-1, new IvoryCLI()
				.run(("instance -status -processName " + "processName"
						+ " -start 2010-01-01T01:00Z  -end 2011-01-01T01:00Z")
						.split("\\s")));

		Assert.assertEquals(-1, new IvoryCLI()
				.run(("instance -status -processName "
						+ "processName -url http://unknownhost:1234/"
						+ " -start 2010-01-01T01:00Z  -end 2011-01-01T01:00Z")
						.split("\\s")));

		System.out.println(System.getenv());
		Map<String, String> overlay = getUniqueOverlay();
		submitTestFiles(overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -schedule -type process -name "
						+ overlay.get("processName")));
		Thread.sleep(2000);

		Map<String, String> newEnv = new HashMap<String, String>();
		newEnv.put("IVORY_URL", BASE_URL);
		setEnv(newEnv);

		Assert.assertEquals(
				0,
				new IvoryCLI().run(("instance -status -processName "
						+ overlay.get("processName") + " -start 2010-01-01T01:00Z  -end 2011-01-01T01:00Z")
						.split("\\s")));

	}

	private void setEnv(Map<String, String> newenv) throws Exception {
		Class[] classes = Collections.class.getDeclaredClasses();
		Map<String, String> env = System.getenv();
		for (Class cl : classes) {
			if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
				Field field = cl.getDeclaredField("m");
				field.setAccessible(true);
				Object obj = field.get(env);
				Map<String, String> map = (Map<String, String>) obj;
				map.clear();
				map.putAll(newenv);
			}
		}
	}

	private int executeWithURL(String command) throws Exception {
		return new IvoryCLI().run((command + " -url " + BASE_URL).split("\\s"));
	}

	private Map<String, String> getUniqueOverlay() {
		Map<String, String> overlay = new HashMap<String, String>();
		String clusterName = "c" + System.currentTimeMillis();
		overlay.put("clusterName", clusterName);
		overlay.put("inputFeedName", "in" + System.currentTimeMillis());
		overlay.put("outputFeedName", "out" + System.currentTimeMillis());
		overlay.put("processName", "p" + System.currentTimeMillis());
		return overlay;
	}

	protected String overlayParametersOverTemplate(String template,
			Map<String, String> overlay) throws IOException {
		File target = new File("webapp/target");
		if (!target.exists()) {
			target = new File("target");
		}

		File tmpFile = File.createTempFile("test", ".xml", target);
		OutputStream out = new FileOutputStream(tmpFile);

		InputStreamReader in;
		if (getClass().getResourceAsStream(template) == null) {
			in = new FileReader(template);
		} else {
			in = new InputStreamReader(getClass().getResourceAsStream(template));
		}
		BufferedReader reader = new BufferedReader(in);
		String line;
		while ((line = reader.readLine()) != null) {
			Matcher matcher = varPattern.matcher(line);
			while (matcher.find()) {
				String variable = line
						.substring(matcher.start(), matcher.end());
				line = line.replace(variable, overlay.get(variable.substring(2,
						variable.length() - 2)));
				matcher = varPattern.matcher(line);
			}
			out.write(line.getBytes());
			out.write("\n".getBytes());
		}
		reader.close();
		out.close();
		return tmpFile.getAbsolutePath();
	}

	private String createTempJobPropertiesFile() throws IOException {
		File target = new File("webapp/target");
		if (!target.exists()) {
			target = new File("target");
		}
		File tmpFile = File.createTempFile("job", ".properties", target);
		OutputStream out = new FileOutputStream(tmpFile);
		out.write("oozie.wf.rerun.failnodes=true\n".getBytes());
		out.close();
		return tmpFile.getAbsolutePath();
	}

	@AfterClass
	public void teardown() throws Exception {
		broker.stop();
		this.cluster.shutdown();
		ivoryServer.stop();
	}

	public void submitTestFiles(Map<String, String> overlay) throws Exception {

		String filePath = overlayParametersOverTemplate(CLUSTER, overlay);
		Assert.assertEquals(
				0,
				executeWithURL("entity -submit -type cluster -file " + filePath));

		filePath = overlayParametersOverTemplate(FEED_INPUT, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -submit -type feed -file " + filePath));

		filePath = overlayParametersOverTemplate(FEED_OUTPUT, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -submit -type feed -file " + filePath));

		filePath = overlayParametersOverTemplate(PROCESS, overlay);
		Assert.assertEquals(
				0,
				executeWithURL("entity -submit -type process -file " + filePath));
	}
}
