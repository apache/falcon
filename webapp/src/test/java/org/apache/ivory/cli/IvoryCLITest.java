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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.Assert;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.cluster.util.EmbeddedCluster;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityType;
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
	private static final Pattern VAR_PATTERN = Pattern
			.compile("##[A-Za-z0-9_]*##");

	private InMemoryWriter stream = new InMemoryWriter(System.out);
	private static final String FEED_INPUT = "/org/apache/ivory/cli/feed-input.xml";
	private static final String FEED_OUTPUT = "/org/apache/ivory/cli/feed-output.xml";
	private static final String PROCESS = "/org/apache/ivory/cli/process.xml";

	private static final String BROKER_URL = "vm://localhost1?broker.useJmx=false&broker.persistent=true";
	// private static final String BROKER_URL =
	// "tcp://localhost:61616?daemon=true";
	private BrokerService broker;
	private FileSystem fs;

	private static final boolean enableTest = true;

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

		cleanupStore();
		this.cluster = EmbeddedCluster.newCluster("testCluster", true);

		fs = FileSystem.get(cluster.getConf());
		fs.mkdirs(new Path("/workflow/lib"));
		fs.copyFromLocalFile(
				new Path(IvoryCLITest.class.getResource(
						"/org/apache/ivory/cli/workflow.xml").toURI()),
				new Path("/workflow/"));
		fs.mkdirs(new Path("/workflow/static/in"));

		broker = new BrokerService();
		broker.setUseJmx(true);
		broker.setDataDirectory("target/activemq");
		broker.addConnector(BROKER_URL);
		broker.setBrokerName("localhost");
		broker.start();
	}

	@Test(enabled = enableTest)
	public void testSubmitEntityValidCommands() throws Exception {

		IvoryCLI.OUT_STREAM = stream;

		String filePath;
		Map<String, String> overlay = getUniqueOverlay();

		filePath = overlayParametersOverTemplate(cluster.getCluster(), overlay);
		Assert.assertEquals(
				0,
				executeWithURL("entity -submit -type cluster -file " + filePath));
		Assert.assertEquals(stream.buffer.toString().trim(),
				"default/Submit successful (cluster) testCluster");

		filePath = overlayParametersOverTemplate(FEED_INPUT, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -submit -type feed -file " + filePath));
		Assert.assertEquals(
				stream.buffer.toString().trim(),
				"default/Submit successful (feed) "
						+ overlay.get("inputFeedName"));

		filePath = overlayParametersOverTemplate(FEED_OUTPUT, overlay);
		Assert.assertEquals(0,
				executeWithURL("entity -submit -type feed -file " + filePath));
		Assert.assertEquals(
				stream.buffer.toString().trim(),
				"default/Submit successful (feed) "
						+ overlay.get("outputFeedName"));

		filePath = overlayParametersOverTemplate(PROCESS, overlay);
		Assert.assertEquals(
				0,
				executeWithURL("entity -submit -type process -file " + filePath));
		Assert.assertEquals(
				stream.buffer.toString().trim(),
				"default/Submit successful (process) "
						+ overlay.get("processName"));
	}

	@Test(enabled = enableTest)
	public void testSubmitAndScheduleEntityValidCommands() throws Exception {

		String filePath;
		Map<String, String> overlay = getUniqueOverlay();

		filePath = overlayParametersOverTemplate(cluster.getCluster(), overlay);
		Assert.assertEquals(-1,
				executeWithURL("entity -submitAndSchedule -type cluster -file "
						+ filePath));

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

		filePath = overlayParametersOverTemplate(cluster.getCluster(), overlay);
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

		Assert.assertEquals(
				0,
				executeWithURL("entity -schedule -type feed -name "
						+ overlay.get("outputFeedName")));

		Assert.assertEquals(0,
				executeWithURL("entity -schedule -type process -name "
						+ overlay.get("processName")));

	}

	@Test(enabled = enableTest)
	public void testSuspendResumeStatusEntityValidCommands() throws Exception {

		Map<String, String> overlay = getUniqueOverlay();
		submitTestFiles(overlay);

		Assert.assertEquals(
				0,
				executeWithURL("entity -status -type feed -name "
						+ overlay.get("outputFeedName")));

		Assert.assertEquals(
				0,
				executeWithURL("entity -status -type process -name "
						+ overlay.get("processName")));

		overlayParametersOverTemplate(cluster.getCluster(), overlay);
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
	public void testSubCommandPresence() throws Exception {
		Assert.assertEquals(-1, executeWithURL("entity -type cluster "));
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

		Thread.sleep(2000);

		Assert.assertEquals(0,
				executeWithURL("instance -running -type process -name "
						+ overlay.get("processName")));

		Assert.assertEquals(0,
				executeWithURL("instance -status -type process -name "
						+ overlay.get("processName")
						+ " -start 2010-01-01T01:00Z"));

		Assert.assertEquals(0,
				executeWithURL("instance -status -type process -name "
						+ overlay.get("processName")
						+ " -start 2010-01-01T01:00Z"
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

		Assert.assertEquals(0,
				executeWithURL("instance -suspend -type process -name "
						+ overlay.get("processName")
						+ " -start 2010-01-01T01:00Z  -end 2010-01-01T01:00Z"));
		Thread.sleep(2000);
		Assert.assertEquals(0,
				executeWithURL("instance -resume -type process -name "
						+ overlay.get("processName")
						+ " -start 2010-01-01T01:00Z"));
	}

	@Test(enabled = enableTest)
	public void testInstanceKillAndRerun() throws Exception {
		Map<String, String> overlay = getUniqueOverlay();
		submitTestFiles(overlay);

		Assert.assertEquals(0,
				executeWithURL("entity -schedule -type process -name "
						+ overlay.get("processName")));

		Thread.sleep(5000);

		Assert.assertEquals(
				0,
				executeWithURL("instance -kill -type process -name "
						+ overlay.get("processName")
						+ " -start 2010-01-01T01:00Z  -end 2010-01-01T01:00Z"));
		Thread.sleep(2000);
		Assert.assertEquals(
				0,
				executeWithURL("instance -rerun -type process -name "
						+ overlay.get("processName")
						+ " -start 2010-01-01T01:00Z -file "
						+ createTempJobPropertiesFile()));
	}

	@Test(enabled = enableTest)
	public void testInvalidCLIInstanceCommands() throws Exception {
		// no command
		Assert.assertEquals(-1, executeWithURL(" -kill -type process -name "
				+ "name" + " -start 2010-01-01T01:00Z  -end 2010-01-01T03:00Z"));

		Assert.assertEquals(-1, executeWithURL("instance -kill  " + "name"
				+ " -start 2010-01-01T01:00Z  -end 2010-01-01T01:00Z"));

		Assert.assertEquals(-1,
				executeWithURL("instance -kill -type process -name " + "name"
						+ " -end 2010-01-01T03:00Z"));

		Assert.assertEquals(-1,
				executeWithURL("instance -kill -type process -name "
						+ " -start 2010-01-01T01:00Z  -end 2010-01-01T03:00Z"));

	}

	@Test(enabled = enableTest)
	public void testIvoryURL() throws Exception {
		Assert.assertEquals(-1, new IvoryCLI()
				.run(("instance -status -type process -name " + "processName"
						+ " -start 2010-01-01T01:00Z  -end 2010-01-01T03:00Z")
						.split("\\s")));

		Assert.assertEquals(-1, new IvoryCLI()
				.run(("instance -status -type process -name "
						+ "processName -url http://unknownhost:1234/"
						+ " -start 2010-01-01T01:00Z  -end 2010-01-01T03:00Z")
						.split("\\s")));

		// TODO add test case for client.properties
	}

	private int executeWithURL(String command) throws Exception {
		return new IvoryCLI()
				.run((command + " -url " + BASE_URL).split("\\s+"));
	}

	private Map<String, String> getUniqueOverlay() {
		Map<String, String> overlay = new HashMap<String, String>();
		long time = System.currentTimeMillis();
		overlay.put("clusterName", cluster.getCluster().getName());
		overlay.put("inputFeedName", "in" + time);
		overlay.put("outputFeedName", "out" + time);
		overlay.put("processName", "p" + time);
		Date endDate = new Date(time + 5 * 60 * 1000);
		overlay.put("endDate", EntityUtil.formatDateUTC(endDate));
		return overlay;
	}

	protected String overlayParametersOverTemplate(Entity templateEntity,
			Map<String, String> overlay) throws IOException {

		return overlayParametersOverTemplate(new ByteArrayInputStream(
				templateEntity.toString().getBytes()), overlay);
	}

	protected String overlayParametersOverTemplate(String template,
			Map<String, String> overlay) throws IOException {

		return overlayParametersOverTemplate(
				IvoryCLITest.class.getResourceAsStream(template), overlay);
	}

	protected String overlayParametersOverTemplate(InputStream templateStream,
			Map<String, String> overlay) throws IOException {
		File target = new File("webapp/target");
		if (!target.exists()) {
			target = new File("target");
		}

		File tmpFile = File.createTempFile("test", ".xml", target);
		OutputStream out = new FileOutputStream(tmpFile);
		InputStreamReader in = new InputStreamReader(templateStream);
		BufferedReader reader = new BufferedReader(in);
		String line;
		while ((line = reader.readLine()) != null) {
			Matcher matcher = VAR_PATTERN.matcher(line);
			while (matcher.find()) {
				String variable = line
						.substring(matcher.start(), matcher.end());
				line = line.replace(variable, overlay.get(variable.substring(2,
						variable.length() - 2)));
				matcher = VAR_PATTERN.matcher(line);
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

		String filePath = overlayParametersOverTemplate(cluster.getCluster(),
				overlay);
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

	private static class InMemoryWriter extends PrintStream {

		private StringBuffer buffer = new StringBuffer();

		public InMemoryWriter(OutputStream out) {
			super(out);
		}

		@Override
		public void println(String x) {
			clear();
			buffer.append(x);
			super.println(x);
		}

		public String getBuffer() {
			return buffer.toString();
		}

		public void clear() {
			buffer.delete(0, buffer.length());
		}
	}

	private void cleanupStore() throws IvoryException {
		for (EntityType type : EntityType.values()) {
			Collection<String> entities = ConfigurationStore.get().getEntities(
					type);
			for (String entity : entities) {
				ConfigurationStore.get().remove(type, entity);
			}
		}
	}

}
